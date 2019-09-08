//! An asynchronous, pipelined, PostgreSQL client.
//!
//! # Example
//!
//! ```no_run
//! use futures::{Future, Stream};
//! use tokio_postgres::NoTls;
//!
//! # #[cfg(not(feature = "runtime"))]
//! # let fut = futures::future::ok(());
//! # #[cfg(feature = "runtime")]
//! let fut =
//!     // Connect to the database
//!     tokio_postgres::connect("host=localhost user=postgres", NoTls)
//!
//!     .map(|(client, connection)| {
//!         // The connection object performs the actual communication with the database,
//!         // so spawn it off to run on its own.
//!         let connection = connection.map_err(|e| eprintln!("connection error: {}", e));
//!         tokio::spawn(connection);
//!
//!         // The client is what you use to make requests.
//!         client
//!     })
//!
//!     .and_then(|mut client| {
//!         // Now we can prepare a simple statement that just returns its parameter.
//!         client.prepare("SELECT $1::TEXT")
//!             .map(|statement| (client, statement))
//!     })
//!
//!     .and_then(|(mut client, statement)| {
//!         // And then execute it, returning a Stream of Rows which we collect into a Vec
//!         client.query(&statement, &[&"hello world"]).collect()
//!     })
//!
//!     // Now we can check that we got back the same string we sent over.
//!     .map(|rows| {
//!         let value: &str = rows[0].get(0);
//!         assert_eq!(value, "hello world");
//!     })
//!
//!     // And report any errors that happened.
//!     .map_err(|e| {
//!         eprintln!("error: {}", e);
//!     });
//!
//! // By default, tokio_postgres uses the tokio crate as its runtime.
//! tokio::run(fut);
//! ```
//!
//! # Behavior
//!
//! Calling a method like `Client::query` on its own does nothing. The associated request is not sent to the database
//! until the future returned by the method is first polled. Requests are executed in the order that they are first
//! polled, not in the order that their futures are created.
//!
//! # Pipelining
//!
//! The client supports *pipelined* requests. Pipelining can improve performance in use cases in which multiple,
//! independent queries need to be executed. In a traditional workflow, each query is sent to the server after the
//! previous query completes. In contrast, pipelining allows the client to send all of the queries to the server up
//! front, minimizing time spent by one side waiting for the other to finish sending data:
//!
//! ```not_rust
//!             Sequential                              Pipelined
//! | Client         | Server          |    | Client         | Server          |
//! |----------------|-----------------|    |----------------|-----------------|
//! | send query 1   |                 |    | send query 1   |                 |
//! |                | process query 1 |    | send query 2   | process query 1 |
//! | receive rows 1 |                 |    | send query 3   | process query 2 |
//! | send query 2   |                 |    | receive rows 1 | process query 3 |
//! |                | process query 2 |    | receive rows 2 |                 |
//! | receive rows 2 |                 |    | receive rows 3 |                 |
//! | send query 3   |                 |
//! |                | process query 3 |
//! | receive rows 3 |                 |
//! ```
//!
//! In both cases, the PostgreSQL server is executing the queries sequentially - pipelining just allows both sides of
//! the connection to work concurrently when possible.
//!
//! Pipelining happens automatically when futures are polled concurrently (for example, by using the futures `join`
//! combinator):
//!
//! ```rust
//! use futures::Future;
//! use tokio_postgres::{Client, Error, Statement};
//!
//! fn pipelined_prepare(
//!     client: &mut Client,
//! ) -> impl Future<Item = (Statement, Statement), Error = Error>
//! {
//!     client.prepare("SELECT * FROM foo")
//!         .join(client.prepare("INSERT INTO bar (id, name) VALUES ($1, $2)"))
//! }
//! ```
//!
//! # Runtime
//!
//! The client works with arbitrary `AsyncRead + AsyncWrite` streams. Convenience APIs are provided to handle the
//! connection process, but these are gated by the `runtime` Cargo feature, which is enabled by default. If disabled,
//! all dependence on the tokio runtime is removed.
//!
//! # SSL/TLS support
//!
//! TLS support is implemented via external libraries. `Client::connect` and `Config::connect` take a TLS implementation
//! as an argument. The `NoTls` type in this crate can be used when TLS is not required. Otherwise, the
//! `postgres-openssl` and `postgres-native-tls` crates provide implementations backed by the `openssl` and `native-tls`
//! crates, respectively.
#![doc(html_root_url = "https://docs.rs/tokio-postgres/0.4.0-rc.3")]
#![warn(rust_2018_idioms, clippy::all, missing_docs)]

use bytes::IntoBuf;
use futures::{Future, Poll, Stream};
use std::error::Error as StdError;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_io::{AsyncRead, AsyncWrite};

pub use crate::config::Config;
use crate::error::DbError;
pub use crate::error::Error;
pub use crate::row::{Row, SimpleQueryRow};
#[cfg(feature = "runtime")]
pub use crate::socket::Socket;
pub use crate::stmt::Column;
#[cfg(feature = "runtime")]
use crate::tls::MakeTlsConnect;
pub use crate::tls::NoTls;
use crate::tls::TlsConnect;
use crate::types::{ToSql, Type};

pub mod config;
pub mod error;
pub mod impls;
mod proto;
pub mod row;
#[cfg(feature = "runtime")]
mod socket;
mod stmt;
pub mod tls;
pub mod types;

fn next_statement() -> String {
    static ID: AtomicUsize = AtomicUsize::new(0);
    format!("s{}", ID.fetch_add(1, Ordering::SeqCst))
}

fn next_portal() -> String {
    static ID: AtomicUsize = AtomicUsize::new(0);
    format!("p{}", ID.fetch_add(1, Ordering::SeqCst))
}

/// A convenience function which parses a connection string and connects to the database.
///
/// See the documentation for [`Config`] for details on the connection string format.
///
/// Requires the `runtime` Cargo feature (enabled by default).
///
/// [`Config`]: config/struct.Config.html
#[cfg(feature = "runtime")]
pub fn connect<T>(config: &str, tls: T) -> impls::Connect<T>
where
    T: MakeTlsConnect<Socket>,
{
    impls::Connect(proto::ConnectFuture::new(tls, config.parse()))
}

/// An asynchronous PostgreSQL client.
///
/// The client is one half of what is returned when a connection is established. Users interact with the database
/// through this client object.
pub struct Client(proto::Client);

impl Client {
    /// Creates a new prepared statement.
    ///
    /// Prepared statements can be executed repeatedly, and may contain query parameters (indicated by `$1`, `$2`, etc),
    /// which are set when executed. Prepared statements can only be used with the connection that created them.
    pub fn prepare(&mut self, query: &str) -> impls::Prepare {
        self.prepare_typed(query, &[])
    }

    /// Like `prepare`, but allows the types of query parameters to be explicitly specified.
    ///
    /// The list of types may be smaller than the number of parameters - the types of the remaining parameters will be
    /// inferred. For example, `client.prepare_typed(query, &[])` is equivalent to `client.prepare(query)`.
    pub fn prepare_typed(&mut self, query: &str, param_types: &[Type]) -> impls::Prepare {
        impls::Prepare(self.0.prepare(next_statement(), query, param_types))
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// If the statement does not modify any rows (e.g. `SELECT`), 0 is returned.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn execute(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> impls::Execute {
        self.execute_iter(statement, params.iter().cloned())
    }

    /// Like [`execute`], but takes an iterator of parameters rather than a slice.
    ///
    /// [`execute`]: #method.execute
    pub fn execute_iter<'a, I>(&mut self, statement: &Statement, params: I) -> impls::Execute
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        impls::Execute(self.0.execute(&statement.0, params))
    }

    /// Executes a statement, returning a stream of the resulting rows.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn query(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> impls::Query {
        self.query_iter(statement, params.iter().cloned())
    }

    /// Like [`query`], but takes an iterator of parameters rather than a slice.
    ///
    /// [`query`]: #method.query
    pub fn query_iter<'a, I>(&mut self, statement: &Statement, params: I) -> impls::Query
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        impls::Query(self.0.query(&statement.0, params))
    }

    /// Binds a statement to a set of parameters, creating a `Portal` which can be incrementally queried.
    ///
    /// Portals only last for the duration of the transaction in which they are created - in particular, a portal
    /// created outside of a transaction is immediately destroyed. Portals can only be used on the connection that
    /// created them.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn bind(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> impls::Bind {
        self.bind_iter(statement, params.iter().cloned())
    }

    /// Like [`bind`], but takes an iterator of parameters rather than a slice.
    ///
    /// [`bind`]: #method.bind
    pub fn bind_iter<'a, I>(&mut self, statement: &Statement, params: I) -> impls::Bind
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        impls::Bind(self.0.bind(&statement.0, next_portal(), params))
    }

    /// Continues execution of a portal, returning a stream of the resulting rows.
    ///
    /// Unlike `query`, portals can be incrementally evaluated by limiting the number of rows returned in each call to
    /// query_portal. If the requested number is negative or 0, all rows will be returned.
    pub fn query_portal(&mut self, portal: &Portal, max_rows: i32) -> impls::QueryPortal {
        impls::QueryPortal(self.0.query_portal(&portal.0, max_rows))
    }

    /// Executes a `COPY FROM STDIN` statement, returning the number of rows created.
    ///
    /// The data in the provided stream is passed along to the server verbatim; it is the caller's responsibility to
    /// ensure it uses the proper format.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn copy_in<S>(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
        stream: S,
    ) -> impls::CopyIn<S>
    where
        S: Stream,
        S::Item: IntoBuf,
        <S::Item as IntoBuf>::Buf: 'static + Send,
        // FIXME error type?
        S::Error: Into<Box<dyn StdError + Sync + Send>>,
    {
        self.copy_in_iter(statement, params.iter().cloned(), stream)
    }

    /// Like [`copy_in`], except that it takes an iterator of parameters rather than a slice.
    ///
    /// [`copy_in`]: #method.copy_in
    pub fn copy_in_iter<'a, I, S>(
        &mut self,
        statement: &Statement,
        params: I,
        stream: S,
    ) -> impls::CopyIn<S>
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
        S: Stream,
        S::Item: IntoBuf,
        <S::Item as IntoBuf>::Buf: 'static + Send,
        // FIXME error type?
        S::Error: Into<Box<dyn StdError + Sync + Send>>,
    {
        impls::CopyIn(self.0.copy_in(&statement.0, params, stream))
    }

    /// Executes a `COPY TO STDOUT` statement, returning a stream of the resulting data.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn copy_out(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> impls::CopyOut {
        self.copy_out_iter(statement, params.iter().cloned())
    }

    /// Like [`copy_out`], except that it takes an iterator of parameters rather than a slice.
    ///
    /// [`copy_out`]: #method.copy_out
    pub fn copy_out_iter<'a, I>(&mut self, statement: &Statement, params: I) -> impls::CopyOut
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        impls::CopyOut(self.0.copy_out(&statement.0, params))
    }

    /// Executes a sequence of SQL statements using the simple query protocol.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. The simple query protocol returns the values in rows as strings rather than in their binary encodings,
    /// so the associated row type doesn't work with the `FromSql` trait. Rather than simply returning a stream over the
    /// rows, this method returns a stream over an enum which indicates either the completion of one of the commands,
    /// or a row of data. This preserves the framing between the separate statements in the request.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely imbed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub fn simple_query(&mut self, query: &str) -> impls::SimpleQuery {
        impls::SimpleQuery(self.0.simple_query(query))
    }

    /// A utility method to wrap a future in a database transaction.
    ///
    /// The returned future will start a transaction and then run the provided future. If the future returns `Ok`, it
    /// will commit the transaction, and if it returns `Err`, it will roll the transaction back.
    ///
    /// This is simply a convenience API; it's roughly equivalent to:
    ///
    /// ```ignore
    /// client.batch_execute("BEGIN")
    ///     .and_then(your_future)
    ///     .and_then(client.batch_execute("COMMIT"))
    ///     .or_else(|e| client.batch_execute("ROLLBACK").then(|_| Err(e)))
    /// ```
    ///
    /// # Warning
    ///
    /// Unlike the other futures created by a client, this future is *not* atomic with respect to other requests. If you
    /// attempt to execute it concurrently with other futures created by the same connection, they will interleave!
    pub fn build_transaction(&mut self) -> TransactionBuilder {
        TransactionBuilder(self.0.clone())
    }

    /// Attempts to cancel an in-progress query.
    ///
    /// The server provides no information about whether a cancellation attempt was successful or not. An error will
    /// only be returned if the client was unable to connect to the database.
    ///
    /// Requires the `runtime` Cargo feature (enabled by default).
    #[cfg(feature = "runtime")]
    pub fn cancel_query<T>(&mut self, make_tls_mode: T) -> impls::CancelQuery<T>
    where
        T: MakeTlsConnect<Socket>,
    {
        impls::CancelQuery(self.0.cancel_query(make_tls_mode))
    }

    /// Like `cancel_query`, but uses a stream which is already connected to the server rather than opening a new
    /// connection itself.
    pub fn cancel_query_raw<S, T>(&mut self, stream: S, tls_mode: T) -> impls::CancelQueryRaw<S, T>
    where
        S: AsyncRead + AsyncWrite,
        T: TlsConnect<S>,
    {
        impls::CancelQueryRaw(self.0.cancel_query_raw(stream, tls_mode))
    }

    /// Determines if the connection to the server has already closed.
    ///
    /// In that case, all future queries will fail.
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    /// Polls the client to check if it is idle.
    ///
    /// A connection is idle if there are no outstanding requests, whether they have begun being polled or not. For
    /// example, this can be used by a connection pool to ensure that all work done by one checkout is done before
    /// making the client available for a new request. Otherwise, any non-completed work from the first request could
    /// interleave with the second.
    pub fn poll_idle(&mut self) -> Poll<(), Error> {
        self.0.poll_idle()
    }
}

/// A connection to a PostgreSQL database.
///
/// This is one half of what is returned when a new connection is established. It performs the actual IO with the
/// server, and should generally be spawned off onto an executor to run in the background.
///
/// `Connection` implements `Future`, and only resolves when the connection is closed, either because a fatal error has
/// occurred, or because its associated `Client` has dropped and all outstanding work has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Connection<S, T>(proto::Connection<proto::MaybeTlsStream<S, T>>);

impl<S, T> Connection<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: AsyncRead + AsyncWrite,
{
    /// Returns the value of a runtime parameter for this connection.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.0.parameter(name)
    }

    /// Polls for asynchronous messages from the server.
    ///
    /// The server can send notices as well as notifications asynchronously to the client. Applications which wish to
    /// examine those messages should use this method to drive the connection rather than its `Future` implementation.
    pub fn poll_message(&mut self) -> Poll<Option<AsyncMessage>, Error> {
        self.0.poll_message()
    }
}

impl<S, T> Future for Connection<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: AsyncRead + AsyncWrite,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        self.0.poll()
    }
}

/// An asynchronous message from the server.
#[allow(clippy::large_enum_variant)]
pub enum AsyncMessage {
    /// A notice.
    ///
    /// Notices use the same format as errors, but aren't "errors" per-se.
    Notice(DbError),
    /// A notification.
    ///
    /// Connections can subscribe to notifications with the `LISTEN` command.
    Notification(Notification),
    #[doc(hidden)]
    __NonExhaustive,
}

/// A prepared statement.
///
/// Prepared statements can only be used with the connection that created them.
#[derive(Clone)]
pub struct Statement(proto::Statement);

impl Statement {
    /// Returns the expected types of the statement's parameters.
    pub fn params(&self) -> &[Type] {
        self.0.params()
    }

    /// Returns information about the columns returned when the statement is queried.
    pub fn columns(&self) -> &[Column] {
        self.0.columns()
    }
}

/// A portal.
///
/// Portals can only be used with the connection that created them, and only exist for the duration of the transaction
/// in which they were created.
pub struct Portal(proto::Portal);

/// A builder type which can wrap a future in a database transaction.
pub struct TransactionBuilder(proto::Client);

impl TransactionBuilder {
    /// Returns a future which wraps another in a database transaction.
    pub fn build<T>(self, future: T) -> impls::Transaction<T>
    where
        T: Future,
        // FIXME error type?
        T::Error: From<Error>,
    {
        impls::Transaction(proto::TransactionFuture::new(self.0, future))
    }
}

/// Message returned by the `SimpleQuery` stream.
pub enum SimpleQueryMessage {
    /// A row of data.
    Row(SimpleQueryRow),
    /// A statement in the query has completed.
    ///
    /// The number of rows modified or selected is returned.
    CommandComplete(u64),
    #[doc(hidden)]
    __NonExhaustive,
}

/// An asynchronous notification.
#[derive(Clone, Debug)]
pub struct Notification {
    process_id: i32,
    channel: String,
    payload: String,
}

impl Notification {
    /// The process ID of the notifying backend process.
    pub fn process_id(&self) -> i32 {
        self.process_id
    }

    /// The name of the channel that the notify has been raised on.
    pub fn channel(&self) -> &str {
        &self.channel
    }

    /// The "payload" string passed from the notifying process.
    pub fn payload(&self) -> &str {
        &self.payload
    }
}

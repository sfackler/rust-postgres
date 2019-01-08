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
//! combinator). Say we want to prepare 2 statements.
//!
//! # Runtime
//!
//! The client works with arbitrary `AsyncRead + AsyncWrite` streams. Convenience APIs are provided to handle the
//! connection process, but these are gated by the `runtime` Cargo feature, which is enabled by default. If disabled,
//! all dependence on the tokio runtime is removed.
#![warn(rust_2018_idioms, clippy::all)]

use bytes::{Bytes, IntoBuf};
use futures::{try_ready, Async, Future, Poll, Stream};
use std::error::Error as StdError;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_io::{AsyncRead, AsyncWrite};

pub use crate::config::*;
pub use crate::error::*;
pub use crate::row::*;
#[cfg(feature = "runtime")]
pub use crate::socket::Socket;
pub use crate::stmt::Column;
pub use crate::tls::*;
use crate::types::{ToSql, Type};

mod config;
pub mod error;
mod proto;
mod row;
#[cfg(feature = "runtime")]
mod socket;
mod stmt;
mod tls;
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
/// [`Config`]: ./Config.t.html
#[cfg(feature = "runtime")]
pub fn connect<T>(config: &str, tls_mode: T) -> Connect<T>
where
    T: MakeTlsMode<Socket>,
{
    Connect(proto::ConnectFuture::new(tls_mode, config.parse()))
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
    pub fn prepare(&mut self, query: &str) -> Prepare {
        self.prepare_typed(query, &[])
    }

    /// Like `prepare`, but allows the types of query parameters to be explicitly specified.
    ///
    /// The list of types may be smaller than the number of parameters - the types of the remaining parameters will be
    /// inferred. For example, `client.prepare_typed(query, &[])` is equivalent to `client.prepare(query)`.
    pub fn prepare_typed(&mut self, query: &str, param_types: &[Type]) -> Prepare {
        Prepare(self.0.prepare(next_statement(), query, param_types))
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// If the statement does not modify any rows (e.g. `SELECT`), 0 is returned.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn execute(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> Execute {
        Execute(self.0.execute(&statement.0, params))
    }

    /// Executes a statement, returning a stream of the resulting rows.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn query(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> Query {
        Query(self.0.query(&statement.0, params))
    }

    /// Binds a statement to a set of parameters, creating a `Portal` which can be incrementally queried.
    ///
    /// Portals only last for the duration of the transaction in which they are created - in particular, a portal
    /// created outside of a transaction is immediately destroyed. Portals can only be used on the connection that
    /// created them.
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn bind(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> Bind {
        Bind(self.0.bind(&statement.0, next_portal(), params))
    }

    /// Continues execution of a portal, returning a stream of the resulting rows.
    ///
    /// Unlike `query`, portals can be incrementally evaluated by limiting the number of rows returned in each call to
    /// query_portal. If the requested number is negative or 0, all rows will be returned.
    pub fn query_portal(&mut self, portal: &Portal, max_rows: i32) -> QueryPortal {
        QueryPortal(self.0.query_portal(&portal.0, max_rows))
    }

    /// Executes a `COPY FROM STDIN` statement, returning the number of rows created.
    ///
    /// The data in the provided stream is passed along to the server verbatim; it is the caller's responsibility to
    /// ensure it uses the proper format.
    pub fn copy_in<S>(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
        stream: S,
    ) -> CopyIn<S>
    where
        S: Stream,
        S::Item: IntoBuf,
        <S::Item as IntoBuf>::Buf: Send,
        // FIXME error type?
        S::Error: Into<Box<dyn StdError + Sync + Send>>,
    {
        CopyIn(self.0.copy_in(&statement.0, params, stream))
    }

    /// Executes a `COPY TO STDOUT` statement, returning a stream of the resulting data.
    pub fn copy_out(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> CopyOut {
        CopyOut(self.0.copy_out(&statement.0, params))
    }

    /// Executes a sequence of SQL statements.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. This is intended for the execution of batches of non-dynamic statements, for example, the creation of
    /// a schema for a fresh database.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely imbed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub fn batch_execute(&mut self, query: &str) -> BatchExecute {
        BatchExecute(self.0.batch_execute(query))
    }

    pub fn transaction(&mut self) -> TransactionBuilder {
        TransactionBuilder(self.0.clone())
    }

    /// Attempts to cancel an in-progress query.
    ///
    /// The server provides no information about whether a cancellation attempt was successful or not. An error will
    /// only be returned if the client was unable to connect to the database.
    ///
    /// Requires the `runtime` Cargo feature (enabled by default).
    #[cfg(feature = "runtime")]
    pub fn cancel_query<T>(&mut self, make_tls_mode: T) -> CancelQuery<T>
    where
        T: MakeTlsMode<Socket>,
    {
        CancelQuery(self.0.cancel_query(make_tls_mode))
    }

    /// Like `cancel_query`, but uses a stream which is already connected to the server rather than opening a new
    /// connection itself.
    pub fn cancel_query_raw<S, T>(&mut self, stream: S, tls_mode: T) -> CancelQueryRaw<S, T>
    where
        S: AsyncRead + AsyncWrite,
        T: TlsMode<S>,
    {
        CancelQueryRaw(self.0.cancel_query_raw(stream, tls_mode))
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
pub struct Connection<S>(proto::Connection<S>);

impl<S> Connection<S>
where
    S: AsyncRead + AsyncWrite,
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

impl<S> Future for Connection<S>
where
    S: AsyncRead + AsyncWrite,
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

#[must_use = "futures do nothing unless polled"]
pub struct CancelQueryRaw<S, T>(proto::CancelQueryRawFuture<S, T>)
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>;

impl<S, T> Future for CancelQueryRaw<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        self.0.poll()
    }
}

#[cfg(feature = "runtime")]
#[must_use = "futures do nothing unless polled"]
pub struct CancelQuery<T>(proto::CancelQueryFuture<T>)
where
    T: MakeTlsMode<Socket>;

#[cfg(feature = "runtime")]
impl<T> Future for CancelQuery<T>
where
    T: MakeTlsMode<Socket>,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        self.0.poll()
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct ConnectRaw<S, T>(proto::ConnectRawFuture<S, T>)
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>;

impl<S, T> Future for ConnectRaw<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>,
{
    type Item = (Client, Connection<T::Stream>);
    type Error = Error;

    fn poll(&mut self) -> Poll<(Client, Connection<T::Stream>), Error> {
        let (client, connection) = try_ready!(self.0.poll());

        Ok(Async::Ready((Client(client), Connection(connection))))
    }
}

#[cfg(feature = "runtime")]
#[must_use = "futures do nothing unless polled"]
pub struct Connect<T>(proto::ConnectFuture<T>)
where
    T: MakeTlsMode<Socket>;

#[cfg(feature = "runtime")]
impl<T> Future for Connect<T>
where
    T: MakeTlsMode<Socket>,
{
    type Item = (Client, Connection<T::Stream>);
    type Error = Error;

    fn poll(&mut self) -> Poll<(Client, Connection<T::Stream>), Error> {
        let (client, connection) = try_ready!(self.0.poll());

        Ok(Async::Ready((Client(client), Connection(connection))))
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct Prepare(proto::PrepareFuture);

impl Future for Prepare {
    type Item = Statement;
    type Error = Error;

    fn poll(&mut self) -> Poll<Statement, Error> {
        let statement = try_ready!(self.0.poll());

        Ok(Async::Ready(Statement(statement)))
    }
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

#[must_use = "futures do nothing unless polled"]
pub struct Execute(proto::ExecuteFuture);

impl Future for Execute {
    type Item = u64;
    type Error = Error;

    fn poll(&mut self) -> Poll<u64, Error> {
        self.0.poll()
    }
}

#[must_use = "streams do nothing unless polled"]
pub struct Query(proto::QueryStream<proto::Statement>);

impl Stream for Query {
    type Item = Row;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Row>, Error> {
        self.0.poll()
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct Bind(proto::BindFuture);

impl Future for Bind {
    type Item = Portal;
    type Error = Error;

    fn poll(&mut self) -> Poll<Portal, Error> {
        match self.0.poll() {
            Ok(Async::Ready(portal)) => Ok(Async::Ready(Portal(portal))),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
    }
}

#[must_use = "streams do nothing unless polled"]
pub struct QueryPortal(proto::QueryStream<proto::Portal>);

impl Stream for QueryPortal {
    type Item = Row;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Row>, Error> {
        self.0.poll()
    }
}

/// A portal.
///
/// Portals can only be used with the connection that created them, and only exist for the duration of the transaction
/// in which they were created.
pub struct Portal(proto::Portal);

#[must_use = "futures do nothing unless polled"]
pub struct CopyIn<S>(proto::CopyInFuture<S>)
where
    S: Stream,
    S::Item: IntoBuf,
    <S::Item as IntoBuf>::Buf: Send,
    S::Error: Into<Box<dyn StdError + Sync + Send>>;

impl<S> Future for CopyIn<S>
where
    S: Stream,
    S::Item: IntoBuf,
    <S::Item as IntoBuf>::Buf: Send,
    S::Error: Into<Box<dyn StdError + Sync + Send>>,
{
    type Item = u64;
    type Error = Error;

    fn poll(&mut self) -> Poll<u64, Error> {
        self.0.poll()
    }
}

#[must_use = "streams do nothing unless polled"]
pub struct CopyOut(proto::CopyOutStream);

impl Stream for CopyOut {
    type Item = Bytes;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Bytes>, Error> {
        self.0.poll()
    }
}

pub struct TransactionBuilder(proto::Client);

impl TransactionBuilder {
    pub fn build<T>(self, future: T) -> Transaction<T>
    where
        T: Future,
        // FIXME error type?
        T::Error: From<Error>,
    {
        Transaction(proto::TransactionFuture::new(self.0, future))
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct Transaction<T>(proto::TransactionFuture<T, T::Item, T::Error>)
where
    T: Future,
    T::Error: From<Error>;

impl<T> Future for Transaction<T>
where
    T: Future,
    T::Error: From<Error>,
{
    type Item = T::Item;
    type Error = T::Error;

    fn poll(&mut self) -> Poll<T::Item, T::Error> {
        self.0.poll()
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct BatchExecute(proto::SimpleQueryStream);

impl Future for BatchExecute {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        while let Some(_) = try_ready!(self.0.poll()) {}

        Ok(Async::Ready(()))
    }
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

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
//! front, eliminating time spent on both sides waiting for the other to finish sending data:
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
//! combinator). Say we want to prepare 2 statements:
//!
//! ```no_run
//! use futures::Future;
//! use tokio_postgres::{Client, Error, Statement};
//!
//! fn prepare_sequential(
//!     client: &mut Client,
//! ) -> impl Future<Item = (Statement, Statement), Error = Error>
//! {
//!     client.prepare("SELECT * FROM foo")
//!         .and_then({
//!             let f = client.prepare("INSERT INTO bar (id, name) VALUES ($1, $2)");
//!             |s1| f.map(|s2| (s1, s2))
//!         })
//! }
//!
//! fn prepare_pipelined(
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
#![warn(rust_2018_idioms, clippy::all)]

use bytes::{Bytes, IntoBuf};
use futures::{try_ready, Async, Future, Poll, Stream};
use std::error::Error as StdError;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_io::{AsyncRead, AsyncWrite};

pub use crate::config::*;
pub use crate::error::*;
use crate::proto::CancelFuture;
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

#[cfg(feature = "runtime")]
pub fn connect<T>(config: &str, tls_mode: T) -> Connect<T>
where
    T: MakeTlsMode<Socket>,
{
    Connect(proto::ConnectFuture::new(tls_mode, config.parse()))
}

pub fn cancel_query<S, T>(stream: S, tls_mode: T, cancel_data: CancelData) -> CancelQuery<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>,
{
    CancelQuery(CancelFuture::new(stream, tls_mode, cancel_data))
}

pub struct Client(proto::Client);

impl Client {
    pub fn prepare(&mut self, query: &str) -> Prepare {
        self.prepare_typed(query, &[])
    }

    pub fn prepare_typed(&mut self, query: &str, param_types: &[Type]) -> Prepare {
        Prepare(self.0.prepare(next_statement(), query, param_types))
    }

    pub fn execute(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> Execute {
        Execute(self.0.execute(&statement.0, params))
    }

    pub fn query(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> Query {
        Query(self.0.query(&statement.0, params))
    }

    pub fn bind(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> Bind {
        Bind(self.0.bind(&statement.0, next_portal(), params))
    }

    pub fn query_portal(&mut self, portal: &Portal, max_rows: i32) -> QueryPortal {
        QueryPortal(self.0.query_portal(&portal.0, max_rows))
    }

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

    pub fn copy_out(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> CopyOut {
        CopyOut(self.0.copy_out(&statement.0, params))
    }

    pub fn transaction(&mut self) -> TransactionBuilder {
        TransactionBuilder(self.0.clone())
    }

    pub fn batch_execute(&mut self, query: &str) -> BatchExecute {
        BatchExecute(self.0.batch_execute(query))
    }

    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    pub fn poll_idle(&mut self) -> Poll<(), Error> {
        self.0.poll_idle()
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct Connection<S>(proto::Connection<S>);

impl<S> Connection<S>
where
    S: AsyncRead + AsyncWrite,
{
    pub fn cancel_data(&self) -> CancelData {
        self.0.cancel_data()
    }

    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.0.parameter(name)
    }

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

#[allow(clippy::large_enum_variant)]
pub enum AsyncMessage {
    Notice(DbError),
    Notification(Notification),
    #[doc(hidden)]
    __NonExhaustive,
}

#[must_use = "futures do nothing unless polled"]
pub struct CancelQuery<S, T>(proto::CancelFuture<S, T>)
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>;

impl<S, T> Future for CancelQuery<S, T>
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

#[must_use = "futures do nothing unless polled"]
pub struct Handshake<S, T>(proto::HandshakeFuture<S, T>)
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>;

impl<S, T> Future for Handshake<S, T>
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

#[derive(Clone)]
pub struct Statement(proto::Statement);

impl Statement {
    pub fn params(&self) -> &[Type] {
        self.0.params()
    }

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

/// Contains information necessary to cancel queries for a session.
#[derive(Copy, Clone, Debug)]
pub struct CancelData {
    /// The process ID of the session.
    pub process_id: i32,
    /// The secret key for the session.
    pub secret_key: i32,
}

/// An asynchronous notification.
#[derive(Clone, Debug)]
pub struct Notification {
    /// The process ID of the notifying backend process.
    pub process_id: i32,
    /// The name of the channel that the notify has been raised on.
    pub channel: String,
    /// The "payload" string passed from the notifying process.
    pub payload: String,
}

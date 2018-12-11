#![warn(rust_2018_idioms, clippy::all)]

use bytes::{Bytes, IntoBuf};
use futures::{try_ready, Async, Future, Poll, Stream};
use std::error::Error as StdError;
use std::sync::atomic::{AtomicUsize, Ordering};
use tokio_io::{AsyncRead, AsyncWrite};

pub use crate::builder::*;
pub use crate::error::*;
use crate::proto::CancelFuture;
pub use crate::row::{Row, RowIndex};
pub use crate::stmt::Column;
pub use crate::tls::*;
use crate::types::{ToSql, Type};

mod builder;
pub mod error;
mod proto;
mod row;
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
pub struct Connect<S, T>(proto::ConnectFuture<S, T>)
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>;

impl<S, T> Future for Connect<S, T>
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
pub struct BatchExecute(proto::SimpleQueryFuture);

impl Future for BatchExecute {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        self.0.poll()
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

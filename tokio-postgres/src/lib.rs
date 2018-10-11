extern crate antidote;
extern crate bytes;
extern crate fallible_iterator;
extern crate futures_cpupool;
extern crate phf;
extern crate postgres_protocol;
extern crate postgres_shared;
extern crate tokio_codec;
extern crate tokio_io;
extern crate tokio_tcp;
extern crate tokio_timer;

#[macro_use]
extern crate futures;
#[macro_use]
extern crate lazy_static;
#[macro_use]
extern crate log;
#[macro_use]
extern crate state_machine_future;

#[cfg(unix)]
extern crate tokio_uds;

use bytes::Bytes;
use futures::{Async, Future, Poll, Stream};
use postgres_shared::rows::RowIndex;
use std::error::Error as StdError;
use std::fmt;
use std::sync::atomic::{AtomicUsize, Ordering};

#[doc(inline)]
pub use postgres_shared::stmt::Column;
#[doc(inline)]
pub use postgres_shared::{params, types};
#[doc(inline)]
pub use postgres_shared::{CancelData, Notification};

use error::{DbError, Error};
use params::ConnectParams;
use tls::TlsConnect;
use types::{FromSql, ToSql, Type};

pub mod error;
mod proto;
pub mod tls;

fn next_statement() -> String {
    static ID: AtomicUsize = AtomicUsize::new(0);
    format!("s{}", ID.fetch_add(1, Ordering::SeqCst))
}

fn next_portal() -> String {
    static ID: AtomicUsize = AtomicUsize::new(0);
    format!("p{}", ID.fetch_add(1, Ordering::SeqCst))
}

pub enum TlsMode {
    None,
    Prefer(Box<TlsConnect + Send>),
    Require(Box<TlsConnect + Send>),
}

pub fn cancel_query(params: ConnectParams, tls: TlsMode, cancel_data: CancelData) -> CancelQuery {
    CancelQuery(proto::CancelFuture::new(params, tls, cancel_data))
}

pub fn connect(params: ConnectParams, tls: TlsMode) -> Handshake {
    Handshake(proto::HandshakeFuture::new(params, tls))
}

pub struct Client(proto::Client);

impl Client {
    pub fn prepare(&mut self, query: &str) -> Prepare {
        self.prepare_typed(query, &[])
    }

    pub fn prepare_typed(&mut self, query: &str, param_types: &[Type]) -> Prepare {
        Prepare(self.0.prepare(next_statement(), query, param_types))
    }

    pub fn execute(&mut self, statement: &Statement, params: &[&ToSql]) -> Execute {
        Execute(self.0.execute(&statement.0, params))
    }

    pub fn query(&mut self, statement: &Statement, params: &[&ToSql]) -> Query {
        Query(self.0.query(&statement.0, params))
    }

    pub fn bind(&mut self, statement: &Statement, params: &[&ToSql]) -> Bind {
        Bind(self.0.bind(&statement.0, next_portal(), params))
    }

    pub fn query_portal(&mut self, portal: &Portal, max_rows: i32) -> QueryPortal {
        QueryPortal(self.0.query_portal(&portal.0, max_rows))
    }

    pub fn copy_in<S>(&mut self, statement: &Statement, params: &[&ToSql], stream: S) -> CopyIn<S>
    where
        S: Stream,
        S::Item: AsRef<[u8]>,
        // FIXME error type?
        S::Error: Into<Box<StdError + Sync + Send>>,
    {
        CopyIn(self.0.copy_in(&statement.0, params, stream))
    }

    pub fn copy_out(&mut self, statement: &Statement, params: &[&ToSql]) -> CopyOut {
        CopyOut(self.0.copy_out(&statement.0, params))
    }

    pub fn transaction<T>(&mut self, future: T) -> Transaction<T>
    where
        T: Future,
        // FIXME error type?
        T::Error: From<Error>,
    {
        Transaction(proto::TransactionFuture::new(self.0.clone(), future))
    }

    pub fn batch_execute(&mut self, query: &str) -> BatchExecute {
        BatchExecute(self.0.batch_execute(query))
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct Connection(proto::Connection);

impl Connection {
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

impl Future for Connection {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        self.0.poll()
    }
}

pub enum AsyncMessage {
    Notice(DbError),
    Notification(Notification),
    #[doc(hidden)]
    __NonExhaustive,
}

#[must_use = "futures do nothing unless polled"]
pub struct CancelQuery(proto::CancelFuture);

impl Future for CancelQuery {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        self.0.poll()
    }
}

#[must_use = "futures do nothing unless polled"]
pub struct Handshake(proto::HandshakeFuture);

impl Future for Handshake {
    type Item = (Client, Connection);
    type Error = Error;

    fn poll(&mut self) -> Poll<(Client, Connection), Error> {
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
        match self.0.poll() {
            Ok(Async::Ready(Some(row))) => Ok(Async::Ready(Some(Row(row)))),
            Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
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
        match self.0.poll() {
            Ok(Async::Ready(Some(row))) => Ok(Async::Ready(Some(Row(row)))),
            Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => Err(e),
        }
    }
}

pub struct Portal(proto::Portal);

#[must_use = "futures do nothing unless polled"]
pub struct CopyIn<S>(proto::CopyInFuture<S>)
where
    S: Stream,
    S::Item: AsRef<[u8]>,
    S::Error: Into<Box<StdError + Sync + Send>>;

impl<S> Future for CopyIn<S>
where
    S: Stream<Item = Vec<u8>>,
    S::Error: Into<Box<StdError + Sync + Send>>,
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

pub struct Row(proto::Row);

impl Row {
    pub fn columns(&self) -> &[Column] {
        self.0.columns()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn get<'a, I, T>(&'a self, idx: I) -> T
    where
        I: RowIndex + fmt::Debug,
        T: FromSql<'a>,
    {
        self.0.get(idx)
    }

    pub fn try_get<'a, I, T>(&'a self, idx: I) -> Result<Option<T>, Error>
    where
        I: RowIndex,
        T: FromSql<'a>,
    {
        self.0.try_get(idx)
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

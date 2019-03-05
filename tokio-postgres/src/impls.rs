//! Futures and stream types used in the crate.
use bytes::{Bytes, IntoBuf};
use futures::{try_ready, Async, Future, Poll, Stream};
use std::error;
use tokio_io::{AsyncRead, AsyncWrite};

use crate::proto;
use crate::{Client, Connection, Error, Portal, Row, SimpleQueryMessage, Statement, TlsConnect};
#[cfg(feature = "runtime")]
use crate::{MakeTlsConnect, Socket};

/// The future returned by `Client::cancel_query_raw`.
#[must_use = "futures do nothing unless polled"]
pub struct CancelQueryRaw<S, T>(pub(crate) proto::CancelQueryRawFuture<S, T>)
where
    S: AsyncRead + AsyncWrite,
    T: TlsConnect<S>;

impl<S, T> Future for CancelQueryRaw<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsConnect<S>,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        self.0.poll()
    }
}

/// The future returned by `Client::cancel_query`.
#[cfg(feature = "runtime")]
#[must_use = "futures do nothing unless polled"]
pub struct CancelQuery<T>(pub(crate) proto::CancelQueryFuture<T>)
where
    T: MakeTlsConnect<Socket>;

#[cfg(feature = "runtime")]
impl<T> Future for CancelQuery<T>
where
    T: MakeTlsConnect<Socket>,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        self.0.poll()
    }
}

/// The future returned by `Config::connect_raw`.
#[must_use = "futures do nothing unless polled"]
pub struct ConnectRaw<S, T>(pub(crate) proto::ConnectRawFuture<S, T>)
where
    S: AsyncRead + AsyncWrite,
    T: TlsConnect<S>;

impl<S, T> Future for ConnectRaw<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsConnect<S>,
{
    type Item = (Client, Connection<S, T::Stream>);
    type Error = Error;

    fn poll(&mut self) -> Poll<(Client, Connection<S, T::Stream>), Error> {
        let (client, connection) = try_ready!(self.0.poll());

        Ok(Async::Ready((Client(client), Connection(connection))))
    }
}

/// The future returned by `Config::connect`.
#[cfg(feature = "runtime")]
#[must_use = "futures do nothing unless polled"]
pub struct Connect<T>(pub(crate) proto::ConnectFuture<T>)
where
    T: MakeTlsConnect<Socket>;

#[cfg(feature = "runtime")]
impl<T> Future for Connect<T>
where
    T: MakeTlsConnect<Socket>,
{
    type Item = (Client, Connection<Socket, T::Stream>);
    type Error = Error;

    fn poll(&mut self) -> Poll<(Client, Connection<Socket, T::Stream>), Error> {
        let (client, connection) = try_ready!(self.0.poll());

        Ok(Async::Ready((Client(client), Connection(connection))))
    }
}

/// The future returned by `Client::prepare`.
#[must_use = "futures do nothing unless polled"]
pub struct Prepare(pub(crate) proto::PrepareFuture);

impl Future for Prepare {
    type Item = Statement;
    type Error = Error;

    fn poll(&mut self) -> Poll<Statement, Error> {
        let statement = try_ready!(self.0.poll());

        Ok(Async::Ready(Statement(statement)))
    }
}

/// The future returned by `Client::query`.
#[must_use = "streams do nothing unless polled"]
pub struct Query(pub(crate) proto::QueryStream<proto::Statement>);

impl Stream for Query {
    type Item = Row;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Row>, Error> {
        self.0.poll()
    }
}

/// The future returned by `Client::execute`.
#[must_use = "futures do nothing unless polled"]
pub struct Execute(pub(crate) proto::ExecuteFuture);

impl Future for Execute {
    type Item = u64;
    type Error = Error;

    fn poll(&mut self) -> Poll<u64, Error> {
        self.0.poll()
    }
}

/// The future returned by `Client::bind`.
#[must_use = "futures do nothing unless polled"]
pub struct Bind(pub(crate) proto::BindFuture);

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

/// The future returned by `Client::query_portal`.
#[must_use = "streams do nothing unless polled"]
pub struct QueryPortal(pub(crate) proto::QueryStream<proto::Portal>);

impl Stream for QueryPortal {
    type Item = Row;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Row>, Error> {
        self.0.poll()
    }
}

/// The future returned by `Client::copy_in`.
#[must_use = "futures do nothing unless polled"]
pub struct CopyIn<S>(pub(crate) proto::CopyInFuture<S>)
where
    S: Stream,
    S::Item: IntoBuf,
    <S::Item as IntoBuf>::Buf: Send,
    S::Error: Into<Box<dyn error::Error + Sync + Send>>;

impl<S> Future for CopyIn<S>
where
    S: Stream,
    S::Item: IntoBuf,
    <S::Item as IntoBuf>::Buf: Send,
    S::Error: Into<Box<dyn error::Error + Sync + Send>>,
{
    type Item = u64;
    type Error = Error;

    fn poll(&mut self) -> Poll<u64, Error> {
        self.0.poll()
    }
}

/// The future returned by `Client::copy_out`.
#[must_use = "streams do nothing unless polled"]
pub struct CopyOut(pub(crate) proto::CopyOutStream);

impl Stream for CopyOut {
    type Item = Bytes;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Bytes>, Error> {
        self.0.poll()
    }
}

/// The stream returned by `Client::simple_query`.
#[must_use = "streams do nothing unless polled"]
pub struct SimpleQuery(pub(crate) proto::SimpleQueryStream);

impl Stream for SimpleQuery {
    type Item = SimpleQueryMessage;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<SimpleQueryMessage>, Error> {
        self.0.poll()
    }
}

/// The future returned by `TransactionBuilder::build`.
#[must_use = "futures do nothing unless polled"]
pub struct Transaction<T>(pub(crate) proto::TransactionFuture<T, T::Item, T::Error>)
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

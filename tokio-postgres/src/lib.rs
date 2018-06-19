extern crate bytes;
extern crate fallible_iterator;
extern crate futures_cpupool;
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

use futures::{Async, Future, Poll};
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering};

#[doc(inline)]
pub use postgres_shared::stmt::Column;
#[doc(inline)]
pub use postgres_shared::{error, params, types};
#[doc(inline)]
pub use postgres_shared::{CancelData, Notification};

use error::Error;
use params::ConnectParams;
use types::Type;

mod proto;

static NEXT_STATEMENT_ID: AtomicUsize = AtomicUsize::new(0);

fn bad_response() -> Error {
    Error::from(io::Error::new(
        io::ErrorKind::InvalidInput,
        "the server returned an unexpected response",
    ))
}

fn disconnected() -> Error {
    Error::from(io::Error::new(
        io::ErrorKind::UnexpectedEof,
        "server disconnected",
    ))
}

pub fn connect(params: ConnectParams) -> Handshake {
    Handshake(proto::HandshakeFuture::new(params))
}

pub struct Client(proto::Client);

impl Client {
    pub fn prepare(&mut self, query: &str) -> Prepare {
        self.prepare_typed(query, &[])
    }

    pub fn prepare_typed(&mut self, query: &str, param_types: &[Type]) -> Prepare {
        let name = format!("s{}", NEXT_STATEMENT_ID.fetch_add(1, Ordering::SeqCst));
        Prepare(self.0.prepare(name, query, param_types))
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
}

impl Future for Connection {
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

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
extern crate state_machine_future;

#[cfg(unix)]
extern crate tokio_uds;

use futures::sync::mpsc;
use futures::{Async, Future, Poll};
use std::io;

#[doc(inline)]
pub use postgres_shared::{error, params};
#[doc(inline)]
pub use postgres_shared::{CancelData, Notification};

use error::Error;
use params::ConnectParams;

mod proto;

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

pub struct Client(mpsc::Sender<proto::Request>);

impl Client {
    pub fn connect(params: ConnectParams) -> Handshake {
        Handshake(proto::HandshakeFuture::new(params))
    }
}

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

pub struct Handshake(proto::HandshakeFuture);

impl Future for Handshake {
    type Item = (Client, Connection);
    type Error = Error;

    fn poll(&mut self) -> Poll<(Client, Connection), Error> {
        let (sender, connection) = try_ready!(self.0.poll());

        Ok(Async::Ready((Client(sender), Connection(connection))))
    }
}

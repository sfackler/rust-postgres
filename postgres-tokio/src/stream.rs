use futures::{BoxFuture, Future, IntoFuture, Async};
use postgres_shared::params::Host;
use postgres_protocol::message::backend::{self, ParseResult};
use std::io::{self, Read, Write};
use tokio_core::io::{Io, Codec, EasyBuf, Framed};
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_dns;
use tokio_uds::UnixStream;

pub type PostgresStream = Framed<InnerStream, PostgresCodec>;

pub fn connect(host: &Host,
                port: u16,
                handle: &Handle)
                -> BoxFuture<PostgresStream, io::Error> {
    match *host {
        Host::Tcp(ref host) => {
            tokio_dns::tcp_connect((&**host, port), handle.remote().clone())
                .map(|s| InnerStream::Tcp(s).framed(PostgresCodec))
                .boxed()
        }
        Host::Unix(ref host) => {
            let addr = host.join(format!(".s.PGSQL.{}", port));
            UnixStream::connect(addr, handle)
                .map(|s| InnerStream::Unix(s).framed(PostgresCodec))
                .into_future()
                .boxed()
        }
    }
}

pub enum InnerStream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl Read for InnerStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            InnerStream::Tcp(ref mut s) => s.read(buf),
            InnerStream::Unix(ref mut s) => s.read(buf),
        }
    }
}

impl Write for InnerStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            InnerStream::Tcp(ref mut s) => s.write(buf),
            InnerStream::Unix(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            InnerStream::Tcp(ref mut s) => s.flush(),
            InnerStream::Unix(ref mut s) => s.flush(),
        }
    }
}

impl Io for InnerStream {
    fn poll_read(&mut self) -> Async<()> {
        match *self {
            InnerStream::Tcp(ref mut s) => s.poll_read(),
            InnerStream::Unix(ref mut s) => s.poll_read(),
        }
    }

    fn poll_write(&mut self) -> Async<()> {
        match *self {
            InnerStream::Tcp(ref mut s) => s.poll_write(),
            InnerStream::Unix(ref mut s) => s.poll_write(),
        }
    }
}

pub struct PostgresCodec;

impl Codec for PostgresCodec {
    type In = backend::Message<Vec<u8>>;
    type Out = Vec<u8>;

    // FIXME ideally we'd avoid re-copying the data
    fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<Self::In>> {
        match try!(backend::Message::parse_owned(buf.as_ref())) {
            ParseResult::Complete { message, consumed } => {
                buf.drain_to(consumed);
                Ok(Some(message))
            }
            ParseResult::Incomplete { .. } => Ok(None)
        }
    }

    fn encode(&mut self, msg: Vec<u8>, buf: &mut Vec<u8>) -> io::Result<()> {
        buf.extend_from_slice(&msg);
        Ok(())
    }
}

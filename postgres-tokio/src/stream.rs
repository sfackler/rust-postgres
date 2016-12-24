use futures::{BoxFuture, Future, IntoFuture, Async, Sink, Stream as FuturesStream};
use futures::future::Either;
use postgres_shared::params::Host;
use postgres_protocol::message::backend::{self, ParseResult};
use postgres_protocol::message::frontend;
use std::io::{self, Read, Write};
use tokio_core::io::{Io, Codec, EasyBuf, Framed};
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_dns;
use tokio_uds::UnixStream;

use TlsMode;
use error::ConnectError;
use tls::TlsStream;

pub type PostgresStream = Framed<Box<TlsStream>, PostgresCodec>;

pub fn connect(host: Host,
               port: u16,
               tls_mode: TlsMode,
               handle: &Handle)
               -> BoxFuture<PostgresStream, ConnectError> {
    let inner = match host {
        Host::Tcp(ref host) => {
            Either::A(tokio_dns::tcp_connect((&**host, port), handle.remote().clone())
                .map(|s| Stream(InnerStream::Tcp(s))))
        }
        Host::Unix(ref host) => {
            let addr = host.join(format!(".s.PGSQL.{}", port));
            Either::B(UnixStream::connect(addr, handle)
                .map(|s| Stream(InnerStream::Unix(s)))
                .into_future())
        }
    };

    let (required, handshaker) = match tls_mode {
        TlsMode::Require(h) => (true, h),
        TlsMode::Prefer(h) => (false, h),
        TlsMode::None => {
            return inner.map(|s| {
                    let s: Box<TlsStream> = Box::new(s);
                    s.framed(PostgresCodec)
                })
                .map_err(ConnectError::Io)
                .boxed()
        },
    };

    inner.map(|s| s.framed(SslCodec))
        .and_then(|s| {
            let mut buf = vec![];
            frontend::ssl_request(&mut buf);
            s.send(buf)
        })
        .and_then(|s| s.into_future().map_err(|e| e.0))
        .map_err(ConnectError::Io)
        .and_then(move |(m, s)| {
            let s = s.into_inner();
            match (m, required) {
                (Some(b'N'), true) => {
                    Either::A(Err(ConnectError::Tls("the server does not support TLS".into())).into_future())
                }
                (Some(b'N'), false) => {
                    let s: Box<TlsStream> = Box::new(s);
                    Either::A(Ok(s).into_future())
                },
                (None, _) => Either::A(Err(ConnectError::Io(io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF"))).into_future()),
                _ => {
                    let host = match host {
                        Host::Tcp(ref host) => host,
                        Host::Unix(_) => unreachable!(),
                    };
                    Either::B(handshaker.handshake(host, s).map_err(ConnectError::Tls))
                }
            }
        })
        .map(|s| s.framed(PostgresCodec))
        .boxed()
}

pub struct Stream(InnerStream);

enum InnerStream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.0 {
            InnerStream::Tcp(ref mut s) => s.read(buf),
            InnerStream::Unix(ref mut s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.0 {
            InnerStream::Tcp(ref mut s) => s.write(buf),
            InnerStream::Unix(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.0 {
            InnerStream::Tcp(ref mut s) => s.flush(),
            InnerStream::Unix(ref mut s) => s.flush(),
        }
    }
}

impl Io for Stream {
    fn poll_read(&mut self) -> Async<()> {
        match self.0 {
            InnerStream::Tcp(ref mut s) => s.poll_read(),
            InnerStream::Unix(ref mut s) => s.poll_read(),
        }
    }

    fn poll_write(&mut self) -> Async<()> {
        match self.0 {
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

struct SslCodec;

impl Codec for SslCodec {
    type In = u8;
    type Out = Vec<u8>;

    fn decode(&mut self, buf: &mut EasyBuf) -> io::Result<Option<u8>> {
        if buf.as_slice().is_empty() {
            Ok(None)
        } else {
            let byte = buf.as_slice()[0];
            buf.drain_to(1);
            Ok(Some(byte))
        }
    }

    fn encode(&mut self, msg: Vec<u8>, buf: &mut Vec<u8>) -> io::Result<()> {
        buf.extend_from_slice(&msg);
        Ok(())
    }
}

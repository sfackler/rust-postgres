use bytes::{BytesMut, BufMut};
use futures::{BoxFuture, Future, IntoFuture, Sink, Stream as FuturesStream, Poll};
use futures::future::Either;
use postgres_shared::params::Host;
use postgres_protocol::message::backend;
use postgres_protocol::message::frontend;
use std::io::{self, Read, Write};
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_io::codec::{Encoder, Decoder, Framed};
use tokio_core::net::TcpStream;
use tokio_core::reactor::Handle;
use tokio_dns;

#[cfg(unix)]
use tokio_uds::UnixStream;

use TlsMode;
use error::ConnectError;
use tls::TlsStream;

pub type PostgresStream = Framed<Box<TlsStream>, PostgresCodec>;

pub fn connect(
    host: Host,
    port: u16,
    tls_mode: TlsMode,
    handle: &Handle,
) -> BoxFuture<PostgresStream, ConnectError> {
    let inner = match host {
        Host::Tcp(ref host) => {
            Either::A(
                tokio_dns::tcp_connect((&**host, port), handle.remote().clone())
                    .map(|s| Stream(InnerStream::Tcp(s)))
                    .map_err(ConnectError::Io),
            )
        }
        #[cfg(unix)]
        Host::Unix(ref host) => {
            let addr = host.join(format!(".s.PGSQL.{}", port));
            Either::B(
                UnixStream::connect(addr, handle)
                    .map(|s| Stream(InnerStream::Unix(s)))
                    .map_err(ConnectError::Io)
                    .into_future(),
            )
        }
        #[cfg(not(unix))]
        Host::Unix(_) => {
            Either::B(
                Err(ConnectError::ConnectParams(
                    "unix sockets are not supported on this \
                                                       platform"
                        .into(),
                )).into_future(),
            )
        }
    };

    let (required, handshaker) = match tls_mode {
        TlsMode::Require(h) => (true, h),
        TlsMode::Prefer(h) => (false, h),
        TlsMode::None => {
            return inner
                .map(|s| {
                    let s: Box<TlsStream> = Box::new(s);
                    s.framed(PostgresCodec)
                })
                .boxed()
        }
    };

    inner
        .map(|s| s.framed(SslCodec))
        .and_then(|s| {
            let mut buf = vec![];
            frontend::ssl_request(&mut buf);
            s.send(buf).map_err(ConnectError::Io)
        })
        .and_then(|s| s.into_future().map_err(|e| ConnectError::Io(e.0)))
        .and_then(move |(m, s)| {
            let s = s.into_inner();
            match (m, required) {
                (Some(b'N'), true) => {
                    Either::A(
                        Err(ConnectError::Tls("the server does not support TLS".into()))
                            .into_future(),
                    )
                }
                (Some(b'N'), false) => {
                    let s: Box<TlsStream> = Box::new(s);
                    Either::A(Ok(s).into_future())
                }
                (None, _) => {
                    Either::A(
                        Err(ConnectError::Io(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "unexpected EOF",
                        ))).into_future(),
                    )
                }
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

/// A raw connection to the database.
pub struct Stream(InnerStream);

enum InnerStream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self.0 {
            InnerStream::Tcp(ref mut s) => s.read(buf),
            #[cfg(unix)]
            InnerStream::Unix(ref mut s) => s.read(buf),
        }
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self.0 {
            InnerStream::Tcp(ref mut s) => s.write(buf),
            #[cfg(unix)]
            InnerStream::Unix(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self.0 {
            InnerStream::Tcp(ref mut s) => s.flush(),
            #[cfg(unix)]
            InnerStream::Unix(ref mut s) => s.flush(),
        }
    }
}

impl AsyncRead for Stream {
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        match self.0 {
            InnerStream::Tcp(ref s) => s.prepare_uninitialized_buffer(buf),
            #[cfg(unix)]
            InnerStream::Unix(ref s) => s.prepare_uninitialized_buffer(buf),
        }
    }

    fn read_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: BufMut,
    {
        match self.0 {
            InnerStream::Tcp(ref mut s) => s.read_buf(buf),
            #[cfg(unix)]
            InnerStream::Unix(ref mut s) => s.read_buf(buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        match self.0 {
            InnerStream::Tcp(ref mut s) => s.shutdown(),
            #[cfg(unix)]
            InnerStream::Unix(ref mut s) => s.shutdown(),
        }
    }
}

pub struct PostgresCodec;

impl Decoder for PostgresCodec {
    type Item = backend::Message;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<backend::Message>> {
        backend::Message::parse(buf)
    }
}

impl Encoder for PostgresCodec {
    type Item = Vec<u8>;
    type Error = io::Error;

    fn encode(&mut self, msg: Vec<u8>, buf: &mut BytesMut) -> io::Result<()> {
        buf.extend(&msg);
        Ok(())
    }
}

struct SslCodec;

impl Decoder for SslCodec {
    type Item = u8;
    type Error = io::Error;

    fn decode(&mut self, buf: &mut BytesMut) -> io::Result<Option<u8>> {
        if buf.is_empty() {
            Ok(None)
        } else {
            Ok(Some(buf.split_to(1)[0]))
        }
    }
}

impl Encoder for SslCodec {
    type Item = Vec<u8>;
    type Error = io::Error;

    fn encode(&mut self, msg: Vec<u8>, buf: &mut BytesMut) -> io::Result<()> {
        buf.extend(&msg);
        Ok(())
    }
}

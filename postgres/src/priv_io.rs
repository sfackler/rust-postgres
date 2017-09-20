use std::io::{self, BufWriter, Read, Write};
use std::net::{ToSocketAddrs, SocketAddr};
use std::time::Duration;
use std::result;
use bytes::{BufMut, BytesMut};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd, FromRawFd, IntoRawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};
use postgres_protocol::message::frontend;
use postgres_protocol::message::backend;
use socket2::{Socket, SockAddr, Domain, Type};

use {Result, TlsMode};
use error;
use tls::TlsStream;
use params::{ConnectParams, Host};

const INITIAL_CAPACITY: usize = 8 * 1024;

pub struct MessageStream {
    stream: BufWriter<Box<TlsStream>>,
    in_buf: BytesMut,
    out_buf: Vec<u8>,
}

impl MessageStream {
    pub fn new(stream: Box<TlsStream>) -> MessageStream {
        MessageStream {
            stream: BufWriter::new(stream),
            in_buf: BytesMut::with_capacity(INITIAL_CAPACITY),
            out_buf: vec![],
        }
    }

    pub fn get_ref(&self) -> &Box<TlsStream> {
        self.stream.get_ref()
    }

    pub fn write_message<F, E>(&mut self, f: F) -> result::Result<(), E>
    where
        F: FnOnce(&mut Vec<u8>) -> result::Result<(), E>,
        E: From<io::Error>,
    {
        self.out_buf.clear();
        f(&mut self.out_buf)?;
        self.stream.write_all(&self.out_buf).map_err(From::from)
    }

    pub fn read_message(&mut self) -> io::Result<backend::Message> {
        loop {
            match backend::Message::parse(&mut self.in_buf) {
                Ok(Some(message)) => return Ok(message),
                Ok(None) => self.read_in()?,
                Err(e) => return Err(e),
            }
        }
    }

    fn read_in(&mut self) -> io::Result<()> {
        self.in_buf.reserve(1);
        match self.stream.get_mut().read(
            unsafe { self.in_buf.bytes_mut() },
        ) {
            Ok(0) => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            )),
            Ok(n) => {
                unsafe { self.in_buf.advance_mut(n) };
                Ok(())
            }
            Err(e) => Err(e),
        }
    }

    pub fn read_message_timeout(
        &mut self,
        timeout: Duration,
    ) -> io::Result<Option<backend::Message>> {
        if self.in_buf.is_empty() {
            self.set_read_timeout(Some(timeout))?;
            let r = self.read_in();
            self.set_read_timeout(None)?;

            match r {
                Ok(()) => {}
                Err(ref e)
                    if e.kind() == io::ErrorKind::WouldBlock ||
                           e.kind() == io::ErrorKind::TimedOut => return Ok(None),
                Err(e) => return Err(e),
            }
        }

        self.read_message().map(Some)
    }

    pub fn read_message_nonblocking(&mut self) -> io::Result<Option<backend::Message>> {
        if self.in_buf.is_empty() {
            self.set_nonblocking(true)?;
            let r = self.read_in();
            self.set_nonblocking(false)?;

            match r {
                Ok(()) => {}
                Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => return Ok(None),
                Err(e) => return Err(e),
            }
        }

        self.read_message().map(Some)
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.stream.flush()
    }

    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        self.stream.get_ref().get_ref().0.set_read_timeout(timeout)
    }

    fn set_nonblocking(&self, nonblock: bool) -> io::Result<()> {
        self.stream.get_ref().get_ref().0.set_nonblocking(nonblock)
    }
}

/// A connection to the Postgres server.
///
/// It implements `Read`, `Write` and `TlsStream`, as well as `AsRawFd` on
/// Unix platforms and `AsRawSocket` on Windows platforms.
#[derive(Debug)]
pub struct Stream(Socket);

impl Read for Stream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        self.0.read(buf)
    }
}

impl Write for Stream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0.write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.0.flush()
    }
}

impl TlsStream for Stream {
    fn get_ref(&self) -> &Stream {
        self
    }

    fn get_mut(&mut self) -> &mut Stream {
        self
    }
}

#[cfg(unix)]
impl AsRawFd for Stream {
    fn as_raw_fd(&self) -> RawFd {
        self.0.as_raw_fd()
    }
}

#[cfg(windows)]
impl AsRawSocket for Stream {
    fn as_raw_socket(&self) -> RawSocket {
        self.0.as_raw_socket()
    }
}

fn open_socket(params: &ConnectParams) -> Result<Socket> {
    let port = params.port();
    match *params.host() {
        Host::Tcp(ref host) => {
            let mut error = None;
            for addr in (&**host, port).to_socket_addrs()? {
                let domain = match addr {
                    SocketAddr::V4(_) => Domain::ipv4(),
                    SocketAddr::V6(_) => Domain::ipv6(),
                };
                let socket = Socket::new(domain, Type::stream(), None)?;
                let addr = SockAddr::from(addr);
                let r = match params.connect_timeout() {
                    Some(timeout) => socket.connect_timeout(&addr, timeout),
                    None => socket.connect(&addr),
                };
                match r {
                    Ok(()) => return Ok(socket),
                    Err(e) => error = Some(e),
                }
            }

            Err(
                error
                    .unwrap_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "could not resolve any addresses",
                        )
                    })
                    .into(),
            )
        }
        #[cfg(unix)]
        Host::Unix(ref path) => {
            let path = path.join(&format!(".s.PGSQL.{}", port));
            Ok(UnixStream::connect(&path).map(|s| unsafe {
                Socket::from_raw_fd(s.into_raw_fd())
            })?)
        }
        #[cfg(not(unix))]
        Host::Unix(..) => {
            Err(
                io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "unix sockets are not supported on this system",
                ).into(),
            )
        }
    }
}

pub fn initialize_stream(params: &ConnectParams, tls: TlsMode) -> Result<Box<TlsStream>> {
    let mut socket = Stream(open_socket(params)?);

    let (tls_required, handshaker) = match tls {
        TlsMode::None => return Ok(Box::new(socket)),
        TlsMode::Prefer(handshaker) => (false, handshaker),
        TlsMode::Require(handshaker) => (true, handshaker),
    };

    let mut buf = vec![];
    frontend::ssl_request(&mut buf);
    socket.write_all(&buf)?;
    socket.flush()?;

    let mut b = [0; 1];
    socket.read_exact(&mut b)?;
    if b[0] == b'N' {
        if tls_required {
            return Err(error::tls("the server does not support TLS".into()));
        } else {
            return Ok(Box::new(socket));
        }
    }

    let host = match *params.host() {
        Host::Tcp(ref host) => host,
        // Postgres doesn't support TLS over unix sockets
        Host::Unix(_) => return Err(::bad_response().into()),
    };

    handshaker.tls_handshake(host, socket).map_err(error::tls)
}

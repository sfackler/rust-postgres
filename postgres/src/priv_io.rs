use std::io::{self, BufWriter, Read, Write};
use std::fmt;
use std::net::TcpStream;
use std::time::Duration;
use bytes::{BufMut, BytesMut};
#[cfg(unix)]
use std::os::unix::net::UnixStream;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};
use postgres_protocol::message::frontend;
use postgres_protocol::message::backend;

use TlsMode;
use error::ConnectError;
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

    pub fn write_message<F, E>(&mut self, f: F) -> Result<(), E>
    where
        F: FnOnce(&mut Vec<u8>) -> Result<(), E>,
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
        match self.stream.get_ref().get_ref().0 {
            InternalStream::Tcp(ref s) => s.set_read_timeout(timeout),
            #[cfg(unix)]
            InternalStream::Unix(ref s) => s.set_read_timeout(timeout),
        }
    }

    fn set_nonblocking(&self, nonblock: bool) -> io::Result<()> {
        match self.stream.get_ref().get_ref().0 {
            InternalStream::Tcp(ref s) => s.set_nonblocking(nonblock),
            #[cfg(unix)]
            InternalStream::Unix(ref s) => s.set_nonblocking(nonblock),
        }
    }
}

/// A connection to the Postgres server.
///
/// It implements `Read`, `Write` and `TlsStream`, as well as `AsRawFd` on
/// Unix platforms and `AsRawSocket` on Windows platforms.
pub struct Stream(InternalStream);

impl fmt::Debug for Stream {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self.0 {
            InternalStream::Tcp(ref s) => fmt::Debug::fmt(s, fmt),
            #[cfg(unix)]
            InternalStream::Unix(ref s) => fmt::Debug::fmt(s, fmt),
        }
    }
}

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
        match self.0 {
            InternalStream::Tcp(ref s) => s.as_raw_fd(),
            InternalStream::Unix(ref s) => s.as_raw_fd(),
        }
    }
}

#[cfg(windows)]
impl AsRawSocket for Stream {
    fn as_raw_socket(&self) -> RawSocket {
        // Unix sockets aren't supported on windows, so no need to match
        match self.0 {
            InternalStream::Tcp(ref s) => s.as_raw_socket(),
        }
    }
}

enum InternalStream {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl Read for InternalStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            InternalStream::Tcp(ref mut s) => s.read(buf),
            #[cfg(unix)]
            InternalStream::Unix(ref mut s) => s.read(buf),
        }
    }
}

impl Write for InternalStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            InternalStream::Tcp(ref mut s) => s.write(buf),
            #[cfg(unix)]
            InternalStream::Unix(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            InternalStream::Tcp(ref mut s) => s.flush(),
            #[cfg(unix)]
            InternalStream::Unix(ref mut s) => s.flush(),
        }
    }
}

fn open_socket(params: &ConnectParams) -> Result<InternalStream, ConnectError> {
    let port = params.port();
    match *params.host() {
        Host::Tcp(ref host) => {
            Ok(TcpStream::connect(&(&**host, port)).map(
                InternalStream::Tcp,
            )?)
        }
        #[cfg(unix)]
        Host::Unix(ref path) => {
            let path = path.join(&format!(".s.PGSQL.{}", port));
            Ok(UnixStream::connect(&path).map(InternalStream::Unix)?)
        }
        #[cfg(not(unix))]
        Host::Unix(..) => {
            Err(ConnectError::Io(io::Error::new(
                io::ErrorKind::InvalidInput,
                "unix sockets are not supported on this system",
            )))
        }
    }
}

pub fn initialize_stream(
    params: &ConnectParams,
    tls: TlsMode,
) -> Result<Box<TlsStream>, ConnectError> {
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
            return Err(ConnectError::Tls("the server does not support TLS".into()));
        } else {
            return Ok(Box::new(socket));
        }
    }

    let host = match *params.host() {
        Host::Tcp(ref host) => host,
        // Postgres doesn't support TLS over unix sockets
        Host::Unix(_) => return Err(ConnectError::Io(::bad_response())),
    };

    handshaker.tls_handshake(host, socket).map_err(
        ConnectError::Tls,
    )
}

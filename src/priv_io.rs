use std::io;
use std::io::prelude::*;
use std::fmt;
use std::net::TcpStream;
use std::time::Duration;
use bufstream::BufStream;
#[cfg(unix)]
use std::os::unix::net::UnixStream;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};
use postgres_protocol::message::frontend;
use postgres_protocol::message::backend::{self, ParseResult};

use TlsMode;
use error::ConnectError;
use io::TlsStream;
use params::{ConnectParams, ConnectTarget};

const DEFAULT_PORT: u16 = 5432;
const MESSAGE_HEADER_SIZE: usize = 5;

pub struct MessageStream {
    stream: BufStream<Box<TlsStream>>,
    buf: Vec<u8>,
}

impl MessageStream {
    pub fn new(stream: Box<TlsStream>) -> MessageStream {
        MessageStream {
            stream: BufStream::new(stream),
            buf: vec![],
        }
    }

    pub fn get_ref(&self) -> &Box<TlsStream> {
        self.stream.get_ref()
    }

    pub fn write_message<F, E>(&mut self, f: F) -> Result<(), E>
        where F: FnOnce(&mut Vec<u8>) -> Result<(), E>,
              E: From<io::Error>
    {
        self.buf.clear();
        try!(f(&mut self.buf));
        self.stream.write_all(&self.buf).map_err(From::from)
    }

    fn inner_read_message(&mut self, b: u8) -> io::Result<backend::Message> {
        self.buf.resize(MESSAGE_HEADER_SIZE, 0);
        self.buf[0] = b;
        try!(self.stream.read_exact(&mut self.buf[1..]));

        let len = match try!(backend::Message::parse(&self.buf)) {
            ParseResult::Complete { message, .. } => return Ok(message),
            ParseResult::Incomplete { required_size } => Some(required_size.unwrap()),
        };

        if let Some(len) = len {
            self.buf.resize(len, 0);
            try!(self.stream.read_exact(&mut self.buf[MESSAGE_HEADER_SIZE..]));
        };

        match try!(backend::Message::parse(&self.buf)) {
            ParseResult::Complete { message, .. } => Ok(message),
            ParseResult::Incomplete { .. } => unreachable!(),
        }
    }

    pub fn read_message(&mut self) -> io::Result<backend::Message> {
        let mut b = [0; 1];
        try!(self.stream.read_exact(&mut b));
        self.inner_read_message(b[0])
    }

    pub fn read_message_timeout(&mut self,
                                timeout: Duration)
                                -> io::Result<Option<backend::Message>> {
        try!(self.set_read_timeout(Some(timeout)));
        let mut b = [0; 1];
        let r = self.stream.read_exact(&mut b);
        try!(self.set_read_timeout(None));

        match r {
            Ok(()) => self.inner_read_message(b[0]).map(Some),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock ||
                          e.kind() == io::ErrorKind::TimedOut => Ok(None),
            Err(e) => Err(e),
        }
    }

    pub fn read_message_nonblocking(&mut self) -> io::Result<Option<backend::Message>> {
        try!(self.set_nonblocking(true));
        let mut b = [0; 1];
        let r = self.stream.read_exact(&mut b);
        try!(self.set_nonblocking(false));

        match r {
            Ok(()) => self.inner_read_message(b[0]).map(Some),
            Err(ref e) if e.kind() == io::ErrorKind::WouldBlock => Ok(None),
            Err(e) => Err(e),
        }
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
    let port = params.port.unwrap_or(DEFAULT_PORT);
    match params.target {
        ConnectTarget::Tcp(ref host) => {
            Ok(try!(TcpStream::connect(&(&**host, port)).map(InternalStream::Tcp)))
        }
        #[cfg(unix)]
        ConnectTarget::Unix(ref path) => {
            let path = path.join(&format!(".s.PGSQL.{}", port));
            Ok(try!(UnixStream::connect(&path).map(InternalStream::Unix)))
        }
        #[cfg(not(unix))]
        ConnectTarget::Unix(..) => {
            Err(ConnectError::Io(io::Error::new(io::ErrorKind::InvalidInput,
                                                "unix sockets are not supported on this system")))
        }
    }
}

pub fn initialize_stream(params: &ConnectParams,
                         tls: TlsMode)
                         -> Result<Box<TlsStream>, ConnectError> {
    let mut socket = Stream(try!(open_socket(params)));

    let (tls_required, handshaker) = match tls {
        TlsMode::None => return Ok(Box::new(socket)),
        TlsMode::Prefer(handshaker) => (false, handshaker),
        TlsMode::Require(handshaker) => (true, handshaker),
    };

    let mut buf = vec![];
    frontend::ssl_request(&mut buf);
    try!(socket.write_all(&buf));
    try!(socket.flush());

    let mut b = [0; 1];
    try!(socket.read_exact(&mut b));
    if b[0] == b'N' {
        if tls_required {
            return Err(ConnectError::Tls("the server does not support TLS".into()));
        } else {
            return Ok(Box::new(socket));
        }
    }

    let host = match params.target {
        ConnectTarget::Tcp(ref host) => host,
        // Postgres doesn't support TLS over unix sockets
        ConnectTarget::Unix(_) => return Err(ConnectError::Io(::bad_response())),
    };

    handshaker.tls_handshake(host, socket).map_err(ConnectError::Tls)
}

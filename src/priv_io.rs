use byteorder::ReadBytesExt;
use std::error::Error;
use std::io;
use std::io::prelude::*;
use std::fmt;
use std::net::TcpStream;
use std::time::Duration;
use bufstream::BufStream;
#[cfg(feature = "with-unix_socket")]
use unix_socket::UnixStream;
#[cfg(all(not(feature = "with-unix_socket"), all(unix, feature = "nightly")))]
use std::os::unix::net::UnixStream;
#[cfg(unix)]
use std::os::unix::io::{AsRawFd, RawFd};
#[cfg(windows)]
use std::os::windows::io::{AsRawSocket, RawSocket};

use {TlsMode, ConnectParams, ConnectTarget};
use error::ConnectError;
use io::TlsStream;
use message::{self, WriteMessage};
use message::Frontend;

const DEFAULT_PORT: u16 = 5432;

#[doc(hidden)]
pub trait StreamOptions {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()>;
    fn set_nonblocking(&self, nonblock: bool) -> io::Result<()>;
}

impl StreamOptions for BufStream<Box<TlsStream>> {
    fn set_read_timeout(&self, timeout: Option<Duration>) -> io::Result<()> {
        match self.get_ref().get_ref().0 {
            InternalStream::Tcp(ref s) => s.set_read_timeout(timeout),
            #[cfg(any(feature = "with-unix_socket", all(unix, feature = "nightly")))]
            InternalStream::Unix(ref s) => s.set_read_timeout(timeout),
        }
    }

    fn set_nonblocking(&self, nonblock: bool) -> io::Result<()> {
        match self.get_ref().get_ref().0 {
            InternalStream::Tcp(ref s) => s.set_nonblocking(nonblock),
            #[cfg(any(feature = "with-unix_socket", all(unix, feature = "nightly")))]
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
            #[cfg(any(feature = "with-unix_socket", all(unix, feature = "nightly")))]
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
            #[cfg(any(feature = "with-unix_socket", all(unix, feature = "nightly")))]
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
    #[cfg(any(feature = "with-unix_socket", all(unix, feature = "nightly")))]
    Unix(UnixStream),
}

impl Read for InternalStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            InternalStream::Tcp(ref mut s) => s.read(buf),
            #[cfg(any(feature = "with-unix_socket", all(unix, feature = "nightly")))]
            InternalStream::Unix(ref mut s) => s.read(buf),
        }
    }
}

impl Write for InternalStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            InternalStream::Tcp(ref mut s) => s.write(buf),
            #[cfg(any(feature = "with-unix_socket", all(unix, feature = "nightly")))]
            InternalStream::Unix(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            InternalStream::Tcp(ref mut s) => s.flush(),
            #[cfg(any(feature = "with-unix_socket", all(unix, feature = "nightly")))]
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
        #[cfg(any(feature = "with-unix_socket", all(unix, feature = "nightly")))]
        ConnectTarget::Unix(ref path) => {
            let path = path.join(&format!(".s.PGSQL.{}", port));
            Ok(try!(UnixStream::connect(&path).map(InternalStream::Unix)))
        }
    }
}

pub fn initialize_stream(params: &ConnectParams,
                         ssl: TlsMode)
                         -> Result<Box<TlsStream>, ConnectError> {
    let mut socket = Stream(try!(open_socket(params)));

    let (ssl_required, negotiator) = match ssl {
        TlsMode::None => return Ok(Box::new(socket)),
        TlsMode::Prefer(negotiator) => (false, negotiator),
        TlsMode::Require(negotiator) => (true, negotiator),
    };

    try!(socket.write_message(&Frontend::SslRequest { code: message::SSL_CODE }));
    try!(socket.flush());

    if try!(socket.read_u8()) == b'N' {
        if ssl_required {
            let err: Box<Error + Sync + Send> = "The server does not support SSL".into();
            return Err(ConnectError::Ssl(err));
        } else {
            return Ok(Box::new(socket));
        }
    }

    // Postgres doesn't support SSL over unix sockets
    let host = match params.target {
        ConnectTarget::Tcp(ref host) => host,
        #[cfg(any(feature = "with-unix_socket", all(unix, feature = "nightly")))]
        ConnectTarget::Unix(_) => return Err(ConnectError::Io(::bad_response())),
    };

    negotiator.tls_handshake(host, socket).map_err(ConnectError::Ssl)
}

use openssl::ssl::{SslStream, MaybeSslStream};
use std::io::BufferedStream;
use std::io::net::ip::Port;
use std::io::net::tcp::TcpStream;
use std::io::net::pipe::UnixStream;
use std::io::{IoResult, Stream};

use {ConnectParams, SslMode, ConnectTarget, ConnectError};
use message;
use message::WriteMessage;
use message::FrontendMessage::SslRequest;

const DEFAULT_PORT: Port = 5432;

#[doc(hidden)]
pub trait Timeout {
    fn set_read_timeout(&mut self, timeout_ms: Option<u64>);
}

impl<S: Stream+Timeout> Timeout for MaybeSslStream<S> {
    fn set_read_timeout(&mut self, timeout_ms: Option<u64>) {
        self.get_mut().set_read_timeout(timeout_ms);
    }
}

impl<S: Stream+Timeout> Timeout for BufferedStream<S> {
    fn set_read_timeout(&mut self, timeout_ms: Option<u64>) {
        self.get_mut().set_read_timeout(timeout_ms);
    }
}

pub enum InternalStream {
    Tcp(TcpStream),
    Unix(UnixStream),
}

impl Reader for InternalStream {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<uint> {
        match *self {
            InternalStream::Tcp(ref mut s) => s.read(buf),
            InternalStream::Unix(ref mut s) => s.read(buf),
        }
    }
}

impl Writer for InternalStream {
    fn write(&mut self, buf: &[u8]) -> IoResult<()> {
        match *self {
            InternalStream::Tcp(ref mut s) => s.write(buf),
            InternalStream::Unix(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        match *self {
            InternalStream::Tcp(ref mut s) => s.flush(),
            InternalStream::Unix(ref mut s) => s.flush(),
        }
    }
}

impl Timeout for InternalStream {
    fn set_read_timeout(&mut self, timeout_ms: Option<u64>) {
        match *self {
            InternalStream::Tcp(ref mut s) => s.set_read_timeout(timeout_ms),
            InternalStream::Unix(ref mut s) => s.set_read_timeout(timeout_ms),
        }
    }
}

fn open_socket(params: &ConnectParams) -> Result<InternalStream, ConnectError> {
    let port = params.port.unwrap_or(DEFAULT_PORT);
    match params.target {
        ConnectTarget::Tcp(ref host) =>
            Ok(try!(TcpStream::connect((host[], port)).map(InternalStream::Tcp))),
        ConnectTarget::Unix(ref path) => {
            let mut path = path.clone();
            path.push(format!(".s.PGSQL.{}", port));
            Ok(try!(UnixStream::connect(&path).map(InternalStream::Unix)))
        }
    }
}

pub fn initialize_stream(params: &ConnectParams, ssl: &SslMode)
                         -> Result<MaybeSslStream<InternalStream>, ConnectError> {
    let mut socket = try!(open_socket(params));

    let (ssl_required, ctx) = match *ssl {
        SslMode::None => return Ok(MaybeSslStream::Normal(socket)),
        SslMode::Prefer(ref ctx) => (false, ctx),
        SslMode::Require(ref ctx) => (true, ctx)
    };

    try!(socket.write_message(&SslRequest { code: message::SSL_CODE }));
    try!(socket.flush());

    if try!(socket.read_u8()) == 'N' as u8 {
        if ssl_required {
            return Err(ConnectError::NoSslSupport);
        } else {
            return Ok(MaybeSslStream::Normal(socket));
        }
    }

    match SslStream::new(ctx, socket) {
        Ok(stream) => Ok(MaybeSslStream::Ssl(stream)),
        Err(err) => Err(ConnectError::SslError(err))
    }
}

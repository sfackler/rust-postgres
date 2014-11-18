use openssl::ssl;
use std::io::net::ip::Port;
use std::io::net::tcp;
use std::io::net::pipe;
use std::io::{Stream, IoResult};

use {ConnectParams, SslMode, ConnectTarget, ConnectError};
use message;
use message::{SslRequest, WriteMessage};

const DEFAULT_PORT: Port = 5432;

pub enum MaybeSslStream<S> {
    Ssl(ssl::SslStream<S>),
    Normal(S),
}

impl<S: Stream> Reader for MaybeSslStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<uint> {
        match *self {
            MaybeSslStream::Ssl(ref mut s) => s.read(buf),
            MaybeSslStream::Normal(ref mut s) => s.read(buf),
        }
    }
}

impl<S: Stream> Writer for MaybeSslStream<S> {
    fn write(&mut self, buf: &[u8]) -> IoResult<()> {
        match *self {
            MaybeSslStream::Ssl(ref mut s) => s.write(buf),
            MaybeSslStream::Normal(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        match *self {
            MaybeSslStream::Ssl(ref mut s) => s.flush(),
            MaybeSslStream::Normal(ref mut s) => s.flush(),
        }
    }
}

pub enum InternalStream {
    TcpStream(tcp::TcpStream),
    UnixStream(pipe::UnixStream),
}

impl Reader for InternalStream {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<uint> {
        match *self {
            TcpStream(ref mut s) => s.read(buf),
            UnixStream(ref mut s) => s.read(buf),
        }
    }
}

impl Writer for InternalStream {
    fn write(&mut self, buf: &[u8]) -> IoResult<()> {
        match *self {
            TcpStream(ref mut s) => s.write(buf),
            UnixStream(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        match *self {
            TcpStream(ref mut s) => s.flush(),
            UnixStream(ref mut s) => s.flush(),
        }
    }
}

fn open_socket(params: &ConnectParams) -> Result<InternalStream, ConnectError> {
    let port = params.port.unwrap_or(DEFAULT_PORT);
    match params.target {
        ConnectTarget::Tcp(ref host) =>
            Ok(try!(tcp::TcpStream::connect((host[], port)).map(TcpStream))),
        ConnectTarget::Unix(ref path) => {
            let mut path = path.clone();
            path.push(format!(".s.PGSQL.{}", port));
            Ok(try!(pipe::UnixStream::connect(&path).map(UnixStream)))
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

    match ssl::SslStream::new(ctx, socket) {
        Ok(stream) => Ok(MaybeSslStream::Ssl(stream)),
        Err(err) => Err(ConnectError::SslError(err))
    }
}

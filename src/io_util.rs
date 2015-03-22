use openssl::ssl::{SslStream, MaybeSslStream};
use std::io;
use std::io::prelude::*;
use std::net::TcpStream;
#[cfg(feature = "unix_socket")]
use unix_socket::UnixStream;
use byteorder::ReadBytesExt;

use {ConnectParams, SslMode, ConnectTarget, ConnectError};
use message;
use message::WriteMessage;
use message::FrontendMessage::SslRequest;

const DEFAULT_PORT: u16 = 5432;

pub enum InternalStream {
    Tcp(TcpStream),
    #[cfg(feature = "unix_socket")]
    Unix(UnixStream),
}

impl Read for InternalStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match *self {
            InternalStream::Tcp(ref mut s) => s.read(buf),
            #[cfg(feature = "unix_socket")]
            InternalStream::Unix(ref mut s) => s.read(buf),
        }
    }
}

impl Write for InternalStream {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match *self {
            InternalStream::Tcp(ref mut s) => s.write(buf),
            #[cfg(feature = "unix_socket")]
            InternalStream::Unix(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match *self {
            InternalStream::Tcp(ref mut s) => s.flush(),
            #[cfg(feature = "unix_socket")]
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
        #[cfg(feature = "unix_socket")]
        ConnectTarget::Unix(ref path) => {
            let mut path = path.clone();
            path.push(&format!(".s.PGSQL.{}", port));
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

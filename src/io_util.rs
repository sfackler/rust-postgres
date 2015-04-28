use openssl::ssl::{SslStream, SslContext};
use std::error::Error;
use std::io;
use std::io::prelude::*;
use std::net::TcpStream;
#[cfg(feature = "unix_socket")]
use unix_socket::UnixStream;
use byteorder::ReadBytesExt;

use {ConnectParams, ConnectTarget, ConnectError};
use message;
use message::WriteMessage;
use message::FrontendMessage::SslRequest;

const DEFAULT_PORT: u16 = 5432;

pub struct Stream(InternalStream);

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

impl StreamWrapper for Stream {
    fn get_ref(&self) -> &Stream {
        self
    }

    fn get_mut(&mut self) -> &mut Stream {
        self
    }
}

pub trait StreamWrapper: Read+Write+Send {
    fn get_ref(&self) -> &Stream;
    fn get_mut(&mut self) -> &mut Stream;
}

impl StreamWrapper for SslStream<Stream> {
    fn get_ref(&self) -> &Stream {
        self.get_ref()
    }

    fn get_mut(&mut self) -> &mut Stream {
        self.get_mut()
    }
}

pub trait NegotiateSsl {
    fn negotiate_ssl(&mut self, host: &str, stream: Stream)
                     -> Result<Box<StreamWrapper>, Box<Error>>;
}

impl NegotiateSsl for SslContext {
    fn negotiate_ssl(&mut self, _: &str, stream: Stream)
                     -> Result<Box<StreamWrapper>, Box<Error>> {
        let stream = try!(SslStream::new(self, stream));
        Ok(Box::new(stream))
    }
}

/// Specifies the SSL support requested for a new connection.
pub enum SslMode<N = NoSsl> {
    /// The connection will not use SSL.
    None,
    /// The connection will use SSL if the backend supports it.
    Prefer(N),
    /// The connection must use SSL.
    Require(N),
}

pub enum NoSsl {}

impl NegotiateSsl for NoSsl {
    fn negotiate_ssl(&mut self, _: &str, _: Stream) -> Result<Box<StreamWrapper>, Box<Error>> {
        match *self {}
    }
}

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

pub fn initialize_stream<N>(params: &ConnectParams, ssl: &mut SslMode<N>)
                            -> Result<Box<StreamWrapper>, ConnectError>
        where N: NegotiateSsl {
    let mut socket = Stream(try!(open_socket(params)));

    let (ssl_required, negotiator) = match *ssl {
        SslMode::None => return Ok(Box::new(socket)),
        SslMode::Prefer(ref mut negotiator) => (false, negotiator),
        SslMode::Require(ref mut negotiator) => (true, negotiator),
    };

    try!(socket.write_message(&SslRequest { code: message::SSL_CODE }));
    try!(socket.flush());

    if try!(socket.read_u8()) == 'N' as u8 {
        if ssl_required {
            return Err(ConnectError::NoSslSupport);
        } else {
            return Ok(Box::new(socket));
        }
    }

    // Postgres doesn't support SSL over unix sockets
    let host = match params.target {
        ConnectTarget::Tcp(ref host) => host,
        #[cfg(feature = "unix_socket")]
        ConnectTarget::Unix(_) => return Err(ConnectError::BadResponse)
    };

    match negotiator.negotiate_ssl(host, socket) {
        Ok(stream) => Ok(stream),
        Err(err) => Err(ConnectError::SslError(err))
    }
}

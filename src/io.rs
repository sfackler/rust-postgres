use openssl::ssl::SslStream;
use std::io::net::ip::Port;
use std::io::net::tcp::TcpStream;
use std::io::net::unix::UnixStream;
use std::io::{Stream, IoResult};

use {ConnectParams,
     SslMode,
     NoSsl,
     PreferSsl,
     RequireSsl,
     TargetTcp,
     TargetUnix};
use error::{PostgresConnectError,
            PgConnectStreamError,
            NoSslSupport,
            SslError,
            SocketError};
use message;
use message::{SslRequest, WriteMessage};

static DEFAULT_PORT: Port = 5432;

pub enum MaybeSslStream<S> {
    SslStream(SslStream<S>),
    NormalStream(S),
}

impl<S: Stream> Reader for MaybeSslStream<S> {
    fn read(&mut self, buf: &mut [u8]) -> IoResult<uint> {
        match *self {
            SslStream(ref mut s) => s.read(buf),
            NormalStream(ref mut s) => s.read(buf),
        }
    }
}

impl<S: Stream> Writer for MaybeSslStream<S> {
    fn write(&mut self, buf: &[u8]) -> IoResult<()> {
        match *self {
            SslStream(ref mut s) => s.write(buf),
            NormalStream(ref mut s) => s.write(buf),
        }
    }

    fn flush(&mut self) -> IoResult<()> {
        match *self {
            SslStream(ref mut s) => s.flush(),
            NormalStream(ref mut s) => s.flush(),
        }
    }
}

pub enum InternalStream {
    TcpStream(TcpStream),
    UnixStream(UnixStream),
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

fn open_socket(params: &ConnectParams) -> Result<InternalStream, PostgresConnectError> {
    let port = params.port.unwrap_or(DEFAULT_PORT);
    let socket = match params.target {
        TargetTcp(ref host) =>
            TcpStream::connect(host.as_slice(), port).map(|s| TcpStream(s)),
        TargetUnix(ref path) => {
            let mut path = path.clone();
            path.push(format!(".s.PGSQL.{}", port));
            UnixStream::connect(&path).map(|s| UnixStream(s))
        }
    };
    socket.map_err(|e| SocketError(e))
}

pub fn initialize_stream(params: &ConnectParams, ssl: &SslMode)
                         -> Result<MaybeSslStream<InternalStream>, PostgresConnectError> {
    let mut socket = try!(open_socket(params));

    let (ssl_required, ctx) = match *ssl {
        NoSsl => return Ok(NormalStream(socket)),
        PreferSsl(ref ctx) => (false, ctx),
        RequireSsl(ref ctx) => (true, ctx)
    };

    try_pg_conn!(socket.write_message(&SslRequest { code: message::SSL_CODE }));
    try_pg_conn!(socket.flush());

    if try_pg_conn!(socket.read_u8()) == 'N' as u8 {
        if ssl_required {
            return Err(NoSslSupport);
        } else {
            return Ok(NormalStream(socket));
        }
    }

    match SslStream::try_new(ctx, socket) {
        Ok(stream) => Ok(SslStream(stream)),
        Err(err) => Err(SslError(err))
    }
}

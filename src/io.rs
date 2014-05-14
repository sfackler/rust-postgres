use openssl::ssl::SslStream;
use std::io::net::ip::Port;
use std::io::net::tcp::TcpStream;
use std::io::net::unix::UnixStream;
use std::io::{Stream, IoResult};

use {PostgresConnectParams,
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

fn open_tcp_socket(host: &str, port: Port) -> Result<TcpStream,
                                                     PostgresConnectError> {
    TcpStream::connect(host, port).map_err(|e| SocketError(e))
}

fn open_unix_socket(path: &Path, port: Port) -> Result<UnixStream,
                                                       PostgresConnectError> {
    let mut socket = path.clone();
    socket.push(format!(".s.PGSQL.{}", port));

    match UnixStream::connect(&socket) {
        Ok(unix) => Ok(unix),
        Err(err) => Err(SocketError(err))
    }
}

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

fn open_socket(params: &PostgresConnectParams)
               -> Result<InternalStream, PostgresConnectError> {
    let port = params.port.unwrap_or(DEFAULT_PORT);
    match params.target {
        TargetTcp(ref host) => open_tcp_socket(host.as_slice(), port)
                .map(|s| TcpStream(s)),
        TargetUnix(ref path) => open_unix_socket(path, port)
                .map(|s| UnixStream(s)),
    }
}

pub fn initialize_stream(params: &PostgresConnectParams, ssl: &SslMode)
                         -> Result<MaybeSslStream<InternalStream>,
                                   PostgresConnectError> {
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

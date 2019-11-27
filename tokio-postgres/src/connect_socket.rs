use crate::config::Host;
use crate::{Error, Socket};
use std::future::Future;
use std::io;
use std::time::Duration;
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::time;

pub(crate) async fn connect_socket(
    host: &Host,
    port: u16,
    connect_timeout: Option<Duration>,
    keepalives: bool,
    keepalives_idle: Duration,
) -> Result<Socket, Error> {
    match host {
        Host::Tcp(host) => {
            let socket =
                connect_with_timeout(TcpStream::connect((&**host, port)), connect_timeout).await?;
            socket.set_nodelay(true).map_err(Error::connect)?;
            if keepalives {
                socket
                    .set_keepalive(Some(keepalives_idle))
                    .map_err(Error::connect)?;
            }

            Ok(Socket::new_tcp(socket))
        }
        #[cfg(unix)]
        Host::Unix(path) => {
            let path = path.join(format!(".s.PGSQL.{}", port));
            let socket = connect_with_timeout(UnixStream::connect(path), connect_timeout).await?;
            Ok(Socket::new_unix(socket))
        }
    }
}

async fn connect_with_timeout<F, T>(connect: F, timeout: Option<Duration>) -> Result<T, Error>
where
    F: Future<Output = io::Result<T>>,
{
    match timeout {
        Some(timeout) => match time::timeout(timeout, connect).await {
            Ok(Ok(socket)) => Ok(socket),
            Ok(Err(e)) => Err(Error::connect(e)),
            Err(_) => Err(Error::connect(io::Error::new(
                io::ErrorKind::TimedOut,
                "connection timed out",
            ))),
        },
        None => match connect.await {
            Ok(socket) => Ok(socket),
            Err(e) => Err(Error::connect(e)),
        },
    }
}

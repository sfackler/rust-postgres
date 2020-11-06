use crate::config::Host;
use crate::{Error, Socket};
use socket2::{Domain, Protocol, Type};
use std::future::Future;
use std::io;
use std::net::SocketAddr;
#[cfg(unix)]
use std::os::unix::io::{FromRawFd, IntoRawFd};
#[cfg(windows)]
use std::os::windows::io::{FromRawSocket, IntoRawSocket};
use std::time::Duration;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::net::{self, TcpSocket};
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
            let addrs = net::lookup_host((&**host, port))
                .await
                .map_err(Error::connect)?;

            let mut last_err = None;

            for addr in addrs {
                let domain = match addr {
                    SocketAddr::V4(_) => Domain::ipv4(),
                    SocketAddr::V6(_) => Domain::ipv6(),
                };

                let socket = socket2::Socket::new(domain, Type::stream(), Some(Protocol::tcp()))
                    .map_err(Error::connect)?;
                socket.set_nonblocking(true).map_err(Error::connect)?;
                socket.set_nodelay(true).map_err(Error::connect)?;
                if keepalives {
                    socket
                        .set_keepalive(Some(keepalives_idle))
                        .map_err(Error::connect)?;
                }

                #[cfg(unix)]
                let socket = unsafe { TcpSocket::from_raw_fd(socket.into_raw_fd()) };
                #[cfg(windows)]
                let socket = unsafe { TcpSocket::from_raw_socket(socket.into_raw_socket()) };

                match connect_with_timeout(socket.connect(addr), connect_timeout).await {
                    Ok(socket) => return Ok(Socket::new_tcp(socket)),
                    Err(e) => last_err = Some(e),
                }
            }

            Err(last_err.unwrap_or_else(|| {
                Error::connect(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "could not resolve any addresses",
                ))
            }))
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

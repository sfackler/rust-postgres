use crate::config::Host;
use crate::{Error, Socket};
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::time::Duration;
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::timer::Timeout;

pub(crate) async fn connect_socket(
    host: &Host,
    port: u16,
    connect_timeout: Option<Duration>,
    keepalives: bool,
    keepalives_idle: Duration,
) -> Result<Socket, Error> {
    match host {
        Host::Tcp(host) => {
            let addrs = match host.parse::<IpAddr>() {
                Ok(ip) => {
                    // avoid dealing with blocking DNS entirely if possible
                    vec![SocketAddr::new(ip, port)].into_iter()
                }
                Err(_) => {
                    // FIXME what do?
                    (&**host, port).to_socket_addrs().map_err(Error::connect)?
                }
            };

            let mut error = None;
            for addr in addrs {
                let new_error =
                    match connect_with_timeout(TcpStream::connect(&addr), connect_timeout).await {
                        Ok(socket) => {
                            socket.set_nodelay(true).map_err(Error::connect)?;
                            if keepalives {
                                socket
                                    .set_keepalive(Some(keepalives_idle))
                                    .map_err(Error::connect)?;
                            }

                            return Ok(Socket::new_tcp(socket));
                        }
                        Err(e) => e,
                    };
                error = Some(new_error);
            }

            let error = error.unwrap_or_else(|| {
                Error::connect(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "resolved 0 addresses",
                ))
            });
            Err(error)
        }
        #[cfg(unix)]
        Host::Unix(path) => {
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
        Some(timeout) => match Timeout::new(connect, timeout).await {
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

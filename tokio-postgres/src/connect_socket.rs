use crate::config::Host;
use crate::{Error, Socket};
use futures::channel::oneshot;
use futures::future;
use std::future::Future;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use std::time::Duration;
use std::vec;
use std::{io, thread};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::timer::Timeout;
use tokio_executor::threadpool;

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
                Err(_) => dns(host, port).await.map_err(Error::connect)?,
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

async fn dns(host: &str, port: u16) -> io::Result<vec::IntoIter<SocketAddr>> {
    // if we're running on a threadpool, use its blocking support
    if let Ok(r) =
        future::poll_fn(|_| threadpool::blocking(|| (host, port).to_socket_addrs())).await
    {
        return r;
    }

    // FIXME what should we do here?
    let (tx, rx) = oneshot::channel();
    let host = host.to_string();
    thread::spawn(move || {
        let addrs = (&*host, port).to_socket_addrs();
        let _ = tx.send(addrs);
    });

    rx.await.unwrap()
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

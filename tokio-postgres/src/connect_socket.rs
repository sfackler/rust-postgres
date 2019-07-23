use crate::config::Host;
use crate::{Config, Error, Socket};
use std::future::Future;
use std::io;
use std::net::{IpAddr, SocketAddr, ToSocketAddrs};
use tokio::net::TcpStream;
#[cfg(unix)]
use tokio::net::UnixStream;
use tokio::timer::Timeout;

pub async fn connect_socket(idx: usize, config: &Config) -> Result<Socket, Error> {
    let port = *config
        .port
        .get(idx)
        .or_else(|| config.port.get(0))
        .unwrap_or(&5432);

    match &config.host[idx] {
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
                let new_error = match connect_timeout(TcpStream::connect(&addr), config).await {
                    Ok(socket) => return Ok(Socket::new_tcp(socket)),
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
            let socket = connect_timeout(UnixStream::connect(path), config).await?;
            Ok(Socket::new_unix(socket))
        }
    }
}

async fn connect_timeout<F, T>(connect: F, config: &Config) -> Result<T, Error>
where
    F: Future<Output = io::Result<T>>,
{
    match config.connect_timeout {
        Some(connect_timeout) => match Timeout::new(connect, connect_timeout).await {
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

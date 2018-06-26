use bytes::{Buf, BufMut};
use futures::{Async, Future, Poll};
use futures_cpupool::{CpuFuture, CpuPool};
use state_machine_future::RentToOwn;
use std::io::{self, Read, Write};
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::{Duration, Instant};
use std::vec;
use tokio_io::{AsyncRead, AsyncWrite};
use tokio_tcp::{self, TcpStream};
use tokio_timer::Delay;

#[cfg(unix)]
use tokio_uds::{self, UnixStream};

use params::{ConnectParams, Host};

lazy_static! {
    static ref DNS_POOL: CpuPool = CpuPool::new(2);
}

pub enum Socket {
    Tcp(TcpStream),
    #[cfg(unix)]
    Unix(UnixStream),
}

impl Read for Socket {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        match self {
            Socket::Tcp(stream) => stream.read(buf),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.read(buf),
        }
    }
}

impl AsyncRead for Socket {
    unsafe fn prepare_uninitialized_buffer(&self, buf: &mut [u8]) -> bool {
        match self {
            Socket::Tcp(stream) => stream.prepare_uninitialized_buffer(buf),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.prepare_uninitialized_buffer(buf),
        }
    }

    fn read_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: BufMut,
    {
        match self {
            Socket::Tcp(stream) => stream.read_buf(buf),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.read_buf(buf),
        }
    }
}

impl Write for Socket {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        match self {
            Socket::Tcp(stream) => stream.write(buf),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.write(buf),
        }
    }

    fn flush(&mut self) -> io::Result<()> {
        match self {
            Socket::Tcp(stream) => stream.flush(),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.flush(),
        }
    }
}

impl AsyncWrite for Socket {
    fn shutdown(&mut self) -> Poll<(), io::Error> {
        match self {
            Socket::Tcp(stream) => stream.shutdown(),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.shutdown(),
        }
    }

    fn write_buf<B>(&mut self, buf: &mut B) -> Poll<usize, io::Error>
    where
        B: Buf,
    {
        match self {
            Socket::Tcp(stream) => stream.write_buf(buf),
            #[cfg(unix)]
            Socket::Unix(stream) => stream.write_buf(buf),
        }
    }
}

impl Socket {
    pub fn connect(params: &ConnectParams) -> ConnectFuture {
        Connect::start(params.clone())
    }
}

#[derive(StateMachineFuture)]
pub enum Connect {
    #[state_machine_future(start)]
    #[cfg_attr(unix, state_machine_future(transitions(ResolvingDns, ConnectingUnix)))]
    #[cfg_attr(not(unix), state_machine_future(transitions(ResolvingDns)))]
    Start { params: ConnectParams },
    #[state_machine_future(transitions(ConnectingTcp))]
    ResolvingDns {
        future: CpuFuture<vec::IntoIter<SocketAddr>, io::Error>,
        timeout: Option<Duration>,
    },
    #[state_machine_future(transitions(Ready))]
    ConnectingTcp {
        addrs: vec::IntoIter<SocketAddr>,
        future: tokio_tcp::ConnectFuture,
        timeout: Option<(Duration, Delay)>,
    },
    #[cfg(unix)]
    #[state_machine_future(transitions(Ready))]
    ConnectingUnix {
        future: tokio_uds::ConnectFuture,
        timeout: Option<Delay>,
    },
    #[state_machine_future(ready)]
    Ready(Socket),
    #[state_machine_future(error)]
    Failed(io::Error),
}

impl PollConnect for Connect {
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start>) -> Poll<AfterStart, io::Error> {
        let timeout = state.params.connect_timeout();
        let port = state.params.port();

        match state.params.host() {
            Host::Tcp(ref host) => {
                let host = host.clone();
                transition!(ResolvingDns {
                    future: DNS_POOL.spawn_fn(move || (&*host, port).to_socket_addrs()),
                    timeout,
                })
            }
            #[cfg(unix)]
            Host::Unix(ref path) => {
                let path = path.join(format!(".s.PGSQL.{}", port));
                transition!(ConnectingUnix {
                    future: UnixStream::connect(path),
                    timeout: timeout.map(|t| Delay::new(Instant::now() + t))
                })
            }
        }
    }

    fn poll_resolving_dns<'a>(
        state: &'a mut RentToOwn<'a, ResolvingDns>,
    ) -> Poll<AfterResolvingDns, io::Error> {
        let mut addrs = try_ready!(state.future.poll());

        let addr = match addrs.next() {
            Some(addr) => addr,
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::Other,
                    "resolved to 0 addresses",
                ))
            }
        };

        transition!(ConnectingTcp {
            addrs,
            future: TcpStream::connect(&addr),
            timeout: state.timeout.map(|t| (t, Delay::new(Instant::now() + t))),
        })
    }

    fn poll_connecting_tcp<'a>(
        state: &'a mut RentToOwn<'a, ConnectingTcp>,
    ) -> Poll<AfterConnectingTcp, io::Error> {
        loop {
            let error = match state.future.poll() {
                Ok(Async::Ready(socket)) => transition!(Ready(Socket::Tcp(socket))),
                Ok(Async::NotReady) => match state.timeout {
                    Some((_, ref mut delay)) => {
                        try_ready!(
                            delay
                                .poll()
                                .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                        );
                        io::Error::new(io::ErrorKind::TimedOut, "connection timed out")
                    }
                    None => return Ok(Async::NotReady),
                },
                Err(e) => e,
            };

            let addr = match state.addrs.next() {
                Some(addr) => addr,
                None => return Err(error),
            };

            state.future = TcpStream::connect(&addr);
            if let Some((timeout, ref mut delay)) = state.timeout {
                delay.reset(Instant::now() + timeout);
            }
        }
    }

    #[cfg(unix)]
    fn poll_connecting_unix<'a>(
        state: &'a mut RentToOwn<'a, ConnectingUnix>,
    ) -> Poll<AfterConnectingUnix, io::Error> {
        match state.future.poll()? {
            Async::Ready(socket) => transition!(Ready(Socket::Unix(socket))),
            Async::NotReady => match state.timeout {
                Some(ref mut delay) => {
                    try_ready!(
                        delay
                            .poll()
                            .map_err(|e| io::Error::new(io::ErrorKind::Other, e))
                    );
                    Err(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "connection timed out",
                    ))
                }
                None => Ok(Async::NotReady),
            },
        }
    }
}

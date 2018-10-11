use futures::{Async, Future, Poll};
use futures_cpupool::{CpuFuture, CpuPool};
use postgres_protocol::message::frontend;
use state_machine_future::RentToOwn;
use std::error::Error as StdError;
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::{Duration, Instant};
use std::vec;
use tokio_io::io::{read_exact, write_all, ReadExact, WriteAll};
use tokio_tcp::{self, TcpStream};
use tokio_timer::Delay;

#[cfg(unix)]
use tokio_uds::{self, UnixStream};

use params::{ConnectParams, Host};
use proto::socket::Socket;
use tls::{self, TlsConnect, TlsStream};
use {Error, TlsMode};

lazy_static! {
    static ref DNS_POOL: CpuPool = CpuPool::new(2);
}

#[derive(StateMachineFuture)]
pub enum Connect {
    #[state_machine_future(start)]
    #[cfg_attr(
        unix,
        state_machine_future(transitions(ResolvingDns, ConnectingUnix))
    )]
    #[cfg_attr(not(unix), state_machine_future(transitions(ResolvingDns)))]
    Start { params: ConnectParams, tls: TlsMode },
    #[state_machine_future(transitions(ConnectingTcp))]
    ResolvingDns {
        future: CpuFuture<vec::IntoIter<SocketAddr>, io::Error>,
        timeout: Option<Duration>,
        params: ConnectParams,
        tls: TlsMode,
    },
    #[state_machine_future(transitions(PreparingSsl))]
    ConnectingTcp {
        addrs: vec::IntoIter<SocketAddr>,
        future: tokio_tcp::ConnectFuture,
        timeout: Option<(Duration, Delay)>,
        params: ConnectParams,
        tls: TlsMode,
    },
    #[cfg(unix)]
    #[state_machine_future(transitions(PreparingSsl))]
    ConnectingUnix {
        future: tokio_uds::ConnectFuture,
        timeout: Option<Delay>,
        params: ConnectParams,
        tls: TlsMode,
    },
    #[state_machine_future(transitions(Ready, SendingSsl))]
    PreparingSsl {
        socket: Socket,
        params: ConnectParams,
        tls: TlsMode,
    },
    #[state_machine_future(transitions(ReadingSsl))]
    SendingSsl {
        future: WriteAll<Socket, Vec<u8>>,
        params: ConnectParams,
        connector: Box<TlsConnect + Send>,
        required: bool,
    },
    #[state_machine_future(transitions(ConnectingTls, Ready))]
    ReadingSsl {
        future: ReadExact<Socket, [u8; 1]>,
        params: ConnectParams,
        connector: Box<TlsConnect + Send>,
        required: bool,
    },
    #[state_machine_future(transitions(Ready))]
    ConnectingTls {
        future:
            Box<Future<Item = Box<TlsStream + Send>, Error = Box<StdError + Sync + Send>> + Sync + Send>,
        params: ConnectParams,
    },
    #[state_machine_future(ready)]
    Ready(Box<TlsStream>),
    #[state_machine_future(error)]
    Failed(Error),
}

impl PollConnect for Connect {
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start>) -> Poll<AfterStart, Error> {
        let state = state.take();

        let timeout = state.params.connect_timeout();
        let port = state.params.port();

        match state.params.host().clone() {
            Host::Tcp(host) => transition!(ResolvingDns {
                future: DNS_POOL.spawn_fn(move || (&*host, port).to_socket_addrs()),
                params: state.params,
                tls: state.tls,
                timeout,
            }),
            #[cfg(unix)]
            Host::Unix(mut path) => {
                path.push(format!(".s.PGSQL.{}", port));
                transition!(ConnectingUnix {
                    future: UnixStream::connect(path),
                    timeout: timeout.map(|t| Delay::new(Instant::now() + t)),
                    params: state.params,
                    tls: state.tls,
                })
            },
            #[cfg(not(unix))]
            Host::Unix(_) => {
                Err(Error::connect(io::Error::new(
                    io::ErrorKind::Other,
                    "unix sockets are not supported on this platform",
                )))
            },
        }
    }

    fn poll_resolving_dns<'a>(
        state: &'a mut RentToOwn<'a, ResolvingDns>,
    ) -> Poll<AfterResolvingDns, Error> {
        let mut addrs = try_ready!(state.future.poll().map_err(Error::connect));
        let state = state.take();

        let addr = match addrs.next() {
            Some(addr) => addr,
            None => {
                return Err(Error::connect(io::Error::new(
                    io::ErrorKind::Other,
                    "resolved to 0 addresses",
                )))
            }
        };

        transition!(ConnectingTcp {
            addrs,
            future: TcpStream::connect(&addr),
            timeout: state.timeout.map(|t| (t, Delay::new(Instant::now() + t))),
            params: state.params,
            tls: state.tls,
        })
    }

    fn poll_connecting_tcp<'a>(
        state: &'a mut RentToOwn<'a, ConnectingTcp>,
    ) -> Poll<AfterConnectingTcp, Error> {
        let socket = loop {
            let error = match state.future.poll() {
                Ok(Async::Ready(socket)) => break socket,
                Ok(Async::NotReady) => match state.timeout {
                    Some((_, ref mut delay)) => {
                        try_ready!(delay.poll().map_err(Error::timer));
                        io::Error::new(io::ErrorKind::TimedOut, "connection timed out")
                    }
                    None => return Ok(Async::NotReady),
                },
                Err(e) => e,
            };

            let addr = match state.addrs.next() {
                Some(addr) => addr,
                None => return Err(Error::connect(error)),
            };

            state.future = TcpStream::connect(&addr);
            if let Some((timeout, ref mut delay)) = state.timeout {
                delay.reset(Instant::now() + timeout);
            }
        };

        // Our read/write patterns may trigger Nagle's algorithm since we're pipelining which
        // we don't want. Each individual write should be a full command we want the backend to
        // see immediately.
        socket.set_nodelay(true).map_err(Error::connect)?;

        let state = state.take();
        transition!(PreparingSsl {
            socket: Socket::Tcp(socket),
            params: state.params,
            tls: state.tls,
        })
    }

    #[cfg(unix)]
    fn poll_connecting_unix<'a>(
        state: &'a mut RentToOwn<'a, ConnectingUnix>,
    ) -> Poll<AfterConnectingUnix, Error> {
        match state.future.poll().map_err(Error::connect)? {
            Async::Ready(socket) => {
                let state = state.take();
                transition!(PreparingSsl {
                    socket: Socket::Unix(socket),
                    params: state.params,
                    tls: state.tls,
                })
            }
            Async::NotReady => match state.timeout {
                Some(ref mut delay) => {
                    try_ready!(delay.poll().map_err(Error::timer));
                    Err(Error::connect(io::Error::new(
                        io::ErrorKind::TimedOut,
                        "connection timed out",
                    )))
                }
                None => Ok(Async::NotReady),
            },
        }
    }

    fn poll_preparing_ssl<'a>(
        state: &'a mut RentToOwn<'a, PreparingSsl>,
    ) -> Poll<AfterPreparingSsl, Error> {
        let state = state.take();

        let (connector, required) = match state.tls {
            TlsMode::None => {
                transition!(Ready(Box::new(state.socket)));
            }
            TlsMode::Prefer(connector) => (connector, false),
            TlsMode::Require(connector) => (connector, true),
        };

        let mut buf = vec![];
        frontend::ssl_request(&mut buf);
        transition!(SendingSsl {
            future: write_all(state.socket, buf),
            params: state.params,
            connector,
            required,
        })
    }

    fn poll_sending_ssl<'a>(
        state: &'a mut RentToOwn<'a, SendingSsl>,
    ) -> Poll<AfterSendingSsl, Error> {
        let (stream, _) = try_ready_closed!(state.future.poll());
        let state = state.take();
        transition!(ReadingSsl {
            future: read_exact(stream, [0]),
            params: state.params,
            connector: state.connector,
            required: state.required,
        })
    }

    fn poll_reading_ssl<'a>(
        state: &'a mut RentToOwn<'a, ReadingSsl>,
    ) -> Poll<AfterReadingSsl, Error> {
        let (stream, buf) = try_ready_closed!(state.future.poll());
        let state = state.take();

        match buf[0] {
            b'S' => {
                let future = match state.params.host() {
                    Host::Tcp(domain) => state.connector.connect(domain, tls::Socket(stream)),
                    Host::Unix(_) => {
                        return Err(Error::tls("TLS over unix sockets not supported".into()))
                    }
                };
                transition!(ConnectingTls {
                    future,
                    params: state.params,
                })
            }
            b'N' if !state.required => transition!(Ready(Box::new(stream))),
            b'N' => Err(Error::tls("TLS was required but not supported".into())),
            _ => Err(Error::unexpected_message()),
        }
    }

    fn poll_connecting_tls<'a>(
        state: &'a mut RentToOwn<'a, ConnectingTls>,
    ) -> Poll<AfterConnectingTls, Error> {
        let stream = try_ready!(state.future.poll().map_err(Error::tls));
        transition!(Ready(stream))
    }
}

impl ConnectFuture {
    pub fn new(params: ConnectParams, tls: TlsMode) -> ConnectFuture {
        Connect::start(params, tls)
    }
}

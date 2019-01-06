use futures::{try_ready, Async, Future, Poll};
use futures_cpupool::{CpuFuture, CpuPool};
use lazy_static::lazy_static;
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
use std::time::Instant;
use std::vec;
use tokio_tcp::TcpStream;
use tokio_timer::Delay;
#[cfg(unix)]
use tokio_uds::UnixStream;

use crate::{Config, Error, Host, Socket};

lazy_static! {
    static ref DNS_POOL: CpuPool = futures_cpupool::Builder::new()
        .name_prefix("postgres-dns-")
        .pool_size(2)
        .create();
}

#[derive(StateMachineFuture)]
pub enum ConnectSocket {
    #[state_machine_future(start)]
    #[cfg_attr(unix, state_machine_future(transitions(ConnectingUnix, ResolvingDns)))]
    #[cfg_attr(not(unix), state_machine_future(transitions(ResolvingDns)))]
    Start { config: Config, idx: usize },
    #[cfg(unix)]
    #[state_machine_future(transitions(Finished))]
    ConnectingUnix {
        future: tokio_uds::ConnectFuture,
        timeout: Option<Delay>,
    },
    #[state_machine_future(transitions(ConnectingTcp))]
    ResolvingDns {
        future: CpuFuture<vec::IntoIter<SocketAddr>, io::Error>,
        config: Config,
    },
    #[state_machine_future(transitions(Finished))]
    ConnectingTcp {
        future: tokio_tcp::ConnectFuture,
        timeout: Option<Delay>,
        addrs: vec::IntoIter<SocketAddr>,
        config: Config,
    },
    #[state_machine_future(ready)]
    Finished(Socket),
    #[state_machine_future(error)]
    Failed(Error),
}

impl PollConnectSocket for ConnectSocket {
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start>) -> Poll<AfterStart, Error> {
        let state = state.take();

        let port = *state
            .config
            .0
            .port
            .get(state.idx)
            .or_else(|| state.config.0.port.get(0))
            .unwrap_or(&5432);

        match &state.config.0.host[state.idx] {
            Host::Tcp(host) => transition!(ResolvingDns {
                future: DNS_POOL.spawn_fn({
                    let host = host.clone();
                    move || (&*host, port).to_socket_addrs()
                }),
                config: state.config,
            }),
            #[cfg(unix)]
            Host::Unix(host) => {
                let path = host.join(format!(".s.PGSQL.{}", port));
                let timeout = state
                    .config
                    .0
                    .connect_timeout
                    .map(|d| Delay::new(Instant::now() + d));
                transition!(ConnectingUnix {
                    future: UnixStream::connect(path),
                    timeout,
                })
            }
        }
    }

    #[cfg(unix)]
    fn poll_connecting_unix<'a>(
        state: &'a mut RentToOwn<'a, ConnectingUnix>,
    ) -> Poll<AfterConnectingUnix, Error> {
        if let Some(timeout) = &mut state.timeout {
            match timeout.poll() {
                Ok(Async::Ready(())) => {
                    return Err(Error::connect(io::Error::from(io::ErrorKind::TimedOut)));
                }
                Ok(Async::NotReady) => {}
                Err(e) => return Err(Error::connect(io::Error::new(io::ErrorKind::Other, e))),
            }
        }
        let socket = try_ready!(state.future.poll().map_err(Error::connect));

        transition!(Finished(Socket::new_unix(socket)))
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
                    io::ErrorKind::InvalidData,
                    "resolved 0 addresses",
                )));
            }
        };

        let timeout = state
            .config
            .0
            .connect_timeout
            .map(|d| Delay::new(Instant::now() + d));

        transition!(ConnectingTcp {
            future: TcpStream::connect(&addr),
            addrs,
            timeout: timeout,
            config: state.config,
        })
    }

    fn poll_connecting_tcp<'a>(
        state: &'a mut RentToOwn<'a, ConnectingTcp>,
    ) -> Poll<AfterConnectingTcp, Error> {
        let stream = loop {
            let error = match state.future.poll() {
                Ok(Async::Ready(stream)) => break stream,
                Ok(Async::NotReady) => match &mut state.timeout {
                    Some(timeout) => {
                        try_ready!(timeout
                            .poll()
                            .map_err(|e| Error::connect(io::Error::new(io::ErrorKind::Other, e))));
                        io::Error::from(io::ErrorKind::TimedOut)
                    }
                    None => return Ok(Async::NotReady),
                },
                Err(e) => e,
            };

            let addr = state.addrs.next().ok_or_else(|| Error::connect(error))?;
            state.future = TcpStream::connect(&addr);
            state.timeout = state
                .config
                .0
                .connect_timeout
                .map(|d| Delay::new(Instant::now() + d));
        };

        stream.set_nodelay(true).map_err(Error::connect)?;
        if state.config.0.keepalives {
            stream
                .set_keepalive(Some(state.config.0.keepalives_idle))
                .map_err(Error::connect)?;
        }

        transition!(Finished(Socket::new_tcp(stream)));
    }
}

impl ConnectSocketFuture {
    pub fn new(config: Config, idx: usize) -> ConnectSocketFuture {
        ConnectSocket::start(config, idx)
    }
}

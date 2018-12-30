#![allow(clippy::large_enum_variant)]

use futures::{try_ready, Async, Future, Poll, Stream};
use futures_cpupool::{CpuFuture, CpuPool};
use lazy_static::lazy_static;
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use std::io;
use std::net::{SocketAddr, ToSocketAddrs};
#[cfg(unix)]
use std::path::Path;
use std::time::Instant;
use std::vec;
use tokio_tcp::TcpStream;
use tokio_timer::Delay;
#[cfg(unix)]
use tokio_uds::UnixStream;

use crate::proto::{Client, Connection, HandshakeFuture, SimpleQueryStream};
use crate::{Config, Error, Host, Socket, TargetSessionAttrs, TlsMode};

lazy_static! {
    static ref DNS_POOL: CpuPool = futures_cpupool::Builder::new()
        .name_prefix("postgres-dns-")
        .pool_size(2)
        .create();
}

#[derive(StateMachineFuture)]
pub enum ConnectOnce<T>
where
    T: TlsMode<Socket>,
{
    #[state_machine_future(start)]
    #[cfg_attr(unix, state_machine_future(transitions(ConnectingUnix, ResolvingDns)))]
    #[cfg_attr(not(unix), state_machine_future(transitions(ConnectingTcp)))]
    Start {
        idx: usize,
        tls_mode: T,
        config: Config,
    },
    #[cfg(unix)]
    #[state_machine_future(transitions(Handshaking))]
    ConnectingUnix {
        future: tokio_uds::ConnectFuture,
        timeout: Option<Delay>,
        tls_mode: T,
        config: Config,
    },
    #[state_machine_future(transitions(ConnectingTcp))]
    ResolvingDns {
        future: CpuFuture<vec::IntoIter<SocketAddr>, io::Error>,
        timeout: Option<Delay>,
        tls_mode: T,
        config: Config,
    },
    #[state_machine_future(transitions(Handshaking))]
    ConnectingTcp {
        future: tokio_tcp::ConnectFuture,
        addrs: vec::IntoIter<SocketAddr>,
        timeout: Option<Delay>,
        tls_mode: T,
        config: Config,
    },
    #[state_machine_future(transitions(CheckingSessionAttrs, Finished))]
    Handshaking {
        future: HandshakeFuture<Socket, T>,
        target_session_attrs: TargetSessionAttrs,
    },
    #[state_machine_future(transitions(Finished))]
    CheckingSessionAttrs {
        stream: SimpleQueryStream,
        client: Client,
        connection: Connection<T::Stream>,
    },
    #[state_machine_future(ready)]
    Finished((Client, Connection<T::Stream>)),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<T> PollConnectOnce<T> for ConnectOnce<T>
where
    T: TlsMode<Socket>,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<T>>) -> Poll<AfterStart<T>, Error> {
        let state = state.take();

        let port = *state
            .config
            .0
            .port
            .get(state.idx)
            .or_else(|| state.config.0.port.get(0))
            .unwrap_or(&5432);

        let timeout = state
            .config
            .0
            .connect_timeout
            .map(|d| Delay::new(Instant::now() + d));

        match &state.config.0.host[state.idx] {
            Host::Tcp(host) => {
                let host = host.clone();
                transition!(ResolvingDns {
                    future: DNS_POOL.spawn_fn(move || (&*host, port).to_socket_addrs()),
                    timeout,
                    tls_mode: state.tls_mode,
                    config: state.config,
                })
            }
            #[cfg(unix)]
            Host::Unix(host) => {
                let path = Path::new(host).join(format!(".s.PGSQL.{}", port));
                transition!(ConnectingUnix {
                    future: UnixStream::connect(path),
                    timeout,
                    tls_mode: state.tls_mode,
                    config: state.config,
                })
            }
        }
    }

    #[cfg(unix)]
    fn poll_connecting_unix<'a>(
        state: &'a mut RentToOwn<'a, ConnectingUnix<T>>,
    ) -> Poll<AfterConnectingUnix<T>, Error> {
        if let Some(timeout) = &mut state.timeout {
            match timeout.poll() {
                Ok(Async::Ready(())) => return Err(Error::connect_timeout()),
                Ok(Async::NotReady) => {}
                Err(e) => return Err(Error::timer(e)),
            }
        }

        let stream = try_ready!(state.future.poll().map_err(Error::connect));
        let stream = Socket::new_unix(stream);
        let state = state.take();

        transition!(Handshaking {
            target_session_attrs: state.config.0.target_session_attrs,
            future: HandshakeFuture::new(stream, state.tls_mode, state.config),
        })
    }

    fn poll_resolving_dns<'a>(
        state: &'a mut RentToOwn<'a, ResolvingDns<T>>,
    ) -> Poll<AfterResolvingDns<T>, Error> {
        if let Some(timeout) = &mut state.timeout {
            match timeout.poll() {
                Ok(Async::Ready(())) => return Err(Error::connect_timeout()),
                Ok(Async::NotReady) => {}
                Err(e) => return Err(Error::timer(e)),
            }
        }

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

        transition!(ConnectingTcp {
            future: TcpStream::connect(&addr),
            addrs,
            timeout: state.timeout,
            tls_mode: state.tls_mode,
            config: state.config,
        })
    }

    fn poll_connecting_tcp<'a>(
        state: &'a mut RentToOwn<'a, ConnectingTcp<T>>,
    ) -> Poll<AfterConnectingTcp<T>, Error> {
        if let Some(timeout) = &mut state.timeout {
            match timeout.poll() {
                Ok(Async::Ready(())) => return Err(Error::connect_timeout()),
                Ok(Async::NotReady) => {}
                Err(e) => return Err(Error::timer(e)),
            }
        }

        let stream = loop {
            match state.future.poll() {
                Ok(Async::Ready(stream)) => break stream,
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => {
                    let addr = match state.addrs.next() {
                        Some(addr) => addr,
                        None => return Err(Error::connect(e)),
                    };
                    state.future = TcpStream::connect(&addr);
                }
            }
        };
        let state = state.take();

        stream.set_nodelay(true).map_err(Error::connect)?;
        if state.config.0.keepalives {
            stream
                .set_keepalive(Some(state.config.0.keepalives_idle))
                .map_err(Error::connect)?;
        }

        let stream = Socket::new_tcp(stream);

        transition!(Handshaking {
            target_session_attrs: state.config.0.target_session_attrs,
            future: HandshakeFuture::new(stream, state.tls_mode, state.config),
        })
    }

    fn poll_handshaking<'a>(
        state: &'a mut RentToOwn<'a, Handshaking<T>>,
    ) -> Poll<AfterHandshaking<T>, Error> {
        let (client, connection) = try_ready!(state.future.poll());

        if let TargetSessionAttrs::ReadWrite = state.target_session_attrs {
            transition!(CheckingSessionAttrs {
                stream: client.batch_execute("SHOW transaction_read_only"),
                client,
                connection,
            })
        } else {
            transition!(Finished((client, connection)))
        }
    }

    fn poll_checking_session_attrs<'a>(
        state: &'a mut RentToOwn<'a, CheckingSessionAttrs<T>>,
    ) -> Poll<AfterCheckingSessionAttrs<T>, Error> {
        if let Async::Ready(()) = state.connection.poll()? {
            return Err(Error::closed());
        }

        match try_ready!(state.stream.poll()) {
            Some(row) => {
                if row.get(0) == Some("on") {
                    Err(Error::read_only_database())
                } else {
                    let state = state.take();
                    transition!(Finished((state.client, state.connection)))
                }
            }
            None => Err(Error::closed()),
        }
    }
}

impl<T> ConnectOnceFuture<T>
where
    T: TlsMode<Socket>,
{
    pub fn new(idx: usize, tls_mode: T, config: Config) -> ConnectOnceFuture<T> {
        ConnectOnce::start(idx, tls_mode, config)
    }
}

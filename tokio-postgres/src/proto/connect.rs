use futures::{Async, Future, Poll};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};

use crate::proto::{Client, ConnectOnceFuture, Connection, MaybeTlsStream};
use crate::{Config, Error, Host, MakeTlsConnect, Socket};

#[derive(StateMachineFuture)]
pub enum Connect<T>
where
    T: MakeTlsConnect<Socket>,
{
    #[state_machine_future(start, transitions(Connecting))]
    Start {
        tls: T,
        config: Result<Config, Error>,
    },
    #[state_machine_future(transitions(Finished))]
    Connecting {
        future: ConnectOnceFuture<T::TlsConnect>,
        idx: usize,
        tls: T,
        config: Config,
    },
    #[state_machine_future(ready)]
    Finished((Client, Connection<MaybeTlsStream<Socket, T::Stream>>)),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<T> PollConnect<T> for Connect<T>
where
    T: MakeTlsConnect<Socket>,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<T>>) -> Poll<AfterStart<T>, Error> {
        let mut state = state.take();

        let config = state.config?;

        if config.0.host.is_empty() {
            return Err(Error::config("host missing".into()));
        }

        if config.0.port.len() > 1 && config.0.port.len() != config.0.host.len() {
            return Err(Error::config("invalid number of ports".into()));
        }

        let hostname = match &config.0.host[0] {
            Host::Tcp(host) => &**host,
            // postgres doesn't support TLS over unix sockets, so the choice here doesn't matter
            #[cfg(unix)]
            Host::Unix(_) => "",
        };
        let tls = state
            .tls
            .make_tls_connect(hostname)
            .map_err(|e| Error::tls(e.into()))?;

        transition!(Connecting {
            future: ConnectOnceFuture::new(0, tls, config.clone()),
            idx: 0,
            tls: state.tls,
            config,
        })
    }

    fn poll_connecting<'a>(
        state: &'a mut RentToOwn<'a, Connecting<T>>,
    ) -> Poll<AfterConnecting<T>, Error> {
        loop {
            match state.future.poll() {
                Ok(Async::Ready(r)) => transition!(Finished(r)),
                Ok(Async::NotReady) => return Ok(Async::NotReady),
                Err(e) => {
                    let state = &mut **state;
                    state.idx += 1;

                    let host = match state.config.0.host.get(state.idx) {
                        Some(host) => host,
                        None => return Err(e),
                    };

                    let hostname = match host {
                        Host::Tcp(host) => &**host,
                        #[cfg(unix)]
                        Host::Unix(_) => "",
                    };
                    let tls = state
                        .tls
                        .make_tls_connect(hostname)
                        .map_err(|e| Error::tls(e.into()))?;

                    state.future = ConnectOnceFuture::new(state.idx, tls, state.config.clone());
                }
            }
        }
    }
}

impl<T> ConnectFuture<T>
where
    T: MakeTlsConnect<Socket>,
{
    pub fn new(tls: T, config: Result<Config, Error>) -> ConnectFuture<T> {
        Connect::start(tls, config)
    }
}

use futures::{try_ready, Async, Future, Poll};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};

use crate::proto::{Client, ConnectOnceFuture, Connection};
use crate::{Builder, Error, Host, MakeTlsMode, Socket};

#[derive(StateMachineFuture)]
pub enum Connect<T>
where
    T: MakeTlsMode<Socket>,
{
    #[state_machine_future(start, transitions(MakingTlsMode))]
    Start { make_tls_mode: T, config: Builder },
    #[state_machine_future(transitions(Connecting))]
    MakingTlsMode {
        future: T::Future,
        idx: usize,
        make_tls_mode: T,
        config: Builder,
    },
    #[state_machine_future(transitions(MakingTlsMode, Finished))]
    Connecting {
        future: ConnectOnceFuture<T::TlsMode>,
        idx: usize,
        make_tls_mode: T,
        config: Builder,
    },
    #[state_machine_future(ready)]
    Finished((Client, Connection<T::Stream>)),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<T> PollConnect<T> for Connect<T>
where
    T: MakeTlsMode<Socket>,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<T>>) -> Poll<AfterStart<T>, Error> {
        let mut state = state.take();

        if state.config.0.host.is_empty() {
            return Err(Error::missing_host());
        }

        if state.config.0.port.len() > 1 && state.config.0.port.len() != state.config.0.host.len() {
            return Err(Error::invalid_port_count());
        }

        let hostname = match &state.config.0.host[0] {
            Host::Tcp(host) => &**host,
            // postgres doesn't support TLS over unix sockets, so the choice here doesn't matter
            #[cfg(unix)]
            Host::Unix(_) => "",
        };
        let future = state.make_tls_mode.make_tls_mode(hostname);

        transition!(MakingTlsMode {
            future,
            idx: 0,
            make_tls_mode: state.make_tls_mode,
            config: state.config,
        })
    }

    fn poll_making_tls_mode<'a>(
        state: &'a mut RentToOwn<'a, MakingTlsMode<T>>,
    ) -> Poll<AfterMakingTlsMode<T>, Error> {
        let tls_mode = try_ready!(state.future.poll().map_err(|e| Error::tls(e.into())));
        let state = state.take();

        transition!(Connecting {
            future: ConnectOnceFuture::new(state.idx, tls_mode, state.config.clone()),
            idx: state.idx,
            make_tls_mode: state.make_tls_mode,
            config: state.config,
        })
    }

    fn poll_connecting<'a>(
        state: &'a mut RentToOwn<'a, Connecting<T>>,
    ) -> Poll<AfterConnecting<T>, Error> {
        match state.future.poll() {
            Ok(Async::Ready(r)) => transition!(Finished(r)),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(e) => {
                let mut state = state.take();
                let idx = state.idx + 1;

                let host = match state.config.0.host.get(idx) {
                    Some(host) => host,
                    None => return Err(e),
                };

                let hostname = match host {
                    Host::Tcp(host) => &**host,
                    #[cfg(unix)]
                    Host::Unix(_) => "",
                };
                let future = state.make_tls_mode.make_tls_mode(hostname);

                transition!(MakingTlsMode {
                    future,
                    idx,
                    make_tls_mode: state.make_tls_mode,
                    config: state.config,
                })
            }
        }
    }
}

impl<T> ConnectFuture<T>
where
    T: MakeTlsMode<Socket>,
{
    pub fn new(make_tls_mode: T, config: Builder) -> ConnectFuture<T> {
        Connect::start(make_tls_mode, config)
    }
}

use futures::{try_ready, Future, Poll};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use std::collections::HashMap;

use crate::proto::{Client, ConnectOnceFuture, Connection};
use crate::{Error, MakeTlsMode, Socket};

#[derive(StateMachineFuture)]
pub enum Connect<T>
where
    T: MakeTlsMode<Socket>,
{
    #[state_machine_future(start, transitions(MakingTlsMode))]
    Start {
        make_tls_mode: T,
        params: HashMap<String, String>,
    },
    #[state_machine_future(transitions(Connecting))]
    MakingTlsMode {
        future: T::Future,
        host: String,
        port: u16,
        params: HashMap<String, String>,
    },
    #[state_machine_future(transitions(Finished))]
    Connecting {
        future: ConnectOnceFuture<T::TlsMode>,
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

        let host = match state.params.remove("host") {
            Some(host) => host,
            None => return Err(Error::missing_host()),
        };

        let port = match state.params.remove("port") {
            Some(port) => port.parse::<u16>().map_err(Error::invalid_port)?,
            None => 5432,
        };

        transition!(MakingTlsMode {
            future: state.make_tls_mode.make_tls_mode(&host),
            host,
            port,
            params: state.params,
        })
    }

    fn poll_making_tls_mode<'a>(
        state: &'a mut RentToOwn<'a, MakingTlsMode<T>>,
    ) -> Poll<AfterMakingTlsMode<T>, Error> {
        let tls_mode = try_ready!(state.future.poll().map_err(|e| Error::tls(e.into())));
        let state = state.take();

        transition!(Connecting {
            future: ConnectOnceFuture::new(state.host, state.port, tls_mode, state.params),
        })
    }

    fn poll_connecting<'a>(
        state: &'a mut RentToOwn<'a, Connecting<T>>,
    ) -> Poll<AfterConnecting<T>, Error> {
        let r = try_ready!(state.future.poll());
        transition!(Finished(r))
    }
}

impl<T> ConnectFuture<T>
where
    T: MakeTlsMode<Socket>,
{
    pub fn new(make_tls_mode: T, params: HashMap<String, String>) -> ConnectFuture<T> {
        Connect::start(make_tls_mode, params)
    }
}

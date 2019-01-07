use futures::{try_ready, Future, Poll};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use std::io;

use crate::proto::{CancelQueryRawFuture, ConnectSocketFuture};
use crate::{Config, Error, Host, MakeTlsMode, Socket};

#[derive(StateMachineFuture)]
pub enum CancelQuery<T>
where
    T: MakeTlsMode<Socket>,
{
    #[state_machine_future(start, transitions(ConnectingSocket))]
    Start {
        make_tls_mode: T,
        idx: Option<usize>,
        config: Config,
        process_id: i32,
        secret_key: i32,
    },
    #[state_machine_future(transitions(Canceling))]
    ConnectingSocket {
        future: ConnectSocketFuture,
        tls_mode: T::TlsMode,
        process_id: i32,
        secret_key: i32,
    },
    #[state_machine_future(transitions(Finished))]
    Canceling {
        future: CancelQueryRawFuture<Socket, T::TlsMode>,
    },
    #[state_machine_future(ready)]
    Finished(()),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<T> PollCancelQuery<T> for CancelQuery<T>
where
    T: MakeTlsMode<Socket>,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<T>>) -> Poll<AfterStart<T>, Error> {
        let mut state = state.take();

        let idx = state.idx.ok_or_else(|| {
            Error::connect(io::Error::new(io::ErrorKind::InvalidInput, "unknown host"))
        })?;

        let hostname = match &state.config.0.host[idx] {
            Host::Tcp(host) => &**host,
            // postgres doesn't support TLS over unix sockets, so the choice here doesn't matter
            #[cfg(unix)]
            Host::Unix(_) => "",
        };
        let tls_mode = state
            .make_tls_mode
            .make_tls_mode(hostname)
            .map_err(|e| Error::tls(e.into()))?;

        transition!(ConnectingSocket {
            future: ConnectSocketFuture::new(state.config, idx),
            tls_mode,
            process_id: state.process_id,
            secret_key: state.secret_key,
        })
    }

    fn poll_connecting_socket<'a>(
        state: &'a mut RentToOwn<'a, ConnectingSocket<T>>,
    ) -> Poll<AfterConnectingSocket<T>, Error> {
        let socket = try_ready!(state.future.poll());
        let state = state.take();

        transition!(Canceling {
            future: CancelQueryRawFuture::new(
                socket,
                state.tls_mode,
                state.process_id,
                state.secret_key
            ),
        })
    }

    fn poll_canceling<'a>(
        state: &'a mut RentToOwn<'a, Canceling<T>>,
    ) -> Poll<AfterCanceling, Error> {
        try_ready!(state.future.poll());
        transition!(Finished(()))
    }
}

impl<T> CancelQueryFuture<T>
where
    T: MakeTlsMode<Socket>,
{
    pub fn new(
        make_tls_mode: T,
        idx: Option<usize>,
        config: Config,
        process_id: i32,
        secret_key: i32,
    ) -> CancelQueryFuture<T> {
        CancelQuery::start(make_tls_mode, idx, config, process_id, secret_key)
    }
}

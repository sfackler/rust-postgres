use futures::{try_ready, Async, Future, Poll};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use std::collections::HashMap;
use std::vec;

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
        addrs: vec::IntoIter<(String, u16)>,
        make_tls_mode: T,
        params: HashMap<String, String>,
    },
    #[state_machine_future(transitions(MakingTlsMode, Finished))]
    Connecting {
        future: ConnectOnceFuture<T::TlsMode>,
        addrs: vec::IntoIter<(String, u16)>,
        make_tls_mode: T,
        params: HashMap<String, String>,
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
        let mut addrs = host
            .split(',')
            .map(|s| (s.to_string(), 0u16))
            .collect::<Vec<_>>();

        let port = state.params.remove("port").unwrap_or_else(String::new);
        let mut ports = port
            .split(',')
            .map(|s| {
                if s.is_empty() {
                    Ok(5432)
                } else {
                    s.parse::<u16>().map_err(Error::invalid_port)
                }
            })
            .collect::<Result<Vec<_>, _>>()?;
        if ports.len() == 1 {
            ports.resize(addrs.len(), ports[0]);
        }
        if addrs.len() != ports.len() {
            return Err(Error::invalid_port_count());
        }

        for (addr, port) in addrs.iter_mut().zip(ports) {
            addr.1 = port;
        }

        let mut addrs = addrs.into_iter();
        let (host, port) = addrs.next().expect("addrs cannot be empty");

        transition!(MakingTlsMode {
            future: state.make_tls_mode.make_tls_mode(&host),
            host,
            port,
            addrs,
            make_tls_mode: state.make_tls_mode,
            params: state.params,
        })
    }

    fn poll_making_tls_mode<'a>(
        state: &'a mut RentToOwn<'a, MakingTlsMode<T>>,
    ) -> Poll<AfterMakingTlsMode<T>, Error> {
        let tls_mode = try_ready!(state.future.poll().map_err(|e| Error::tls(e.into())));
        let state = state.take();

        transition!(Connecting {
            future: ConnectOnceFuture::new(state.host, state.port, tls_mode, state.params.clone()),
            addrs: state.addrs,
            make_tls_mode: state.make_tls_mode,
            params: state.params,
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
                let (host, port) = match state.addrs.next() {
                    Some(addr) => addr,
                    None => return Err(e),
                };

                transition!(MakingTlsMode {
                    future: state.make_tls_mode.make_tls_mode(&host),
                    host,
                    port,
                    addrs: state.addrs,
                    make_tls_mode: state.make_tls_mode,
                    params: state.params,
                })
            }
        }
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

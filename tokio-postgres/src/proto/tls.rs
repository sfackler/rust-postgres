use futures::{Future, Poll};
use postgres_protocol::message::frontend;
use state_machine_future::RentToOwn;
use tokio_io::io::{self, ReadExact, WriteAll};
use tokio_io::{AsyncRead, AsyncWrite};

use {ChannelBinding, Error, TlsMode};

#[derive(StateMachineFuture)]
pub enum Tls<S, T>
where
    T: TlsMode<S>,
    S: AsyncRead + AsyncWrite,
{
    #[state_machine_future(start, transitions(SendingTls, ConnectingTls))]
    Start { stream: S, tls_mode: T },
    #[state_machine_future(transitions(ReadingTls))]
    SendingTls {
        future: WriteAll<S, Vec<u8>>,
        tls_mode: T,
    },
    #[state_machine_future(transitions(ConnectingTls))]
    ReadingTls {
        future: ReadExact<S, [u8; 1]>,
        tls_mode: T,
    },
    #[state_machine_future(transitions(Ready))]
    ConnectingTls { future: T::Future },
    #[state_machine_future(ready)]
    Ready((T::Stream, ChannelBinding)),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<S, T> PollTls<S, T> for Tls<S, T>
where
    T: TlsMode<S>,
    S: AsyncRead + AsyncWrite,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<S, T>>) -> Poll<AfterStart<S, T>, Error> {
        let state = state.take();

        if state.tls_mode.request_tls() {
            let mut buf = vec![];
            frontend::ssl_request(&mut buf);

            transition!(SendingTls {
                future: io::write_all(state.stream, buf),
                tls_mode: state.tls_mode,
            })
        } else {
            transition!(ConnectingTls {
                future: state.tls_mode.handle_tls(false, state.stream),
            })
        }
    }

    fn poll_sending_tls<'a>(
        state: &'a mut RentToOwn<'a, SendingTls<S, T>>,
    ) -> Poll<AfterSendingTls<S, T>, Error> {
        let (stream, _) = try_ready!(state.future.poll().map_err(Error::io));
        let state = state.take();
        transition!(ReadingTls {
            future: io::read_exact(stream, [0]),
            tls_mode: state.tls_mode,
        })
    }

    fn poll_reading_tls<'a>(
        state: &'a mut RentToOwn<'a, ReadingTls<S, T>>,
    ) -> Poll<AfterReadingTls<S, T>, Error> {
        let (stream, buf) = try_ready!(state.future.poll().map_err(Error::io));
        let state = state.take();

        let use_tls = buf[0] == b'S';
        transition!(ConnectingTls {
            future: state.tls_mode.handle_tls(use_tls, stream)
        })
    }

    fn poll_connecting_tls<'a>(
        state: &'a mut RentToOwn<'a, ConnectingTls<S, T>>,
    ) -> Poll<AfterConnectingTls<S, T>, Error> {
        let t = try_ready!(state.future.poll().map_err(|e| Error::tls(e.into())));
        transition!(Ready(t))
    }
}

impl<S, T> TlsFuture<S, T>
where
    T: TlsMode<S>,
    S: AsyncRead + AsyncWrite,
{
    pub fn new(stream: S, tls_mode: T) -> TlsFuture<S, T> {
        Tls::start(stream, tls_mode)
    }
}

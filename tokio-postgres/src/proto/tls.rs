use futures::{try_ready, Future, Poll};
use postgres_protocol::message::frontend;
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use tokio_io::io::{self, ReadExact, WriteAll};
use tokio_io::{AsyncRead, AsyncWrite};

use crate::proto::MaybeTlsStream;
use crate::tls::private::ForcePrivateApi;
use crate::{ChannelBinding, Error, SslMode, TlsConnect};

#[derive(StateMachineFuture)]
pub enum Tls<S, T>
where
    T: TlsConnect<S>,
    S: AsyncRead + AsyncWrite,
{
    #[state_machine_future(start, transitions(SendingTls, Ready))]
    Start { stream: S, mode: SslMode, tls: T },
    #[state_machine_future(transitions(ReadingTls))]
    SendingTls {
        future: WriteAll<S, Vec<u8>>,
        mode: SslMode,
        tls: T,
    },
    #[state_machine_future(transitions(ConnectingTls, Ready))]
    ReadingTls {
        future: ReadExact<S, [u8; 1]>,
        mode: SslMode,
        tls: T,
    },
    #[state_machine_future(transitions(Ready))]
    ConnectingTls { future: T::Future },
    #[state_machine_future(ready)]
    Ready((MaybeTlsStream<S, T::Stream>, ChannelBinding)),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<S, T> PollTls<S, T> for Tls<S, T>
where
    T: TlsConnect<S>,
    S: AsyncRead + AsyncWrite,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<S, T>>) -> Poll<AfterStart<S, T>, Error> {
        let state = state.take();

        match state.mode {
            SslMode::Disable => transition!(Ready((
                MaybeTlsStream::Raw(state.stream),
                ChannelBinding::none()
            ))),
            SslMode::Prefer if !state.tls.can_connect(ForcePrivateApi) => transition!(Ready((
                MaybeTlsStream::Raw(state.stream),
                ChannelBinding::none()
            ))),
            SslMode::Prefer | SslMode::Require => {
                let mut buf = vec![];
                frontend::ssl_request(&mut buf);

                transition!(SendingTls {
                    future: io::write_all(state.stream, buf),
                    mode: state.mode,
                    tls: state.tls,
                })
            }
            SslMode::__NonExhaustive => unreachable!(),
        }
    }

    fn poll_sending_tls<'a>(
        state: &'a mut RentToOwn<'a, SendingTls<S, T>>,
    ) -> Poll<AfterSendingTls<S, T>, Error> {
        let (stream, _) = try_ready!(state.future.poll().map_err(Error::io));
        let state = state.take();
        transition!(ReadingTls {
            future: io::read_exact(stream, [0]),
            mode: state.mode,
            tls: state.tls,
        })
    }

    fn poll_reading_tls<'a>(
        state: &'a mut RentToOwn<'a, ReadingTls<S, T>>,
    ) -> Poll<AfterReadingTls<S, T>, Error> {
        let (stream, buf) = try_ready!(state.future.poll().map_err(Error::io));
        let state = state.take();

        if buf[0] == b'S' {
            transition!(ConnectingTls {
                future: state.tls.connect(stream),
            })
        } else if state.mode == SslMode::Require {
            Err(Error::tls("server does not support TLS".into()))
        } else {
            transition!(Ready((MaybeTlsStream::Raw(stream), ChannelBinding::none())))
        }
    }

    fn poll_connecting_tls<'a>(
        state: &'a mut RentToOwn<'a, ConnectingTls<S, T>>,
    ) -> Poll<AfterConnectingTls<S, T>, Error> {
        let (stream, channel_binding) =
            try_ready!(state.future.poll().map_err(|e| Error::tls(e.into())));
        transition!(Ready((MaybeTlsStream::Tls(stream), channel_binding)))
    }
}

impl<S, T> TlsFuture<S, T>
where
    T: TlsConnect<S>,
    S: AsyncRead + AsyncWrite,
{
    pub fn new(stream: S, mode: SslMode, tls: T) -> TlsFuture<S, T> {
        Tls::start(stream, mode, tls)
    }
}

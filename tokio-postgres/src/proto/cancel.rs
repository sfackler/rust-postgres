use futures::{Future, Poll};
use postgres_protocol::message::frontend;
use state_machine_future::RentToOwn;
use tokio_io::io::{self, Flush, WriteAll};
use tokio_io::{AsyncRead, AsyncWrite};

use error::Error;
use proto::TlsFuture;
use {CancelData, TlsMode};

#[derive(StateMachineFuture)]
pub enum Cancel<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>,
{
    #[state_machine_future(start, transitions(SendingCancel))]
    Start {
        future: TlsFuture<S, T>,
        cancel_data: CancelData,
    },
    #[state_machine_future(transitions(FlushingCancel))]
    SendingCancel {
        future: WriteAll<T::Stream, Vec<u8>>,
    },
    #[state_machine_future(transitions(Finished))]
    FlushingCancel { future: Flush<T::Stream> },
    #[state_machine_future(ready)]
    Finished(()),
    #[state_machine_future(error)]
    Failed(Error),
}

impl<S, T> PollCancel<S, T> for Cancel<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<S, T>>) -> Poll<AfterStart<S, T>, Error> {
        let (stream, _) = try_ready!(state.future.poll());

        let mut buf = vec![];
        frontend::cancel_request(
            state.cancel_data.process_id,
            state.cancel_data.secret_key,
            &mut buf,
        );

        transition!(SendingCancel {
            future: io::write_all(stream, buf),
        })
    }

    fn poll_sending_cancel<'a>(
        state: &'a mut RentToOwn<'a, SendingCancel<S, T>>,
    ) -> Poll<AfterSendingCancel<S, T>, Error> {
        let (stream, _) = try_ready_closed!(state.future.poll());

        transition!(FlushingCancel {
            future: io::flush(stream),
        })
    }

    fn poll_flushing_cancel<'a>(
        state: &'a mut RentToOwn<'a, FlushingCancel<S, T>>,
    ) -> Poll<AfterFlushingCancel, Error> {
        try_ready_closed!(state.future.poll());
        transition!(Finished(()))
    }
}

impl<S, T> CancelFuture<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>,
{
    pub fn new(stream: S, tls_mode: T, cancel_data: CancelData) -> CancelFuture<S, T> {
        Cancel::start(TlsFuture::new(stream, tls_mode), cancel_data)
    }
}

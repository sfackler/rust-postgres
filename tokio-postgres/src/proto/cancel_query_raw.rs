use futures::{try_ready, Future, Poll};
use postgres_protocol::message::frontend;
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use tokio_io::io::{self, Flush, WriteAll};
use tokio_io::{AsyncRead, AsyncWrite};

use crate::error::Error;
use crate::proto::TlsFuture;
use crate::TlsMode;

#[derive(StateMachineFuture)]
pub enum CancelQueryRaw<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>,
{
    #[state_machine_future(start, transitions(SendingCancel))]
    Start {
        future: TlsFuture<S, T>,
        process_id: i32,
        secret_key: i32,
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

impl<S, T> PollCancelQueryRaw<S, T> for CancelQueryRaw<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>,
{
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start<S, T>>) -> Poll<AfterStart<S, T>, Error> {
        let (stream, _) = try_ready!(state.future.poll());

        let mut buf = vec![];
        frontend::cancel_request(state.process_id, state.secret_key, &mut buf);

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

impl<S, T> CancelQueryRawFuture<S, T>
where
    S: AsyncRead + AsyncWrite,
    T: TlsMode<S>,
{
    pub fn new(
        stream: S,
        tls_mode: T,
        process_id: i32,
        secret_key: i32,
    ) -> CancelQueryRawFuture<S, T> {
        CancelQueryRaw::start(TlsFuture::new(stream, tls_mode), process_id, secret_key)
    }
}

use futures::{Future, Poll};
use postgres_protocol::message::frontend;
use state_machine_future::RentToOwn;
use tokio::io::{self, Flush, WriteAll};

use error::Error;
use params::ConnectParams;
use proto::connect::ConnectFuture;
use tls::TlsStream;
use {CancelData, TlsMode};

#[derive(StateMachineFuture)]
pub enum Cancel {
    #[state_machine_future(start, transitions(SendingCancel))]
    Start {
        future: ConnectFuture,
        cancel_data: CancelData,
    },
    #[state_machine_future(transitions(FlushingCancel))]
    SendingCancel {
        future: WriteAll<Box<TlsStream>, Vec<u8>>,
    },
    #[state_machine_future(transitions(Finished))]
    FlushingCancel { future: Flush<Box<TlsStream>> },
    #[state_machine_future(ready)]
    Finished(()),
    #[state_machine_future(error)]
    Failed(Error),
}

impl PollCancel for Cancel {
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start>) -> Poll<AfterStart, Error> {
        let stream = try_ready!(state.future.poll());

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
        state: &'a mut RentToOwn<'a, SendingCancel>,
    ) -> Poll<AfterSendingCancel, Error> {
        let (stream, _) = try_ready_closed!(state.future.poll());

        transition!(FlushingCancel {
            future: io::flush(stream),
        })
    }

    fn poll_flushing_cancel<'a>(
        state: &'a mut RentToOwn<'a, FlushingCancel>,
    ) -> Poll<AfterFlushingCancel, Error> {
        try_ready_closed!(state.future.poll());
        transition!(Finished(()))
    }
}

impl CancelFuture {
    pub fn new(params: ConnectParams, mode: TlsMode, cancel_data: CancelData) -> CancelFuture {
        Cancel::start(ConnectFuture::new(params, mode), cancel_data)
    }
}

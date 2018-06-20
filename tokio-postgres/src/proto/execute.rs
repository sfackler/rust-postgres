use futures::sync::mpsc;
use futures::{Poll, Stream};
use postgres_protocol::message::backend::Message;
use state_machine_future::RentToOwn;

use error::{self, Error};
use proto::client::PendingRequest;
use proto::statement::Statement;
use {bad_response, disconnected};

#[derive(StateMachineFuture)]
pub enum Execute {
    #[state_machine_future(start, transitions(ReadResponse))]
    Start {
        request: PendingRequest,
        statement: Statement,
    },
    #[state_machine_future(transitions(ReadReadyForQuery))]
    ReadResponse { receiver: mpsc::Receiver<Message> },
    #[state_machine_future(transitions(Finished))]
    ReadReadyForQuery {
        receiver: mpsc::Receiver<Message>,
        rows: u64,
    },
    #[state_machine_future(ready)]
    Finished(u64),
    #[state_machine_future(error)]
    Failed(Error),
}

impl PollExecute for Execute {
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start>) -> Poll<AfterStart, Error> {
        let state = state.take();
        let receiver = state.request.send()?;

        // the statement can drop after this point, since its close will queue up after the execution
        transition!(ReadResponse { receiver })
    }

    fn poll_read_response<'a>(
        state: &'a mut RentToOwn<'a, ReadResponse>,
    ) -> Poll<AfterReadResponse, Error> {
        loop {
            let message = try_receive!(state.receiver.poll());

            match message {
                Some(Message::BindComplete) => {}
                Some(Message::DataRow(_)) => {}
                Some(Message::ErrorResponse(body)) => return Err(error::__db(body)),
                Some(Message::CommandComplete(body)) => {
                    let rows = body.tag()?.rsplit(' ').next().unwrap().parse().unwrap_or(0);
                    let state = state.take();
                    transition!(ReadReadyForQuery {
                        receiver: state.receiver,
                        rows,
                    });
                }
                Some(Message::EmptyQueryResponse) => {
                    let state = state.take();
                    transition!(ReadReadyForQuery {
                        receiver: state.receiver,
                        rows: 0,
                    });
                }
                Some(_) => return Err(bad_response()),
                None => return Err(disconnected()),
            }
        }
    }

    fn poll_read_ready_for_query<'a>(
        state: &'a mut RentToOwn<'a, ReadReadyForQuery>,
    ) -> Poll<AfterReadReadyForQuery, Error> {
        let message = try_receive!(state.receiver.poll());

        match message {
            Some(Message::ReadyForQuery(_)) => transition!(Finished(state.rows)),
            Some(_) => Err(bad_response()),
            None => Err(disconnected()),
        }
    }
}

impl ExecuteFuture {
    pub fn new(request: PendingRequest, statement: Statement) -> ExecuteFuture {
        Execute::start(request, statement)
    }
}

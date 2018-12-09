use futures::sync::mpsc;
use futures::{Poll, Stream};
use postgres_protocol::message::backend::Message;
use state_machine_future::{transition, RentToOwn, StateMachineFuture};

use crate::proto::client::{Client, PendingRequest};
use crate::Error;

#[derive(StateMachineFuture)]
pub enum SimpleQuery {
    #[state_machine_future(start, transitions(ReadResponse))]
    Start {
        client: Client,
        request: PendingRequest,
    },
    #[state_machine_future(transitions(Finished))]
    ReadResponse { receiver: mpsc::Receiver<Message> },
    #[state_machine_future(ready)]
    Finished(()),
    #[state_machine_future(error)]
    Failed(Error),
}

impl PollSimpleQuery for SimpleQuery {
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start>) -> Poll<AfterStart, Error> {
        let state = state.take();
        let receiver = state.client.send(state.request)?;

        transition!(ReadResponse { receiver })
    }

    fn poll_read_response<'a>(
        state: &'a mut RentToOwn<'a, ReadResponse>,
    ) -> Poll<AfterReadResponse, Error> {
        loop {
            let message = try_ready_receive!(state.receiver.poll());

            match message {
                Some(Message::CommandComplete(_))
                | Some(Message::RowDescription(_))
                | Some(Message::DataRow(_))
                | Some(Message::EmptyQueryResponse) => {}
                Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
                Some(Message::ReadyForQuery(_)) => transition!(Finished(())),
                Some(_) => return Err(Error::unexpected_message()),
                None => return Err(Error::closed()),
            }
        }
    }
}

impl SimpleQueryFuture {
    pub fn new(client: Client, request: PendingRequest) -> SimpleQueryFuture {
        SimpleQuery::start(client, request)
    }
}

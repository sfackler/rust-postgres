use futures::sync::mpsc;
use futures::{Poll, Stream};
use postgres_protocol::message::backend::Message;
use state_machine_future::RentToOwn;

use error::{self, Error};
use proto::client::{Client, PendingRequest};
use {bad_response, disconnected};

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
            let message = try_receive!(state.receiver.poll());

            match message {
                Some(Message::CommandComplete(_))
                | Some(Message::RowDescription(_))
                | Some(Message::DataRow(_))
                | Some(Message::EmptyQueryResponse) => {}
                Some(Message::ErrorResponse(body)) => return Err(error::__db(body)),
                Some(Message::ReadyForQuery(_)) => transition!(Finished(())),
                Some(_) => return Err(bad_response()),
                None => return Err(disconnected()),
            }
        }
    }
}

impl SimpleQueryFuture {
    pub fn new(client: Client, request: PendingRequest) -> SimpleQueryFuture {
        SimpleQuery::start(client, request)
    }
}

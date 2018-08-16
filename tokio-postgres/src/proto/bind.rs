use futures::sync::mpsc;
use futures::{Poll, Stream};
use postgres_protocol::message::backend::Message;
use proto::client::{Client, PendingRequest};
use proto::portal::Portal;
use proto::statement::Statement;
use state_machine_future::RentToOwn;
use Error;

#[derive(StateMachineFuture)]
pub enum Bind {
    #[state_machine_future(start, transitions(ReadBindComplete))]
    Start {
        client: Client,
        request: PendingRequest,
        name: String,
        statement: Statement,
    },
    #[state_machine_future(transitions(Finished))]
    ReadBindComplete {
        receiver: mpsc::Receiver<Message>,
        client: Client,
        name: String,
        statement: Statement,
    },
    #[state_machine_future(ready)]
    Finished(Portal),
    #[state_machine_future(error)]
    Failed(Error),
}

impl PollBind for Bind {
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start>) -> Poll<AfterStart, Error> {
        let state = state.take();
        let receiver = state.client.send(state.request)?;

        transition!(ReadBindComplete {
            receiver,
            client: state.client,
            name: state.name,
            statement: state.statement,
        })
    }

    fn poll_read_bind_complete<'a>(
        state: &'a mut RentToOwn<'a, ReadBindComplete>,
    ) -> Poll<AfterReadBindComplete, Error> {
        let message = try_ready_receive!(state.receiver.poll());
        let state = state.take();

        match message {
            Some(Message::BindComplete) => transition!(Finished(Portal::new(
                state.client.downgrade(),
                state.name,
                state.statement,
            ))),
            Some(_) => Err(Error::unexpected_message()),
            None => Err(Error::closed()),
        }
    }
}

impl BindFuture {
    pub fn new(
        client: Client,
        request: PendingRequest,
        name: String,
        statement: Statement,
    ) -> BindFuture {
        Bind::start(client, request, name, statement)
    }
}

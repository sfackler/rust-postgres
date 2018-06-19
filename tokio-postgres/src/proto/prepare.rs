use fallible_iterator::FallibleIterator;
use futures::sync::mpsc;
use futures::{Poll, Stream};
use postgres_protocol::message::backend::{Message, ParameterDescriptionBody, RowDescriptionBody};
use state_machine_future::RentToOwn;

use error::{self, Error};
use proto::client::PendingRequest;
use proto::connection::Request;
use proto::statement::Statement;
use types::Type;
use Column;
use {bad_response, disconnected};

#[derive(StateMachineFuture)]
pub enum Prepare {
    #[state_machine_future(start, transitions(ReadParseComplete))]
    Start {
        request: Result<PendingRequest, Error>,
        sender: mpsc::UnboundedSender<Request>,
        name: String,
    },
    #[state_machine_future(transitions(ReadParameterDescription))]
    ReadParseComplete {
        sender: mpsc::UnboundedSender<Request>,
        receiver: mpsc::Receiver<Message>,
        name: String,
    },
    #[state_machine_future(transitions(ReadRowDescription))]
    ReadParameterDescription {
        sender: mpsc::UnboundedSender<Request>,
        receiver: mpsc::Receiver<Message>,
        name: String,
    },
    #[state_machine_future(transitions(ReadReadyForQuery))]
    ReadRowDescription {
        sender: mpsc::UnboundedSender<Request>,
        receiver: mpsc::Receiver<Message>,
        name: String,
        parameters: ParameterDescriptionBody,
    },
    #[state_machine_future(transitions(Finished))]
    ReadReadyForQuery {
        sender: mpsc::UnboundedSender<Request>,
        receiver: mpsc::Receiver<Message>,
        name: String,
        parameters: ParameterDescriptionBody,
        columns: RowDescriptionBody,
    },
    #[state_machine_future(ready)]
    Finished(Statement),
    #[state_machine_future(error)]
    Failed(Error),
}

impl PollPrepare for Prepare {
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start>) -> Poll<AfterStart, Error> {
        let state = state.take();
        let receiver = state.request?.send()?;

        transition!(ReadParseComplete {
            sender: state.sender,
            receiver,
            name: state.name,
        })
    }

    fn poll_read_parse_complete<'a>(
        state: &'a mut RentToOwn<'a, ReadParseComplete>,
    ) -> Poll<AfterReadParseComplete, Error> {
        let message = try_receive!(state.receiver.poll());
        let state = state.take();

        match message {
            Some(Message::ParseComplete) => transition!(ReadParameterDescription {
                sender: state.sender,
                receiver: state.receiver,
                name: state.name,
            }),
            Some(Message::ErrorResponse(body)) => Err(error::__db(body)),
            Some(_) => Err(bad_response()),
            None => Err(disconnected()),
        }
    }

    fn poll_read_parameter_description<'a>(
        state: &'a mut RentToOwn<'a, ReadParameterDescription>,
    ) -> Poll<AfterReadParameterDescription, Error> {
        let message = try_receive!(state.receiver.poll());
        let state = state.take();

        match message {
            Some(Message::ParameterDescription(body)) => transition!(ReadRowDescription {
                sender: state.sender,
                receiver: state.receiver,
                name: state.name,
                parameters: body,
            }),
            Some(Message::ErrorResponse(body)) => Err(error::__db(body)),
            Some(_) => Err(bad_response()),
            None => Err(disconnected()),
        }
    }

    fn poll_read_row_description<'a>(
        state: &'a mut RentToOwn<'a, ReadRowDescription>,
    ) -> Poll<AfterReadRowDescription, Error> {
        let message = try_receive!(state.receiver.poll());
        let state = state.take();

        match message {
            Some(Message::RowDescription(body)) => transition!(ReadReadyForQuery {
                sender: state.sender,
                receiver: state.receiver,
                name: state.name,
                parameters: state.parameters,
                columns: body,
            }),
            Some(Message::ErrorResponse(body)) => Err(error::__db(body)),
            Some(_) => Err(bad_response()),
            None => Err(disconnected()),
        }
    }

    fn poll_read_ready_for_query<'a>(
        state: &'a mut RentToOwn<'a, ReadReadyForQuery>,
    ) -> Poll<AfterReadReadyForQuery, Error> {
        let message = try_receive!(state.receiver.poll());
        let state = state.take();

        match message {
            Some(Message::ReadyForQuery(_)) => {
                // FIXME handle custom types
                let parameters = state
                    .parameters
                    .parameters()
                    .map(|oid| Type::from_oid(oid).unwrap())
                    .collect()?;
                let columns = state
                    .columns
                    .fields()
                    .map(|f| {
                        Column::new(f.name().to_string(), Type::from_oid(f.type_oid()).unwrap())
                    })
                    .collect()?;

                transition!(Finished(Statement::new(
                    state.sender,
                    state.name,
                    parameters,
                    columns
                )))
            }
            Some(Message::ErrorResponse(body)) => Err(error::__db(body)),
            Some(_) => Err(bad_response()),
            None => Err(disconnected()),
        }
    }
}

impl PrepareFuture {
    pub fn new(
        request: Result<PendingRequest, Error>,
        sender: mpsc::UnboundedSender<Request>,
        name: String,
    ) -> PrepareFuture {
        Prepare::start(request, sender, name)
    }
}

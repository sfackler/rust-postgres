use fallible_iterator::FallibleIterator;
use futures::sync::mpsc;
use futures::{Future, Poll, Stream};
use postgres_protocol::message::backend::Message;
use state_machine_future::RentToOwn;
use std::mem;
use std::vec;

use error::{self, Error};
use proto::client::{Client, PendingRequest};
use proto::connection::Request;
use proto::statement::Statement;
use proto::typeinfo::TypeinfoFuture;
use types::{Oid, Type};
use Column;
use {bad_response, disconnected};

#[derive(StateMachineFuture)]
pub enum Prepare {
    #[state_machine_future(start, transitions(ReadParseComplete))]
    Start {
        request: PendingRequest,
        sender: mpsc::UnboundedSender<Request>,
        name: String,
        client: Client,
    },
    #[state_machine_future(transitions(ReadParameterDescription))]
    ReadParseComplete {
        sender: mpsc::UnboundedSender<Request>,
        receiver: mpsc::Receiver<Message>,
        name: String,
        client: Client,
    },
    #[state_machine_future(transitions(ReadRowDescription))]
    ReadParameterDescription {
        sender: mpsc::UnboundedSender<Request>,
        receiver: mpsc::Receiver<Message>,
        name: String,
        client: Client,
    },
    #[state_machine_future(transitions(ReadReadyForQuery))]
    ReadRowDescription {
        sender: mpsc::UnboundedSender<Request>,
        receiver: mpsc::Receiver<Message>,
        name: String,
        parameters: Vec<Oid>,
        client: Client,
    },
    #[state_machine_future(transitions(GetParameterTypes, GetColumnTypes, Finished))]
    ReadReadyForQuery {
        sender: mpsc::UnboundedSender<Request>,
        receiver: mpsc::Receiver<Message>,
        name: String,
        parameters: Vec<Oid>,
        columns: Vec<(String, Oid)>,
        client: Client,
    },
    #[state_machine_future(transitions(GetColumnTypes, Finished))]
    GetParameterTypes {
        future: TypeinfoFuture,
        remaining_parameters: vec::IntoIter<Oid>,
        sender: mpsc::UnboundedSender<Request>,
        name: String,
        parameters: Vec<Type>,
        columns: Vec<(String, Oid)>,
    },
    #[state_machine_future(transitions(Finished))]
    GetColumnTypes {
        future: TypeinfoFuture,
        cur_column_name: String,
        remaining_columns: vec::IntoIter<(String, Oid)>,
        sender: mpsc::UnboundedSender<Request>,
        name: String,
        parameters: Vec<Type>,
        columns: Vec<Column>,
    },
    #[state_machine_future(ready)]
    Finished(Statement),
    #[state_machine_future(error)]
    Failed(Error),
}

impl PollPrepare for Prepare {
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start>) -> Poll<AfterStart, Error> {
        let state = state.take();
        let receiver = state.request.send()?;

        transition!(ReadParseComplete {
            sender: state.sender,
            receiver,
            name: state.name,
            client: state.client,
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
                client: state.client,
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
                parameters: body.parameters().collect()?,
                client: state.client,
            }),
            Some(_) => Err(bad_response()),
            None => Err(disconnected()),
        }
    }

    fn poll_read_row_description<'a>(
        state: &'a mut RentToOwn<'a, ReadRowDescription>,
    ) -> Poll<AfterReadRowDescription, Error> {
        let message = try_receive!(state.receiver.poll());
        let state = state.take();

        let columns = match message {
            Some(Message::RowDescription(body)) => body
                .fields()
                .map(|f| (f.name().to_string(), f.type_oid()))
                .collect()?,
            Some(Message::NoData) => vec![],
            Some(_) => return Err(bad_response()),
            None => return Err(disconnected()),
        };

        transition!(ReadReadyForQuery {
            sender: state.sender,
            receiver: state.receiver,
            name: state.name,
            parameters: state.parameters,
            columns,
            client: state.client,
        })
    }

    fn poll_read_ready_for_query<'a>(
        state: &'a mut RentToOwn<'a, ReadReadyForQuery>,
    ) -> Poll<AfterReadReadyForQuery, Error> {
        let message = try_receive!(state.receiver.poll());
        let state = state.take();

        match message {
            Some(Message::ReadyForQuery(_)) => {}
            Some(_) => return Err(bad_response()),
            None => return Err(disconnected()),
        }

        let mut parameters = state.parameters.into_iter();
        if let Some(oid) = parameters.next() {
            transition!(GetParameterTypes {
                future: TypeinfoFuture::new(oid, state.client),
                remaining_parameters: parameters,
                sender: state.sender,
                name: state.name,
                parameters: vec![],
                columns: state.columns,
            });
        }

        let mut columns = state.columns.into_iter();
        if let Some((name, oid)) = columns.next() {
            transition!(GetColumnTypes {
                future: TypeinfoFuture::new(oid, state.client),
                cur_column_name: name,
                remaining_columns: columns,
                sender: state.sender,
                name: state.name,
                parameters: vec![],
                columns: vec![],
            });
        }

        transition!(Finished(Statement::new(
            state.sender,
            state.name,
            vec![],
            vec![]
        )))
    }

    fn poll_get_parameter_types<'a>(
        state: &'a mut RentToOwn<'a, GetParameterTypes>,
    ) -> Poll<AfterGetParameterTypes, Error> {
        let client = loop {
            let (ty, client) = try_ready!(state.future.poll());
            state.parameters.push(ty);

            match state.remaining_parameters.next() {
                Some(oid) => state.future = TypeinfoFuture::new(oid, client),
                None => break client,
            }
        };
        let state = state.take();

        let mut columns = state.columns.into_iter();
        if let Some((name, oid)) = columns.next() {
            transition!(GetColumnTypes {
                future: TypeinfoFuture::new(oid, client),
                cur_column_name: name,
                remaining_columns: columns,
                sender: state.sender,
                name: state.name,
                parameters: state.parameters,
                columns: vec![],
            })
        }

        transition!(Finished(Statement::new(
            state.sender,
            state.name,
            state.parameters,
            vec![],
        )))
    }

    fn poll_get_column_types<'a>(
        state: &'a mut RentToOwn<'a, GetColumnTypes>,
    ) -> Poll<AfterGetColumnTypes, Error> {
        loop {
            let (ty, client) = try_ready!(state.future.poll());
            let name = mem::replace(&mut state.cur_column_name, String::new());
            state.columns.push(Column::new(name, ty));

            match state.remaining_columns.next() {
                Some((name, oid)) => {
                    state.cur_column_name = name;
                    state.future = TypeinfoFuture::new(oid, client);
                }
                None => break,
            }
        }
        let state = state.take();

        transition!(Finished(Statement::new(
            state.sender,
            state.name,
            state.parameters,
            state.columns,
        )))
    }
}

impl PrepareFuture {
    pub fn new(
        request: PendingRequest,
        sender: mpsc::UnboundedSender<Request>,
        name: String,
        client: Client,
    ) -> PrepareFuture {
        Prepare::start(request, sender, name, client)
    }
}

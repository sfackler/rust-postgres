use fallible_iterator::FallibleIterator;
use futures::sync::mpsc;
use futures::{try_ready, Future, Poll, Stream};
use postgres_protocol::message::backend::Message;
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use std::mem;
use std::vec;

use crate::proto::client::{Client, PendingRequest};
use crate::proto::statement::Statement;
use crate::proto::typeinfo::TypeinfoFuture;
use crate::types::{Oid, Type};
use crate::{Column, Error};

#[derive(StateMachineFuture)]
pub enum Prepare {
    #[state_machine_future(start, transitions(ReadParseComplete))]
    Start {
        client: Client,
        request: PendingRequest,
        name: String,
    },
    #[state_machine_future(transitions(ReadParameterDescription))]
    ReadParseComplete {
        client: Client,
        receiver: mpsc::Receiver<Message>,
        name: String,
    },
    #[state_machine_future(transitions(ReadRowDescription))]
    ReadParameterDescription {
        client: Client,
        receiver: mpsc::Receiver<Message>,
        name: String,
    },
    #[state_machine_future(transitions(GetParameterTypes, GetColumnTypes, Finished))]
    ReadRowDescription {
        client: Client,
        receiver: mpsc::Receiver<Message>,
        name: String,
        parameters: Vec<Oid>,
    },
    #[state_machine_future(transitions(GetColumnTypes, Finished))]
    GetParameterTypes {
        future: TypeinfoFuture,
        remaining_parameters: vec::IntoIter<Oid>,
        name: String,
        parameters: Vec<Type>,
        columns: Vec<(String, Oid)>,
    },
    #[state_machine_future(transitions(Finished))]
    GetColumnTypes {
        future: TypeinfoFuture,
        cur_column_name: String,
        remaining_columns: vec::IntoIter<(String, Oid)>,
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
        let receiver = state.client.send(state.request)?;

        transition!(ReadParseComplete {
            receiver,
            name: state.name,
            client: state.client,
        })
    }

    fn poll_read_parse_complete<'a>(
        state: &'a mut RentToOwn<'a, ReadParseComplete>,
    ) -> Poll<AfterReadParseComplete, Error> {
        let message = try_ready_receive!(state.receiver.poll());
        let state = state.take();

        match message {
            Some(Message::ParseComplete) => transition!(ReadParameterDescription {
                receiver: state.receiver,
                name: state.name,
                client: state.client,
            }),
            Some(Message::ErrorResponse(body)) => Err(Error::db(body)),
            Some(_) => Err(Error::unexpected_message()),
            None => Err(Error::closed()),
        }
    }

    fn poll_read_parameter_description<'a>(
        state: &'a mut RentToOwn<'a, ReadParameterDescription>,
    ) -> Poll<AfterReadParameterDescription, Error> {
        let message = try_ready_receive!(state.receiver.poll());
        let state = state.take();

        match message {
            Some(Message::ParameterDescription(body)) => transition!(ReadRowDescription {
                receiver: state.receiver,
                name: state.name,
                parameters: body.parameters().collect().map_err(Error::parse)?,
                client: state.client,
            }),
            Some(_) => Err(Error::unexpected_message()),
            None => Err(Error::closed()),
        }
    }

    fn poll_read_row_description<'a>(
        state: &'a mut RentToOwn<'a, ReadRowDescription>,
    ) -> Poll<AfterReadRowDescription, Error> {
        let message = try_ready_receive!(state.receiver.poll());
        let state = state.take();

        let columns = match message {
            Some(Message::RowDescription(body)) => body
                .fields()
                .map(|f| (f.name().to_string(), f.type_oid()))
                .collect()
                .map_err(Error::parse)?,
            Some(Message::NoData) => vec![],
            Some(_) => return Err(Error::unexpected_message()),
            None => return Err(Error::closed()),
        };

        let mut parameters = state.parameters.into_iter();
        if let Some(oid) = parameters.next() {
            transition!(GetParameterTypes {
                future: TypeinfoFuture::new(oid, state.client),
                remaining_parameters: parameters,
                name: state.name,
                parameters: vec![],
                columns: columns,
            });
        }

        let mut columns = columns.into_iter();
        if let Some((name, oid)) = columns.next() {
            transition!(GetColumnTypes {
                future: TypeinfoFuture::new(oid, state.client),
                cur_column_name: name,
                remaining_columns: columns,
                name: state.name,
                parameters: vec![],
                columns: vec![],
            });
        }

        transition!(Finished(Statement::new(
            state.client.downgrade(),
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
                name: state.name,
                parameters: state.parameters,
                columns: vec![],
            })
        }

        transition!(Finished(Statement::new(
            client.downgrade(),
            state.name,
            state.parameters,
            vec![],
        )))
    }

    fn poll_get_column_types<'a>(
        state: &'a mut RentToOwn<'a, GetColumnTypes>,
    ) -> Poll<AfterGetColumnTypes, Error> {
        let client = loop {
            let (ty, client) = try_ready!(state.future.poll());
            let name = mem::replace(&mut state.cur_column_name, String::new());
            state.columns.push(Column::new(name, ty));

            match state.remaining_columns.next() {
                Some((name, oid)) => {
                    state.cur_column_name = name;
                    state.future = TypeinfoFuture::new(oid, client);
                }
                None => break client,
            }
        };
        let state = state.take();

        transition!(Finished(Statement::new(
            client.downgrade(),
            state.name,
            state.parameters,
            state.columns,
        )))
    }
}

impl PrepareFuture {
    pub fn new(client: Client, request: PendingRequest, name: String) -> PrepareFuture {
        Prepare::start(client, request, name)
    }
}

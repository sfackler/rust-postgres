use futures::stream::{self, Stream};
use futures::{try_ready, Future, Poll};
use state_machine_future::{transition, RentToOwn, StateMachineFuture};
use std::mem;
use std::vec;

use crate::error::Error;
use crate::next_statement;
use crate::proto::client::Client;
use crate::proto::prepare::PrepareFuture;
use crate::proto::query::QueryStream;
use crate::proto::statement::Statement;
use crate::proto::typeinfo::TypeinfoFuture;
use crate::types::{Field, Oid};

const TYPEINFO_COMPOSITE_QUERY: &'static str = "
SELECT attname, atttypid
FROM pg_catalog.pg_attribute
WHERE attrelid = $1
AND NOT attisdropped
AND attnum > 0
ORDER BY attnum
";

#[derive(StateMachineFuture)]
pub enum TypeinfoComposite {
    #[state_machine_future(
        start,
        transitions(PreparingTypeinfoComposite, QueryingCompositeFields)
    )]
    Start { oid: Oid, client: Client },
    #[state_machine_future(transitions(QueryingCompositeFields))]
    PreparingTypeinfoComposite {
        future: Box<PrepareFuture>,
        oid: Oid,
        client: Client,
    },
    #[state_machine_future(transitions(QueryingCompositeFieldTypes, Finished))]
    QueryingCompositeFields {
        future: stream::Collect<QueryStream<Statement>>,
        client: Client,
    },
    #[state_machine_future(transitions(Finished))]
    QueryingCompositeFieldTypes {
        future: Box<TypeinfoFuture>,
        cur_field_name: String,
        remaining_fields: vec::IntoIter<(String, Oid)>,
        fields: Vec<Field>,
    },
    #[state_machine_future(ready)]
    Finished((Vec<Field>, Client)),
    #[state_machine_future(error)]
    Failed(Error),
}

impl PollTypeinfoComposite for TypeinfoComposite {
    fn poll_start<'a>(state: &'a mut RentToOwn<'a, Start>) -> Poll<AfterStart, Error> {
        let state = state.take();

        match state.client.typeinfo_composite_query() {
            Some(statement) => transition!(QueryingCompositeFields {
                future: state.client.query(&statement, &[&state.oid]).collect(),
                client: state.client,
            }),
            None => transition!(PreparingTypeinfoComposite {
                future: Box::new(state.client.prepare(
                    next_statement(),
                    TYPEINFO_COMPOSITE_QUERY,
                    &[]
                )),
                oid: state.oid,
                client: state.client,
            }),
        }
    }

    fn poll_preparing_typeinfo_composite<'a>(
        state: &'a mut RentToOwn<'a, PreparingTypeinfoComposite>,
    ) -> Poll<AfterPreparingTypeinfoComposite, Error> {
        let statement = try_ready!(state.future.poll());
        let state = state.take();

        state.client.set_typeinfo_composite_query(&statement);
        transition!(QueryingCompositeFields {
            future: state.client.query(&statement, &[&state.oid]).collect(),
            client: state.client,
        })
    }

    fn poll_querying_composite_fields<'a>(
        state: &'a mut RentToOwn<'a, QueryingCompositeFields>,
    ) -> Poll<AfterQueryingCompositeFields, Error> {
        let rows = try_ready!(state.future.poll());
        let state = state.take();

        let fields = rows
            .iter()
            .map(|row| {
                let name = row.try_get(0)?.ok_or_else(Error::unexpected_message)?;
                let oid = row.try_get(1)?.ok_or_else(Error::unexpected_message)?;
                Ok((name, oid))
            })
            .collect::<Result<Vec<(String, Oid)>, Error>>()?;

        let mut remaining_fields = fields.into_iter();
        match remaining_fields.next() {
            Some((cur_field_name, oid)) => transition!(QueryingCompositeFieldTypes {
                future: Box::new(TypeinfoFuture::new(oid, state.client)),
                cur_field_name,
                fields: vec![],
                remaining_fields,
            }),
            None => transition!(Finished((vec![], state.client))),
        }
    }

    fn poll_querying_composite_field_types<'a>(
        state: &'a mut RentToOwn<'a, QueryingCompositeFieldTypes>,
    ) -> Poll<AfterQueryingCompositeFieldTypes, Error> {
        loop {
            let (ty, client) = try_ready!(state.future.poll());

            let name = mem::replace(&mut state.cur_field_name, String::new());
            state.fields.push(Field::new(name, ty));

            match state.remaining_fields.next() {
                Some((cur_field_name, oid)) => {
                    state.cur_field_name = cur_field_name;
                    state.future = Box::new(TypeinfoFuture::new(oid, client));
                }
                None => {
                    let state = state.take();
                    transition!(Finished((state.fields, client)));
                }
            }
        }
    }
}

impl TypeinfoCompositeFuture {
    pub fn new(oid: Oid, client: Client) -> TypeinfoCompositeFuture {
        TypeinfoComposite::start(oid, client)
    }
}

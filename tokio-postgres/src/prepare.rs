use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::{Oid, Type};
use crate::{Column, Error, Statement};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

pub async fn prepare(
    client: Arc<InnerClient>,
    query: &str,
    types: &[Type],
) -> Result<Statement, Error> {
    let name = format!("s{}", NEXT_ID.fetch_add(1, Ordering::SeqCst));

    let mut buf = vec![];
    frontend::parse(&name, query, types.iter().map(Type::oid), &mut buf).map_err(Error::encode)?;
    frontend::describe(b'S', &name, &mut buf).map_err(Error::encode)?;
    frontend::sync(&mut buf);

    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next().await? {
        Message::ParseComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    let parameter_description = match responses.next().await? {
        Message::ParameterDescription(body) => body,
        _ => return Err(Error::unexpected_message()),
    };

    let row_description = match responses.next().await? {
        Message::RowDescription(body) => Some(body),
        Message::NoData => None,
        _ => return Err(Error::unexpected_message()),
    };

    let mut parameters = vec![];
    let mut it = parameter_description.parameters();
    while let Some(oid) = it.next().map_err(Error::parse)? {
        let type_ = get_type(&client, oid).await?;
        parameters.push(type_);
    }

    let mut columns = vec![];
    if let Some(row_description) = row_description {
        let mut it = row_description.fields();
        while let Some(field) = it.next().map_err(Error::parse)? {
            let type_ = get_type(&client, field.type_oid()).await?;
            let column = Column::new(field.name().to_string(), type_);
            columns.push(column);
        }
    }

    Ok(Statement::new(&client, name, parameters, columns))
}

async fn get_type(client: &Arc<InnerClient>, oid: Oid) -> Result<Type, Error> {
    if let Some(type_) = Type::from_oid(oid) {
        return Ok(type_);
    }

    if let Some(type_) = client.type_(oid) {
        return Ok(type_);
    }

    unimplemented!()
}

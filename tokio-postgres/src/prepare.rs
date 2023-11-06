use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::{Oid, Type};
use crate::{Column, Error, Statement};
use bytes::Bytes;
use fallible_iterator::FallibleIterator;
use log::debug;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

pub async fn prepare(
    client: &Arc<InnerClient>,
    query: &str,
    types: &[Type],
    unnamed: bool,
) -> Result<Statement, Error> {
    let name = if unnamed {
        String::new()
    } else {
        format!("s{}", NEXT_ID.fetch_add(1, Ordering::SeqCst))
    };

    let buf = encode(client, &name, query, types)?;
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next().await? {
        Message::ParseComplete => {}
        m => return Err(Error::unexpected_message(m)),
    }

    let parameter_description = match responses.next().await? {
        Message::ParameterDescription(body) => body,
        m => return Err(Error::unexpected_message(m)),
    };

    let row_description = match responses.next().await? {
        Message::RowDescription(body) => Some(body),
        Message::NoData => None,
        m => return Err(Error::unexpected_message(m)),
    };

    let mut parameters = vec![];
    let mut it = parameter_description.parameters();
    while let Some(oid) = it.next().map_err(Error::parse)? {
        let type_ = get_type(client, oid).await?;
        parameters.push(type_);
    }

    let mut columns = vec![];
    if let Some(row_description) = row_description {
        let mut it = row_description.fields();
        while let Some(field) = it.next().map_err(Error::parse)? {
            let type_ = get_type(client, field.type_oid()).await?;
            let column = Column::new(field.name().to_string(), type_, field);
            columns.push(column);
        }
    }

    if unnamed {
        Ok(Statement::unnamed(query.to_owned(), parameters, columns))
    } else {
        Ok(Statement::named(client, name, parameters, columns))
    }
}

fn encode(client: &InnerClient, name: &str, query: &str, types: &[Type]) -> Result<Bytes, Error> {
    if types.is_empty() {
        debug!("preparing query {}: {}", name, query);
    } else {
        debug!("preparing query {} with types {:?}: {}", name, types, query);
    }

    client.with_buf(|buf| {
        frontend::parse(name, query, types.iter().map(Type::oid), buf).map_err(Error::encode)?;
        frontend::describe(b'S', name, buf).map_err(Error::encode)?;
        frontend::sync(buf);
        Ok(buf.split().freeze())
    })
}

pub async fn get_type(client: &Arc<InnerClient>, oid: Oid) -> Result<Type, Error> {
    if let Some(type_) = Type::from_oid(oid) {
        return Ok(type_);
    }

    if let Some(type_) = client.type_(oid) {
        return Ok(type_);
    }

    Ok(Type::TEXT)
}

use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::ToSql;
use crate::{query, Error, Portal, Statement};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

static NEXT_ID: AtomicUsize = AtomicUsize::new(0);

pub async fn bind(
    client: Arc<InnerClient>,
    statement: Statement,
    bind: Result<PendingBind, Error>,
) -> Result<Portal, Error> {
    let bind = bind?;

    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(bind.buf)))?;

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(Portal::new(&client, bind.name, statement))
}

pub struct PendingBind {
    buf: Vec<u8>,
    name: String,
}

pub fn encode<'a, I>(statement: &Statement, params: I) -> Result<PendingBind, Error>
where
    I: IntoIterator<Item = &'a dyn ToSql>,
    I::IntoIter: ExactSizeIterator,
{
    let name = format!("p{}", NEXT_ID.fetch_add(1, Ordering::SeqCst));
    let mut buf = query::encode_bind(statement, params, &name)?;
    frontend::sync(&mut buf);

    Ok(PendingBind { buf, name })
}

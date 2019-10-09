use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::{IsNull, ToSql};
use crate::{Error, Portal, Row, Statement};
use futures::{ready, Stream, TryFutureExt};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::pin::Pin;
use std::task::{Context, Poll};

pub async fn query<'a, I>(
    client: &InnerClient,
    statement: Statement,
    params: I,
) -> Result<RowStream, Error>
where
    I: IntoIterator<Item = &'a dyn ToSql>,
    I::IntoIter: ExactSizeIterator,
{
    let buf = encode(&statement, params)?;
    let responses = start(client, buf).await?;
    Ok(RowStream {
        statement,
        responses,
    })
}

pub fn query_portal<'a>(
    client: &'a InnerClient,
    portal: &'a Portal,
    max_rows: i32,
) -> impl Stream<Item = Result<Row, Error>> + 'a {
    let start = async move {
        let mut buf = vec![];
        frontend::execute(portal.name(), max_rows, &mut buf).map_err(Error::encode)?;
        frontend::sync(&mut buf);

        let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

        Ok(RowStream {
            statement: portal.statement().clone(),
            responses,
        })
    };

    start.try_flatten_stream()
}

pub async fn execute<'a, I>(
    client: &InnerClient,
    statement: Statement,
    params: I,
) -> Result<u64, Error>
where
    I: IntoIterator<Item = &'a dyn ToSql>,
    I::IntoIter: ExactSizeIterator,
{
    let buf = encode(&statement, params)?;
    let mut responses = start(client, buf).await?;

    loop {
        match responses.next().await? {
            Message::DataRow(_) => {}
            Message::CommandComplete(body) => {
                let rows = body
                    .tag()
                    .map_err(Error::parse)?
                    .rsplit(' ')
                    .next()
                    .unwrap()
                    .parse()
                    .unwrap_or(0);
                return Ok(rows);
            }
            Message::EmptyQueryResponse => return Ok(0),
            _ => return Err(Error::unexpected_message()),
        }
    }
}

async fn start(client: &InnerClient, buf: Vec<u8>) -> Result<Responses, Error> {
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

pub fn encode<'a, I>(statement: &Statement, params: I) -> Result<Vec<u8>, Error>
where
    I: IntoIterator<Item = &'a dyn ToSql>,
    I::IntoIter: ExactSizeIterator,
{
    let mut buf = encode_bind(statement, params, "")?;
    frontend::execute("", 0, &mut buf).map_err(Error::encode)?;
    frontend::sync(&mut buf);

    Ok(buf)
}

pub fn encode_bind<'a, I>(statement: &Statement, params: I, portal: &str) -> Result<Vec<u8>, Error>
where
    I: IntoIterator<Item = &'a dyn ToSql>,
    I::IntoIter: ExactSizeIterator,
{
    let params = params.into_iter();

    assert!(
        statement.params().len() == params.len(),
        "expected {} parameters but got {}",
        statement.params().len(),
        params.len()
    );

    let mut buf = vec![];

    let mut error_idx = 0;
    let r = frontend::bind(
        portal,
        statement.name(),
        Some(1),
        params.zip(statement.params()).enumerate(),
        |(idx, (param, ty)), buf| match param.to_sql_checked(ty, buf) {
            Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
            Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
            Err(e) => {
                error_idx = idx;
                Err(e)
            }
        },
        Some(1),
        &mut buf,
    );
    match r {
        Ok(()) => Ok(buf),
        Err(frontend::BindError::Conversion(e)) => return Err(Error::to_sql(e, error_idx)),
        Err(frontend::BindError::Serialization(e)) => return Err(Error::encode(e)),
    }
}

pub struct RowStream {
    statement: Statement,
    responses: Responses,
}

impl Stream for RowStream {
    type Item = Result<Row, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.responses.poll_next(cx)?) {
            Message::DataRow(body) => {
                Poll::Ready(Some(Ok(Row::new(self.statement.clone(), body)?)))
            }
            Message::EmptyQueryResponse
            | Message::CommandComplete(_)
            | Message::PortalSuspended => Poll::Ready(None),
            Message::ErrorResponse(body) => Poll::Ready(Some(Err(Error::db(body)))),
            _ => Poll::Ready(Some(Err(Error::unexpected_message()))),
        }
    }
}

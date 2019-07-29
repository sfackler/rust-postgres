use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{Error, SimpleQueryMessage, SimpleQueryRow};
use fallible_iterator::FallibleIterator;
use futures::{ready, Stream, TryFutureExt};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub fn simple_query(
    client: Arc<InnerClient>,
    query: &str,
) -> impl Stream<Item = Result<SimpleQueryMessage, Error>> {
    let buf = encode(query);

    let start = async move {
        let buf = buf?;
        let responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

        Ok(SimpleQuery {
            responses,
            columns: None,
        })
    };

    start.try_flatten_stream()
}

pub fn batch_execute(
    client: Arc<InnerClient>,
    query: &str,
) -> impl Future<Output = Result<(), Error>> {
    let buf = encode(query);

    async move {
        let buf = buf?;
        let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

        loop {
            match responses.next().await? {
                Message::ReadyForQuery(_) => return Ok(()),
                Message::CommandComplete(_)
                | Message::EmptyQueryResponse
                | Message::RowDescription(_)
                | Message::DataRow(_) => {}
                _ => return Err(Error::unexpected_message()),
            }
        }
    }
}

fn encode(query: &str) -> Result<Vec<u8>, Error> {
    let mut buf = vec![];
    frontend::query(query, &mut buf).map_err(Error::encode)?;
    Ok(buf)
}

struct SimpleQuery {
    responses: Responses,
    columns: Option<Arc<[String]>>,
}

impl Stream for SimpleQuery {
    type Item = Result<SimpleQueryMessage, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match ready!(self.responses.poll_next(cx)?) {
                Message::CommandComplete(body) => {
                    let rows = body
                        .tag()
                        .map_err(Error::parse)?
                        .rsplit(' ')
                        .next()
                        .unwrap()
                        .parse()
                        .unwrap_or(0);
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::CommandComplete(rows))));
                }
                Message::EmptyQueryResponse => {
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::CommandComplete(0))));
                }
                Message::RowDescription(body) => {
                    let columns = body
                        .fields()
                        .map(|f| Ok(f.name().to_string()))
                        .collect::<Vec<_>>()
                        .map_err(Error::parse)?
                        .into();
                    self.columns = Some(columns);
                }
                Message::DataRow(body) => {
                    let row = match &self.columns {
                        Some(columns) => SimpleQueryRow::new(columns.clone(), body)?,
                        None => return Poll::Ready(Some(Err(Error::unexpected_message()))),
                    };
                    return Poll::Ready(Some(Ok(SimpleQueryMessage::Row(row))));
                }
                Message::ReadyForQuery(_) => return Poll::Ready(None),
                _ => return Poll::Ready(Some(Err(Error::unexpected_message()))),
            }
        }
    }
}

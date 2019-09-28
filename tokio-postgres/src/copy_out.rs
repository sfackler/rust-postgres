use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::ToSql;
use crate::{query, Error, Statement};
use bytes::Bytes;
use futures::{ready, Stream, TryFutureExt};
use postgres_protocol::message::backend::Message;
use std::pin::Pin;
use std::task::{Context, Poll};

pub fn copy_out<'a, I>(
    client: &'a InnerClient,
    statement: Statement,
    params: I,
) -> impl Stream<Item = Result<Bytes, Error>> + 'a
where
    I: IntoIterator<Item = &'a dyn ToSql> + 'a,
    I::IntoIter: ExactSizeIterator,
{
    let f = async move {
        let buf = query::encode(&statement, params)?;
        let responses = start(client, buf).await?;
        Ok(CopyOut { responses })
    };
    f.try_flatten_stream()
}

async fn start(client: &InnerClient, buf: Vec<u8>) -> Result<Responses, Error> {
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next().await? {
        Message::BindComplete => {}
        _ => return Err(Error::unexpected_message()),
    }

    match responses.next().await? {
        Message::CopyOutResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

struct CopyOut {
    responses: Responses,
}

impl Stream for CopyOut {
    type Item = Result<Bytes, Error>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match ready!(self.responses.poll_next(cx)?) {
            Message::CopyData(body) => Poll::Ready(Some(Ok(body.into_bytes()))),
            Message::CopyDone => Poll::Ready(None),
            _ => Poll::Ready(Some(Err(Error::unexpected_message()))),
        }
    }
}

use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::Error;
use bytes::Bytes;
use futures::{ready, Stream, TryFutureExt};
use postgres_protocol::message::backend::Message;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

pub fn copy_out(
    client: Arc<InnerClient>,
    buf: Result<Vec<u8>, Error>,
) -> impl Stream<Item = Result<Bytes, Error>> {
    start(client, buf)
        .map_ok(|responses| CopyOut { responses })
        .try_flatten_stream()
}

async fn start(client: Arc<InnerClient>, buf: Result<Vec<u8>, Error>) -> Result<Responses, Error> {
    let buf = buf?;
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

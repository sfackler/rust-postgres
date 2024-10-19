use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{simple_query, Error};
use bytes::Bytes;
use futures_util::{ready, Stream};
use log::debug;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};

pub async fn copy_both(client: &InnerClient, query: &str) -> Result<CopyBothStream, Error> {
    debug!("executing copy out statement {query}");

    let buf = simple_query::encode(client, query)?;
    let responses = start(client, buf).await?;
    Ok(CopyBothStream {
        responses,
        _p: PhantomPinned,
    })
}

async fn start(client: &InnerClient, buf: Bytes) -> Result<Responses, Error> {
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    match responses.next().await? {
        Message::CopyBothResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

pin_project! {
    pub struct CopyBothStream {
        responses: Responses,
        #[pin]
        _p: PhantomPinned,
    }
}

impl Stream for CopyBothStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match ready!(this.responses.poll_next(cx)?) {
            Message::CopyData(body) => Poll::Ready(Some(Ok(body.into_bytes()))),
            Message::CopyDone => Poll::Ready(None),
            _ => Poll::Ready(Some(Err(Error::unexpected_message()))),
        }
    }
}

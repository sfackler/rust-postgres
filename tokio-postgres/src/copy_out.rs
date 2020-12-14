use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{query, simple_query, slice_iter, Error, Statement};
use bytes::Bytes;
use futures_util::{ready, Stream};
use log::debug;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};

pub async fn copy_out_simple(client: &InnerClient, query: &str) -> Result<CopyOutStream, Error> {
    debug!("executing copy out query {}", query);

    let buf = simple_query::encode(client, query)?;
    let responses = start(client, buf, true).await?;
    Ok(CopyOutStream {
        responses,
        _p: PhantomPinned,
    })
}

pub async fn copy_out(client: &InnerClient, statement: Statement) -> Result<CopyOutStream, Error> {
    debug!("executing copy out statement {}", statement.name());

    let buf = query::encode(client, &statement, slice_iter(&[]))?;
    let responses = start(client, buf, false).await?;
    Ok(CopyOutStream {
        responses,
        _p: PhantomPinned,
    })
}

async fn start(client: &InnerClient, buf: Bytes, simple: bool) -> Result<Responses, Error> {
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;

    if !simple {
        match responses.next().await? {
            Message::BindComplete => {}
            _ => return Err(Error::unexpected_message()),
        }
    }

    match responses.next().await? {
        Message::CopyOutResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

pin_project! {
    /// A stream of `COPY ... TO STDOUT` query data.
    pub struct CopyOutStream {
        responses: Responses,
        #[pin]
        _p: PhantomPinned,
    }
}

impl Stream for CopyOutStream {
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

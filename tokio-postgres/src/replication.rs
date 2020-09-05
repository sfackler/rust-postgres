use crate::client::{InnerClient, Responses};
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{simple_query, Error};
use bytes::{Bytes, BytesMut};
use futures::{ready, Stream};
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
//use std::marker::PhantomPinned;
use std::pin::Pin;
use std::task::{Context, Poll};
use log::{trace};
use postgres_protocol::message::frontend;

pub async fn start_replication(client: &InnerClient, query: &str) -> Result<ReplicationStream, Error> {
    trace!("executing start replication query {}", query);

    let buf = simple_query::encode(client, query)?;
    let responses = start(client, buf).await?;
    Ok(ReplicationStream {
        responses,
        // _p: PhantomPinned,
    })
}

pub async fn stop_replication(client: &InnerClient) -> Result<(), Error> {
    trace!("executing stop replication");
    let mut buf = BytesMut::new();
        frontend::copy_done(&mut buf);
        frontend::sync(&mut buf);
    let _ = client.send(RequestMessages::Single(FrontendMessage::Raw(buf.freeze())))?;
    Ok(())
}

async fn start(client: &InnerClient, buf: Bytes) -> Result<Responses, Error> {
    
    let mut responses = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)))?;
    trace!("start in repication");

    match responses.next().await? {
        Message::CopyBothResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }

    Ok(responses)
}

pin_project! {
    /// A stream of `START_REPLICATION` query data.
    pub struct ReplicationStream {
        responses: Responses,
        // #[pin]
        // _p: PhantomPinned,
    }
}

impl Stream for ReplicationStream {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();

        match ready!(this.responses.poll_next(cx)?) {
            Message::CopyData(body) => Poll::Ready(Some(Ok(body.into_bytes()))),
            Message::CopyDone => Poll::Ready(None),
            _ => Poll::Ready(Some(Err(Error::unexpected_message()))),
            // m => {
            //     debug!("msg: {:?}", m);
            //     Poll::Ready(Some(Ok(Bytes::from("Hello world"))))
            // }
        }
    }
}

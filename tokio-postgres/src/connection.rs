use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::error::{DbError, Error};
use crate::maybe_tls_stream::MaybeTlsStream;
use futures::channel::mpsc;
use futures::task::Context;
use futures::{ready, Future, Poll, Stream, StreamExt};
use std::collections::HashMap;
use std::io;
use std::pin::Pin;
use tokio::codec::Framed;
use tokio::io::{AsyncRead, AsyncWrite};

/// An asynchronous notification.
#[derive(Clone, Debug)]
pub struct Notification {
    process_id: i32,
    channel: String,
    payload: String,
}

/// An asynchronous message from the server.
#[allow(clippy::large_enum_variant)]
pub enum AsyncMessage {
    /// A notice.
    ///
    /// Notices use the same format as errors, but aren't "errors" per-se.
    Notice(DbError),
    /// A notification.
    ///
    /// Connections can subscribe to notifications with the `LISTEN` command.
    Notification(Notification),
    #[doc(hidden)]
    __NonExhaustive,
}

pub enum RequestMessages {
    Single(FrontendMessage),
}

pub struct Request {
    pub messages: RequestMessages,
    pub sender: mpsc::Sender<BackendMessages>,
}

pub struct Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    parameters: HashMap<String, String>,
    receiver: mpsc::UnboundedReceiver<Request>,

    pending_response: Option<BackendMessage>,
}

impl<S, T> Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        parameters: HashMap<String, String>,
        receiver: mpsc::UnboundedReceiver<Request>,
    ) -> Connection<S, T> {
        Connection {
            stream,
            parameters,
            receiver,

            pending_response: None,
        }
    }

    // #TODO
    // Original source:
    // https://github.com/sfackler/rust-postgres/blob/master/tokio-postgres/src/proto/connection.rs#L87

    fn poll_response(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<BackendMessage, io::Error>>> {
        if let Some(message) = self.pending_response.take() {
            //trace!("retrying pending response");
            return Poll::Ready(Some(Ok(message)));
        }

        self.stream.poll_next_unpin(cx)
    }

    pub fn poll_message(&self, cx: &mut Context<'_>) -> Poll<Result<Option<AsyncMessage>, Error>> {
        unimplemented!()
    }
}

impl<S, T> Future for Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        while let Ok(Some(_)) = ready!(self.poll_message(cx)) {}
        Poll::Ready(Ok(()))
    }
}

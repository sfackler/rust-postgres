use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::error::{DbError, Error};
use crate::maybe_tls_stream::MaybeTlsStream;
use futures::channel::mpsc;
use futures::task::Context;
use futures::{ready, Future, Poll};
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

pub struct Connection<S, T> {
    stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    parameters: HashMap<String, String>,
    receiver: mpsc::UnboundedReceiver<Request>,

    pending_response: Option<BackendMessage>,
}

impl<S, T> Connection<S, T> {
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
}

impl<S, T> Connection<S, T>
where
    S: AsyncRead + AsyncWrite,
{
    // #TODO
    // Original source:
    // https://github.com/sfackler/rust-postgres/blob/master/tokio-postgres/src/proto/connection.rs#L87

    fn poll_response(&self, cx: &mut Context) -> Poll<Result<Option<BackendMessage>, io::Error>> {
        /*if let Some(message) = self.pending_response.take() {
            trace!("retrying pending response");
            return Ok(Async::Ready(Some(message)));
        }

        self.stream.poll_next(cx)*/
        unimplemented!()
    }

    pub fn poll_message(&self, cx: &mut Context) -> Poll<Result<Option<AsyncMessage>, Error>> {
        unimplemented!()
    }
}

impl<S, T> Future for Connection<S, T>
where
    S: AsyncRead + AsyncWrite,
{
    type Output = Result<(), Error>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), Error>> {
        while let Ok(Some(_)) = ready!(self.poll_message(cx)) {}
        Poll::Ready(Ok(()))
    }
}

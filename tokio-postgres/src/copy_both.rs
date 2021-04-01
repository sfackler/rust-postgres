use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::{simple_query, Error};
use bytes::{Buf, BufMut, Bytes, BytesMut};
use futures_channel::mpsc;
use futures_util::{ready, Sink, SinkExt, Stream, StreamExt};
use log::debug;
use pin_project_lite::pin_project;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use postgres_protocol::message::frontend::CopyData;
use std::marker::{PhantomData, PhantomPinned};
use std::pin::Pin;
use std::task::{Context, Poll};

/// The state machine of CopyBothReceiver
///
/// ```ignore
///      CopyBoth
///       /   \
///      v     v
///  CopyOut  CopyIn
///       \   /
///        v v
///      CopyNone
///         |
///         v
///    CopyComplete
///         |
///         v
///   CommandComplete
/// ```
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CopyBothState {
    /// Initial state where CopyData messages can go in both directions
    CopyBoth,
    /// The server->client stream is closed and we're in CopyIn mode
    CopyIn,
    /// The client->server stream is closed and we're in CopyOut mode
    CopyOut,
    /// Both directions are closed, we waiting for CommandComplete messages
    CopyNone,
    /// We have received the first CommandComplete message for the copy
    CopyComplete,
    /// We have received the final CommandComplete message for the statement
    CommandComplete,
}

/// A CopyBothReceiver is responsible for handling the CopyBoth subprotocol. It ensures that no
/// matter what the users do with their CopyBothDuplex handle we're always going to send the
/// correct messages to the backend in order to restore the connection into a usable state.
///
/// ```ignore
///                                          |
///          <tokio_postgres owned>          |    <userland owned>
///                                          |
///  pg -> Connection -> CopyBothReceiver ---+---> CopyBothDuplex
///                                          |          ^   \
///                                          |         /     v
///                                          |      Sink    Stream
/// ```
pub struct CopyBothReceiver {
    /// Receiver of backend messages from the underlying [Connection](crate::Connection)
    message_receiver: mpsc::Receiver<Result<Message, Error>>,
    /// Receiver of frontend messages sent by the user using <CopyBothDuplex as Sink>
    sink_receiver: mpsc::Receiver<FrontendMessage>,
    /// Sender of CopyData contents to be consumed by the user using <CopyBothDuplex as Stream>
    stream_sender: mpsc::Sender<Result<Bytes, Error>>,
    /// The current state of the subprotocol
    state: CopyBothState,
    /// Holds a buffered message until we are ready to send it to the user's stream
    buffered_message: Option<Result<Bytes, Error>>,
}

impl CopyBothReceiver {
    pub(crate) fn new(
        message_receiver: mpsc::Receiver<Result<Message, Error>>,
        sink_receiver: mpsc::Receiver<FrontendMessage>,
        stream_sender: mpsc::Sender<Result<Bytes, Error>>,
    ) -> CopyBothReceiver {
        CopyBothReceiver {
            message_receiver,
            sink_receiver,
            stream_sender,
            state: CopyBothState::CopyBoth,
            buffered_message: None,
        }
    }

    /// Convenience method to set the subprotocol into an unexpected message state
    fn unexpected_message(&mut self) {
        self.sink_receiver.close();
        self.buffered_message = Some(Err(Error::unexpected_message()));
        self.state = CopyBothState::CommandComplete;
    }

    /// Processes messages from the backend, it will resolve once all backend messages have been
    /// processed
    fn poll_backend(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        use CopyBothState::*;

        loop {
            // Deliver the buffered message (if any) to the user to ensure we can potentially
            // buffer a new one in response to a server message
            if let Some(message) = self.buffered_message.take() {
                match self.stream_sender.poll_ready(cx) {
                    Poll::Ready(_) => {
                        // If the receiver has hung up we'll just drop the message
                        let _ = self.stream_sender.start_send(message);
                    }
                    Poll::Pending => {
                        // Stash the message and try again later
                        self.buffered_message = Some(message);
                        return Poll::Pending;
                    }
                }
            }

            match ready!(self.message_receiver.poll_next_unpin(cx)) {
                Some(Ok(Message::CopyData(body))) => match self.state {
                    CopyBoth | CopyOut => {
                        self.buffered_message = Some(Ok(body.into_bytes()));
                    }
                    _ => self.unexpected_message(),
                },
                // The server->client stream is done
                Some(Ok(Message::CopyDone)) => {
                    match self.state {
                        CopyBoth => self.state = CopyIn,
                        CopyOut => self.state = CopyNone,
                        _ => self.unexpected_message(),
                    };
                }
                Some(Ok(Message::CommandComplete(_))) => {
                    match self.state {
                        CopyNone => self.state = CopyComplete,
                        CopyComplete => {
                            self.stream_sender.close_channel();
                            self.sink_receiver.close();
                            self.state = CommandComplete;
                        }
                        _ => self.unexpected_message(),
                    };
                }
                // The server indicated an error, terminate our side if we haven't already
                Some(Err(err)) => {
                    match self.state {
                        CopyBoth | CopyOut | CopyIn => {
                            self.sink_receiver.close();
                            self.buffered_message = Some(Err(err));
                            self.state = CommandComplete;
                        }
                        _ => self.unexpected_message(),
                    };
                }
                Some(Ok(Message::ReadyForQuery(_))) => match self.state {
                    CommandComplete => {
                        self.sink_receiver.close();
                        self.stream_sender.close_channel();
                    }
                    _ => self.unexpected_message(),
                },
                Some(Ok(_)) => self.unexpected_message(),
                None => return Poll::Ready(()),
            }
        }
    }
}

/// The [Connection](crate::Connection) will keep polling this stream until it is exhausted. This
/// is the mechanism that drives the CopyBoth subprotocol forward
impl Stream for CopyBothReceiver {
    type Item = FrontendMessage;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<FrontendMessage>> {
        use CopyBothState::*;

        match self.poll_backend(cx) {
            Poll::Ready(()) => Poll::Ready(None),
            Poll::Pending => match self.state {
                CopyBoth | CopyIn => match ready!(self.sink_receiver.poll_next_unpin(cx)) {
                    Some(msg) => Poll::Ready(Some(msg)),
                    None => {
                        self.state = match self.state {
                            CopyBoth => CopyOut,
                            CopyIn => CopyNone,
                            _ => unreachable!(),
                        };

                        let mut buf = BytesMut::new();
                        frontend::copy_done(&mut buf);
                        Poll::Ready(Some(FrontendMessage::Raw(buf.freeze())))
                    }
                },
                _ => Poll::Pending,
            },
        }
    }
}

pin_project! {
    /// A duplex stream for consuming streaming replication data.
    ///
    /// Users should ensure that CopyBothDuplex is dropped before attempting to await on a new
    /// query. This will ensure that the connection returns into normal processing mode.
    ///
    /// ```no_run
    /// use tokio_postgres::Client;
    ///
    /// async fn foo(client: &Client) {
    ///   let duplex_stream = client.copy_both_simple::<&[u8]>("..").await;
    ///
    ///   // ⚠️ INCORRECT ⚠️
    ///   client.query("SELECT 1", &[]).await; // hangs forever
    ///
    ///   // duplex_stream drop-ed here
    /// }
    /// ```
    ///
    /// ```no_run
    /// use tokio_postgres::Client;
    ///
    /// async fn foo(client: &Client) {
    ///   let duplex_stream = client.copy_both_simple::<&[u8]>("..").await;
    ///
    ///   // ✅ CORRECT ✅
    ///   drop(duplex_stream);
    ///
    ///   client.query("SELECT 1", &[]).await;
    /// }
    /// ```
    pub struct CopyBothDuplex<T> {
        #[pin]
        sink_sender: mpsc::Sender<FrontendMessage>,
        #[pin]
        stream_receiver: mpsc::Receiver<Result<Bytes, Error>>,
        buf: BytesMut,
        #[pin]
        _p: PhantomPinned,
        _p2: PhantomData<T>,
    }
}

impl<T> Stream for CopyBothDuplex<T> {
    type Item = Result<Bytes, Error>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().stream_receiver.poll_next(cx)
    }
}

impl<T> Sink<T> for CopyBothDuplex<T>
where
    T: Buf + 'static + Send,
{
    type Error = Error;

    fn poll_ready(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        self.project()
            .sink_sender
            .poll_ready(cx)
            .map_err(|_| Error::closed())
    }

    fn start_send(self: Pin<&mut Self>, item: T) -> Result<(), Error> {
        let this = self.project();

        let data: Box<dyn Buf + Send> = if item.remaining() > 4096 {
            if this.buf.is_empty() {
                Box::new(item)
            } else {
                Box::new(this.buf.split().freeze().chain(item))
            }
        } else {
            this.buf.put(item);
            if this.buf.len() > 4096 {
                Box::new(this.buf.split().freeze())
            } else {
                return Ok(());
            }
        };

        let data = CopyData::new(data).map_err(Error::encode)?;
        this.sink_sender
            .start_send(FrontendMessage::CopyData(data))
            .map_err(|_| Error::closed())
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        let mut this = self.project();

        if !this.buf.is_empty() {
            ready!(this.sink_sender.as_mut().poll_ready(cx)).map_err(|_| Error::closed())?;
            let data: Box<dyn Buf + Send> = Box::new(this.buf.split().freeze());
            let data = CopyData::new(data).map_err(Error::encode)?;
            this.sink_sender
                .as_mut()
                .start_send(FrontendMessage::CopyData(data))
                .map_err(|_| Error::closed())?;
        }

        this.sink_sender.poll_flush(cx).map_err(|_| Error::closed())
    }

    fn poll_close(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        ready!(self.as_mut().poll_flush(cx))?;
        let mut this = self.as_mut().project();
        this.sink_sender.disconnect();
        Poll::Ready(Ok(()))
    }
}

pub async fn copy_both_simple<T>(
    client: &InnerClient,
    query: &str,
) -> Result<CopyBothDuplex<T>, Error>
where
    T: Buf + 'static + Send,
{
    debug!("executing copy both query {}", query);

    let buf = simple_query::encode(client, query)?;

    let (message_sender, message_receiver) = mpsc::channel(0);
    let (stream_sender, stream_receiver) = mpsc::channel(0);
    let (mut sink_sender, sink_receiver) = mpsc::channel(0);

    let receiver = CopyBothReceiver::new(message_receiver, sink_receiver, stream_sender);
    let mut responses = client.send(RequestMessages::CopyBoth(receiver))?;

    sink_sender
        .send(FrontendMessage::Raw(buf))
        .await
        .map_err(|_| Error::closed())?;

    match responses.next().await? {
        Message::CopyBothResponse(_) => {}
        _ => return Err(Error::unexpected_message()),
    }

    // We've entered CopyBoth mode, all backend messages should now go to the sub-protocol handler
    // to ensure that we always send the correct messages regardless of what the user does with
    // their CopyBothDuplex handle.
    tokio::spawn(responses.map(Ok).forward(message_sender));

    Ok(CopyBothDuplex {
        stream_receiver,
        sink_sender,
        buf: BytesMut::new(),
        _p: PhantomPinned,
        _p2: PhantomData,
    })
}

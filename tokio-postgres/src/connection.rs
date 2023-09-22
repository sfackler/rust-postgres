//! [`Connection`] definitions.

use bytes::BytesMut;
use futures_channel::mpsc;
use futures_util::{Sink, Stream, StreamExt};
use postgres_protocol::message::{backend::Message, frontend};
use std::{
    collections::{HashMap, VecDeque},
    future::Future,
    mem,
    pin::Pin,
    task::{ready, Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

use crate::{
    codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec},
    copy_in::CopyInReceiver,
    error::DbError,
    maybe_tls_stream::MaybeTlsStream,
    AsyncMessage, Error, Notification,
};

/// A background connection to a PostgreSQL database.
///
/// Instead of [`Client`] the [`Connection`] implements [`Future`] and performs I/O operations with
/// the PostgreSQL server. This type mostly should be spawned off onto an executor to run in the
/// background.
///
/// Also, the [`Connection`] is used to receive the [`AsyncMessage`]s (notices and notifications)
/// from the PostgreSQL server. To do that, the [`Connection`] should be polled manually with the
/// [`poll_message`] method.
///
/// [`Client`]: crate::Client
/// [`poll_message`]: Connection::poll_message
#[must_use = "futures do nothing unless polled"]
pub struct Connection<S, T> {
    /// Current state of this [`Connection`].
    state: State,

    /// Receiver of the new [`Request`]s.
    receiver: mpsc::UnboundedReceiver<Request>,

    /// Database connection stream.
    stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,

    /// Parameters of this [`Connection`].
    parameters: HashMap<String, String>,

    /// Messages received from the database.
    messages: VecDeque<AsyncMessage>,

    /// [`Request`]s not fully received yet.
    pending_requests: VecDeque<PendingRequest>,

    /// [`Response`]s not fully sent yet.
    pending_responses: VecDeque<PendingResponse>,

    /// Expected [`Response`]s of the received [`Request`]s.
    expected_responses: VecDeque<Response>,

    /// Indicator whether this [`Connection`] is flushing the data to its
    /// `stream`.
    flushing: bool,
}

impl<S, T> Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    /// Creates a new [`Connection`].
    ///
    /// # Errors
    ///
    /// If failed to process the delayed [`Message`]s.
    pub(crate) fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        receiver: mpsc::UnboundedReceiver<Request>,
        parameters: HashMap<String, String>,
        delayed_messages: VecDeque<Message>,
    ) -> Result<Self, Error> {
        let mut this = Connection {
            state: State::Active,
            stream,
            receiver,
            parameters,
            messages: VecDeque::new(),
            pending_requests: VecDeque::new(),
            pending_responses: VecDeque::new(),
            expected_responses: VecDeque::new(),
            flushing: false,
        };

        for msg in delayed_messages {
            this.handle_message(msg)?;
        }

        Ok(this)
    }

    /// Returns the value of a runtime parameter for this [`Connection`].
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(|s| &**s)
    }

    /// Handles a [`Message`] received from the database.
    ///
    /// # Errors
    ///
    /// If failed to parse the provided [`Message`].
    fn handle_message(&mut self, msg: Message) -> Result<(), Error> {
        match msg {
            Message::NoticeResponse(body) => self.messages.push_back(AsyncMessage::Notice(
                DbError::parse(&mut body.fields()).map_err(Error::parse)?,
            )),
            Message::NotificationResponse(body) => {
                self.messages
                    .push_back(AsyncMessage::Notification(Notification {
                        process_id: body.process_id(),
                        channel: body.channel().map_err(Error::parse)?.to_string(),
                        payload: body.message().map_err(Error::parse)?.to_string(),
                    }))
            }
            Message::ParameterStatus(body) => {
                self.parameters.insert(
                    body.name().map_err(Error::parse)?.to_string(),
                    body.value().map_err(Error::parse)?.to_string(),
                );
            }
            _ => unreachable!("unexpected message"),
        }

        Ok(())
    }

    /// Polls this [`Connection`] updating its state and returning the next
    /// [`AsyncMessage`], if available.
    ///
    /// `None` or `Some(Err(_))` are returned in case this [`Connection`] is
    /// closed. Callers should not invoke this method again after receiving one
    /// of those values.
    ///
    /// The PostgreSQL server can send notices as well as notifications asynchronously to
    /// this [`Connection`]. Applications that wish to examine those [`AsyncMessage`]s should use
    /// this method to drive the [`Connection`] rather than its [`Future`] implementation.
    pub fn poll_message(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<AsyncMessage, Error>>> {
        self.poll_read(cx)?;
        self.poll_write(cx)?;
        let shutdown = self.poll_shutdown(cx)?;

        self.messages
            .pop_front()
            .map_or(shutdown.map(|_| None), |m| Poll::Ready(Some(Ok(m))))
    }

    /// Reads incoming messages from PostgreSQL server:
    /// - [`AsyncMessage`]s (notices and notifications);
    /// - [`BackendMessage`]s ([`Response`]s to the [`Request`]s).
    ///
    /// # Errors
    ///
    /// If failed to read the messages from the PostgreSQL server.
    fn poll_read(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        // First, read all available messages from the server.
        if self.state == State::Active {
            loop {
                let res = Pin::new(&mut self.stream)
                    .poll_next(cx)
                    .map(|o| o.map(|r| r.map_err(Error::io)))?;
                let (messages, response_complete) = match res {
                    Poll::Ready(Some(BackendMessage::Async(msg))) => {
                        self.handle_message(msg)?;
                        continue;
                    }
                    Poll::Ready(Some(BackendMessage::Normal {
                        messages,
                        request_complete,
                    })) => (messages, request_complete),
                    Poll::Ready(None) | Poll::Pending => {
                        break;
                    }
                };

                if let Some(pending) = self.pending_responses.back_mut() {
                    if !pending.completed {
                        pending.messages.extend(messages);
                        pending.completed = response_complete;
                        continue;
                    }
                }

                let response = self.expected_responses.pop_front().unwrap_or_else(|| {
                    panic!("Response received from PostgreSQL without a matching request")
                });
                self.pending_responses.push_back(PendingResponse {
                    response: Some(response),
                    messages,
                    completed: response_complete,
                });
            }
        }

        // Then send the received responses to the client of this connection.
        self.pending_responses.retain_mut(|pending| {
            let Some(response) = &mut pending.response else {
                return !pending.completed;
            };

            match response.sender.poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    // We don't care about the result of this send, because channel can be closed
                    // by the client side while we're receiving the responses from the postgres server.
                    let _ = response.sender.start_send(mem::take(&mut pending.messages));
                    !pending.completed
                }
                Poll::Ready(Err(_)) => {
                    // Keep the message even if the client side receiver is closed to
                    // read full response from the postgres server.
                    pending.response = None;
                    !pending.completed
                }
                Poll::Pending => true,
            }
        });

        Ok(())
    }

    /// Sends the [`Request`]s to PostgreSQL server.
    ///
    /// # Errors
    ///
    /// If failed to send the [`Request`]s to the PostgreSQL server.
    fn poll_write(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        // First, receive all available requests from the client.
        if self.state != State::Closing {
            loop {
                let req = match self.receiver.poll_next_unpin(cx) {
                    Poll::Ready(Some(req)) => req.into(),
                    Poll::Ready(None)
                        if self.pending_responses.is_empty() && self.state == State::Active =>
                    {
                        self.state = State::CloseRequested;
                        let mut request = BytesMut::new();
                        frontend::terminate(&mut request);
                        PendingRequest {
                            messages: RequestMessages::Single(FrontendMessage::Raw(
                                request.freeze(),
                            )),
                            sender: None,
                        }
                    }
                    Poll::Ready(None) | Poll::Pending => {
                        // Even if client is closed, wait for all pending responses to be sent.
                        break;
                    }
                };

                self.pending_requests.push_back(req);
            }
        }

        loop {
            // Flush the previously written messages.
            if self.flushing {
                match Pin::new(&mut self.stream)
                    .poll_flush(cx)
                    .map_err(Error::io)?
                {
                    Poll::Ready(()) => {
                        self.flushing = false;
                    }
                    Poll::Pending => {
                        // We can't write any more messages until the previous ones are flushed.
                        break;
                    }
                }
            }

            let Some(req) = self.pending_requests.pop_front() else {
                // Nothing to send.
                break;
            };

            if Pin::new(&mut self.stream)
                .poll_ready(cx)
                .map_err(Error::io)?
                .is_pending()
            {
                // Socket is not ready yet, try in the next iteration.
                self.pending_requests.push_front(req);
                break;
            }

            match req.messages {
                RequestMessages::Single(msg) => {
                    Pin::new(&mut self.stream)
                        .start_send(msg)
                        .map_err(Error::io)?;
                    if self.state == State::CloseRequested {
                        // EOF message sent, now wait for a flush to perform a graceful shutdown.
                        self.state = State::Closing;
                    }
                }
                RequestMessages::CopyIn(mut receiver) => {
                    let msg = match receiver.poll_next_unpin(cx) {
                        Poll::Ready(Some(msg)) => msg,
                        Poll::Ready(None) => {
                            // No receiver found, move to the next message.
                            continue;
                        }
                        Poll::Pending => {
                            // Message not ready yet, wait for it.
                            self.pending_requests.push_front(PendingRequest {
                                messages: RequestMessages::CopyIn(receiver),
                                sender: req.sender,
                            });
                            continue;
                        }
                    };

                    Pin::new(&mut self.stream)
                        .start_send(msg)
                        .map_err(Error::io)?;
                }
            }

            if let Some(sender) = req.sender {
                self.expected_responses.push_back(Response { sender });
            }
            self.flushing = true;
        }

        Ok(())
    }

    /// Performs a graceful shutdown of this [`Connection`].
    ///
    /// # Errors
    ///
    /// If failed to perform a graceful shutdown of this [`Connection`].
    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        match self.state {
            State::Active | State::CloseRequested => Poll::Pending,
            State::Closing if self.flushing => Poll::Pending,
            State::Closing => {
                ready!(Pin::new(&mut self.stream).poll_close(cx)).map_err(Error::io)?;
                self.state = State::Closed;
                Poll::Ready(Ok(()))
            }
            State::Closed => Poll::Ready(Ok(())),
        }
    }
}

impl<S, T> Future for Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    type Output = Result<(), Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        while let Some(AsyncMessage::Notice(notice)) = ready!(self.poll_message(cx)?) {
            log::info!("{}: {}", notice.severity(), notice.message());
        }

        Poll::Ready(Ok(()))
    }
}

/// State of the [`Connection`].
#[derive(PartialEq, Debug)]
enum State {
    /// [`Connection`] is active.
    Active,

    /// [`Connection`] requested to terminate from the [`Client`].
    ///
    /// [`Client`]: crate::Client
    CloseRequested,

    /// [`Connection`] performing a graceful shutdown.
    Closing,

    /// [`Connection`] is closed.
    Closed,
}

/// Request sent by a [`Client`].
///
/// [`Client`]: crate::Client
pub struct Request {
    /// Messages to send to the database.
    pub messages: RequestMessages,

    /// Sender to which the responses to this request should be sent.
    pub sender: mpsc::Sender<BackendMessages>,
}

/// [`Request`] not fully received by a [`Connection`].
struct PendingRequest {
    /// Messages to send to the database.
    messages: RequestMessages,

    /// Sender to which the responses to this request should be sent, if any.
    sender: Option<mpsc::Sender<BackendMessages>>,
}

impl From<Request> for PendingRequest {
    fn from(req: Request) -> Self {
        Self {
            messages: req.messages,
            sender: Some(req.sender),
        }
    }
}

/// Messages sent by a [`Client`] to a [`Connection`].
///
/// [`Client`]: crate::Client
pub enum RequestMessages {
    /// A single message.
    Single(FrontendMessage),

    /// A copy-in stream.
    CopyIn(CopyInReceiver),
}

/// Response being sent to a [`Client`].
#[derive(Debug)]
pub struct Response {
    /// Sender to which [`BackendMessages`] of this [`Response`] should be sent.
    sender: mpsc::Sender<BackendMessages>,
}

/// [`Response`] not fully sent to a [`Client`].
#[derive(Debug)]
struct PendingResponse {
    /// [`Response`] itself.
    ///
    /// [`None`] if [`Client`]-side receiver is closed.
    response: Option<Response>,

    /// [`BackendMessages`] of this [`Response`].
    messages: BackendMessages,

    /// Indicator whether this [`Response`] is fully received from the PostgreSQL server.
    completed: bool,
}

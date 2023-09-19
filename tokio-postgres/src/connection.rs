use crate::codec::{BackendMessage, BackendMessages, FrontendMessage, PostgresCodec};
use crate::copy_in::CopyInReceiver;
use crate::error::DbError;
use crate::maybe_tls_stream::MaybeTlsStream;
use crate::{AsyncMessage, Error, Notification};
use bytes::BytesMut;
use futures_channel::mpsc;
use futures_util::{ready, Sink, Stream, StreamExt};
use log::info;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;

pub enum RequestMessages {
    Single(FrontendMessage),
    CopyIn(CopyInReceiver),
}

pub struct Request {
    pub messages: RequestMessages,
    pub sender: mpsc::Sender<BackendMessages>,
}

pub struct Response {
    sender: mpsc::Sender<BackendMessages>,
}

#[derive(PartialEq, Debug)]
enum State {
    Active,
    Terminating,
    Closing,
}

/// A connection to a PostgreSQL database.
///
/// This is one half of what is returned when a new connection is established. It performs the actual IO with the
/// server, and should generally be spawned off onto an executor to run in the background.
///
/// `Connection` implements `Future`, and only resolves when the connection is closed, either because a fatal error has
/// occurred, or because its associated `Client` has dropped and all outstanding work has completed.
#[must_use = "futures do nothing unless polled"]
pub struct Connection<S, T> {
    stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    parameters: HashMap<String, String>,
    receiver: mpsc::UnboundedReceiver<Request>,
    pending_requests: VecDeque<PendingRequest>,
    pending_responses: VecDeque<PendingResponse>,
    responses: VecDeque<Response>,
    messages: VecDeque<AsyncMessage>,
    flushing: bool,
    state: State,
}

impl<S, T> Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    pub(crate) fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        delayed_messages: VecDeque<Message>,
        parameters: HashMap<String, String>,
        receiver: mpsc::UnboundedReceiver<Request>,
    ) -> Result<Self, Error> {
        let mut this = Connection {
            stream,
            parameters,
            receiver,
            pending_requests: VecDeque::new(),
            pending_responses: VecDeque::new(),
            responses: VecDeque::new(),
            messages: VecDeque::new(),
            flushing: false,
            state: State::Active,
        };

        for msg in delayed_messages {
            this.handle_message(msg)?;
        }

        Ok(this)
    }

    /// Returns the value of a runtime parameter for this connection.
    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(|s| &**s)
    }

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
            _ => unreachable!("unexpected message: {:?}", msg),
        }

        Ok(())
    }

    /// Polls for asynchronous messages from the server.
    ///
    /// The server can send notices as well as notifications asynchronously to the client. Applications that wish to
    /// examine those messages should use this method to drive the connection rather than its `Future` implementation.
    ///
    /// Return values of `None` or `Some(Err(_))` are "terminal"; callers should not invoke this method again after
    /// receiving one of those values.
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

    fn poll_read(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        // Poll incoming data from postgres first.
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

                let response = self.responses.pop_front().unwrap_or_else(|| {
                    panic!("Response received from postgres without a matching request")
                });
                self.pending_responses.push_back(PendingResponse {
                    response: Some(response),
                    messages,
                    completed: response_complete,
                });
            }
        }

        // Then send the received data to the client of this connection.
        self.pending_responses.retain_mut(|pending| {
            let Some(response) = &mut pending.response else {
                return !pending.completed;
            };

            match response.sender.poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    // We don't care about the result of this send, because channel can be closed
                    // by client side while we're receiving the responses from postgres.
                    let _ = response.sender.start_send(mem::take(&mut pending.messages));
                    !pending.completed
                }
                Poll::Ready(Err(_)) => {
                    // We still need to fully receive the response from postgres.
                    pending.response = None;
                    !pending.completed
                }
                Poll::Pending => true,
            }
        });

        Ok(())
    }

    fn poll_write(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        if self.state != State::Closing {
            loop {
                let req = match self.receiver.poll_next_unpin(cx) {
                    Poll::Ready(Some(req)) => req.into(),
                    Poll::Ready(None)
                        if self.responses.is_empty()
                            && self.pending_responses.is_empty()
                            && self.state == State::Active =>
                    {
                        self.state = State::Terminating;
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
                        break;
                    }
                };

                self.pending_requests.push_back(req);
            }
        }

        loop {
            // First, flush previous messages.
            if self.flushing {
                match Pin::new(&mut self.stream)
                    .poll_flush(cx)
                    .map_err(Error::io)?
                {
                    Poll::Ready(()) => {
                        self.flushing = false;
                    }
                    Poll::Pending => {
                        // A previous message is not flushed, so we can't write a new one.
                        break;
                    }
                }
            }

            let Some(req) = self.pending_requests.pop_front() else {
                break;
            };

            // Socket is not ready yet, so we can't write a new message.
            if Pin::new(&mut self.stream)
                .poll_ready(cx)
                .map_err(Error::io)?
                .is_pending()
            {
                self.pending_requests.push_front(req);
                break;
            }

            match req.messages {
                RequestMessages::Single(msg) => {
                    Pin::new(&mut self.stream)
                        .start_send(msg)
                        .map_err(Error::io)?;
                    if self.state == State::Terminating {
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
                            break;
                        }
                    };

                    Pin::new(&mut self.stream)
                        .start_send(msg)
                        .map_err(Error::io)?;
                }
            }

            if let Some(sender) = req.sender {
                self.responses.push_back(Response { sender });
            }
            self.flushing = true;
        }

        Ok(())
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        if self.state != State::Closing {
            return Poll::Pending;
        }

        Pin::new(&mut self.stream).poll_close(cx).map_err(Error::io)
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
            info!("{}: {}", notice.severity(), notice.message());
        }

        Poll::Ready(Ok(()))
    }
}

struct PendingRequest {
    messages: RequestMessages,
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

struct PendingResponse {
    response: Option<Response>,
    messages: BackendMessages,
    completed: bool,
}

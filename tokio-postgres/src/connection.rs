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
use std::pin::Pin;
use std::task::{Context, Poll};
use std::{mem, panic};
use std::fmt::Formatter;
use std::panic::AssertUnwindSafe;
use tokio::io::{AsyncRead, AsyncWrite};
use tokio_util::codec::Framed;
use tracing::{info_span, instrument, Span};
use uuid_1::Uuid;

pub enum RequestMessages {
    Single(FrontendMessage),
    CopyIn(CopyInReceiver),
}

pub struct Request {
    pub messages: RequestMessages,
    pub sender: mpsc::Sender<BackendMessages>,
    pub span: Span,
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
    id: Uuid,
    stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
    parameters: HashMap<String, String>,
    receiver: mpsc::UnboundedReceiver<Request>,
    pending_requests: VecDeque<PendingRequest>,
    pending_responses: VecDeque<PendingResponse>,
    responses: VecDeque<Response>,
    messages: VecDeque<AsyncMessage>,
    flushing: bool,
    state: State,
    span: Span,
}

impl<S, T> Connection<S, T>
where
    S: AsyncRead + AsyncWrite + Unpin,
    T: AsyncRead + AsyncWrite + Unpin,
{
    #[instrument(name = "PgConn", skip_all)]
    pub(crate) fn new(
        stream: Framed<MaybeTlsStream<S, T>, PostgresCodec>,
        delayed_messages: VecDeque<Message>,
        parameters: HashMap<String, String>,
        receiver: mpsc::UnboundedReceiver<Request>,
    ) -> Result<Connection<S, T>, Error> {
        let id = Uuid::new_v4();
        let mut this = Connection {
            id,
            stream,
            parameters,
            receiver,
            pending_requests: VecDeque::new(),
            pending_responses: VecDeque::new(),
            responses: VecDeque::new(),
            messages: VecDeque::new(),
            flushing: false,
            state: State::Active,
            span: info_span!("PgConnection", id = id.to_string().as_str()),
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
            _ => unreachable!("unexpected message: {msg:?}"),
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
        let span = self.span.clone();
        let _guard = span.enter();

        info!("poll_message");

        self.poll_read(cx)?;
        self.poll_write(cx)?;
        let shutdown = self.poll_shutdown(cx)?;

        self.messages
            .pop_front()
            .map_or(shutdown.map(|_| None), |m| Poll::Ready(Some(Ok(m))))
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        info!("poll_read");

        // Poll incoming data from postgres first.
        if self.state == State::Active {
            loop {
                info!("poll_read: polling incoming data from postgres");
                let res = Pin::new(&mut self.stream)
                    .poll_next(cx)
                    .map(|o| o.map(|r| r.map_err(Error::io)))?;
                let (messages, response_complete) = match res {
                    Poll::Ready(Some(BackendMessage::Async(msg))) => {
                        info!("poll_read: received message from postgres");
                        self.handle_message(msg)?;
                        continue;
                    }
                    Poll::Ready(Some(BackendMessage::Normal {
                        messages,
                        request_complete,
                    })) => (messages, request_complete),
                    Poll::Ready(None) | Poll::Pending => {
                        info!("poll_read: no more data from postgres");
                        break;
                    }
                };

                info!("poll_read: received response from postgres");

                if let Some(pending) = self.pending_responses.back_mut() {
                    if !pending.completed {
                        info!("poll_read: appending last pending response");
                        pending.messages.extend(messages);
                        pending.completed = response_complete;
                        continue;
                    }
                }

                info!("poll_read: inserting a new pending response");
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

        info!("poll_read: tick pending responses");
        // Then send the received data to the client of this connection.
        self.pending_responses.retain_mut(|pending| {
            let Some(response) = &mut pending.response else {
                info!(
                    "poll_read: response has no receiver, completed: {}",
                    pending.completed
                );
                return !pending.completed;
            };

            info!("poll_read: sending response from postgres");
            match response.sender.poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    info!(
                        "poll_read: response ready to be send, completed: {}",
                        pending.completed
                    );
                    // We don't care about the result of this send, because channel can be closed
                    // by client side while we're receiving the responses from postgres.
                    let _ = response.sender.start_send(mem::take(&mut pending.messages));
                    !pending.completed
                }
                Poll::Ready(Err(_)) => {
                    info!(
                        "poll_read: response receiver is dropped, completed: {}",
                        pending.completed
                    );
                    // We still need to fully receive the response from postgres.
                    pending.response = None;
                    !pending.completed
                }
                Poll::Pending => {
                    info!("poll_read: waiting for response receiver to be ready");
                    true
                }
            }
        });

        Ok(())
    }

    fn poll_write(&mut self, cx: &mut Context<'_>) -> Result<(), Error> {
        info!("poll_write");

        if self.state != State::Closing {
            loop {
                info!("poll_write: receiving request from client");
                let req = match self.receiver.poll_next_unpin(cx) {
                    Poll::Ready(Some(req)) => {
                        req.span.in_scope(|| {
                            info!(
                                "poll_write: request received by `Connection(id: {})`",
                                self.id
                            )
                        });
                        req.into()
                    }
                    Poll::Ready(None)
                        if self.responses.is_empty()
                            && self.pending_responses.is_empty()
                            && self.state == State::Active =>
                    {
                        info!("poll_write: client is dropped, terminating connection");
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
                        info!("poll_write: no more requests from client");
                        break;
                    }
                };

                info!("poll_write: new request from client");
                self.pending_requests.push_back(req);
            }
        }

        loop {
            info!("poll_write: tick writing");
            // First, flush previous messages.
            if self.flushing {
                info!("poll_write: waiting for previous messages to be flushed");
                match Pin::new(&mut self.stream)
                    .poll_flush(cx)
                    .map_err(Error::io)?
                {
                    Poll::Ready(()) => {
                        info!("poll_write: previous messages are flushed");
                        self.flushing = false;
                    }
                    Poll::Pending => {
                        info!("poll_write: previous messages are not flushed yet");
                        // A previous message is not flushed, so we can't write a new one.
                        break;
                    }
                }
            }

            let Some(req) = self.pending_requests.pop_front() else {
                info!("poll_write: no more pending requests");
                break;
            };

            info!("poll_write: waiting for socket to be ready");
            // Socket is not ready yet, so we can't write a new message.
            if Pin::new(&mut self.stream)
                .poll_ready(cx)
                .map_err(Error::io)?
                .is_pending()
            {
                info!("poll_write: socket is not ready yet");
                self.pending_requests.push_front(req);
                break;
            }

            match req.messages {
                RequestMessages::Single(msg) => {
                    info!("poll_write: sending a single message");
                    Pin::new(&mut self.stream)
                        .start_send(msg)
                        .map_err(Error::io)?;
                    if self.state == State::Terminating {
                        info!("poll_write: EOF request was sent, closing connection");
                        self.state = State::Closing;
                    }
                }
                RequestMessages::CopyIn(mut receiver) => {
                    info!("poll_write: sending copy data");
                    let msg = match receiver.poll_next_unpin(cx) {
                        Poll::Ready(Some(msg)) => msg,
                        Poll::Ready(None) => {
                            info!("poll_write: copy data source is dropped");
                            // No receiver found, move to the next message.
                            continue;
                        }
                        Poll::Pending => {
                            info!("poll_write: copy data is not ready yet");
                            // Message not ready yet, wait for it.
                            self.pending_requests.push_front(PendingRequest {
                                messages: RequestMessages::CopyIn(receiver),
                                sender: req.sender,
                            });
                            break;
                        }
                    };
                    info!("poll_write: copy data source is ready, sending");
                    Pin::new(&mut self.stream)
                        .start_send(msg)
                        .map_err(Error::io)?;
                }
            }

            if let Some(sender) = req.sender {
                info!("poll_write: inserting a new response");
                self.responses.push_back(Response { sender });
            }
            self.flushing = true;
        }

        Ok(())
    }

    fn poll_shutdown(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Error>> {
        info!("poll_shutdown");
        if self.state != State::Closing {
            info!("poll_shutdown: not closing");
            return Poll::Pending;
        }

        info!("poll_shutdown: waiting for socket to be ready");
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
        let res = panic::catch_unwind(AssertUnwindSafe(|| {
            while let Some(AsyncMessage::Notice(notice)) = ready!(self.poll_message(cx)?) {
                info!("{}: {}", notice.severity(), notice.message());
            }

            Poll::Ready(Ok(()))
        }));

        match res {
            Ok(res) => res,
            Err(err) => {
                let err = err.downcast::<String>().map_or_else(|_| {
                    String::from("unknown panic occurred in `Connection`")
                }, |s| *s);
                println!("`Connection` errored: {err}");
                Poll::Ready(Err(Error::config(Box::new(CustomErr(err)))))
            }
        }
    }
}

#[derive(Debug)]
struct CustomErr(String);

impl std::fmt::Display for CustomErr {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        self.0.fmt(f)
    }
}

impl std::error::Error for CustomErr {}

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

use futures::sync::mpsc;
use futures::{try_ready, Async, AsyncSink, Future, Poll, Sink, Stream};
use log::trace;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::{HashMap, VecDeque};
use std::io;
use tokio_codec::Framed;
use tokio_io::{AsyncRead, AsyncWrite};

use crate::proto::codec::PostgresCodec;
use crate::proto::copy_in::CopyInReceiver;
use crate::proto::idle::IdleGuard;
use crate::{AsyncMessage, Notification};
use crate::{DbError, Error};

pub enum RequestMessages {
    Single(Vec<u8>),
    CopyIn {
        receiver: CopyInReceiver,
        pending_message: Option<Vec<u8>>,
    },
}

pub struct Request {
    pub messages: RequestMessages,
    pub sender: mpsc::Sender<Message>,
    pub idle: Option<IdleGuard>,
}

struct Response {
    sender: mpsc::Sender<Message>,
    _idle: Option<IdleGuard>,
}

#[derive(PartialEq, Debug)]
enum State {
    Active,
    Terminating,
    Closing,
}

pub struct Connection<S> {
    stream: Framed<S, PostgresCodec>,
    parameters: HashMap<String, String>,
    receiver: mpsc::UnboundedReceiver<Request>,
    pending_request: Option<RequestMessages>,
    pending_response: Option<Message>,
    responses: VecDeque<Response>,
    state: State,
}

impl<S> Connection<S>
where
    S: AsyncRead + AsyncWrite,
{
    pub fn new(
        stream: Framed<S, PostgresCodec>,
        parameters: HashMap<String, String>,
        receiver: mpsc::UnboundedReceiver<Request>,
    ) -> Connection<S> {
        Connection {
            stream,
            parameters,
            receiver,
            pending_request: None,
            pending_response: None,
            responses: VecDeque::new(),
            state: State::Active,
        }
    }

    pub fn parameter(&self, name: &str) -> Option<&str> {
        self.parameters.get(name).map(|s| &**s)
    }

    fn poll_response(&mut self) -> Poll<Option<Message>, io::Error> {
        if let Some(message) = self.pending_response.take() {
            trace!("retrying pending response");
            return Ok(Async::Ready(Some(message)));
        }

        self.stream.poll()
    }

    fn poll_read(&mut self) -> Result<Option<AsyncMessage>, Error> {
        if self.state != State::Active {
            trace!("poll_read: done");
            return Ok(None);
        }

        loop {
            let message = match self.poll_response().map_err(Error::io)? {
                Async::Ready(Some(message)) => message,
                Async::Ready(None) => {
                    return Err(Error::closed());
                }
                Async::NotReady => {
                    trace!("poll_read: waiting on response");
                    return Ok(None);
                }
            };

            let message = match message {
                Message::NoticeResponse(body) => {
                    let error = DbError::parse(&mut body.fields()).map_err(Error::parse)?;
                    return Ok(Some(AsyncMessage::Notice(error)));
                }
                Message::NotificationResponse(body) => {
                    let notification = Notification {
                        process_id: body.process_id(),
                        channel: body.channel().map_err(Error::parse)?.to_string(),
                        payload: body.message().map_err(Error::parse)?.to_string(),
                    };
                    return Ok(Some(AsyncMessage::Notification(notification)));
                }
                Message::ParameterStatus(body) => {
                    self.parameters.insert(
                        body.name().map_err(Error::parse)?.to_string(),
                        body.value().map_err(Error::parse)?.to_string(),
                    );
                    continue;
                }
                m => m,
            };

            let mut response = match self.responses.pop_front() {
                Some(response) => response,
                None => match message {
                    Message::ErrorResponse(error) => return Err(Error::db(error)),
                    _ => return Err(Error::unexpected_message()),
                },
            };

            let request_complete = match message {
                Message::ReadyForQuery(_) => true,
                _ => false,
            };

            match response.sender.start_send(message) {
                // if the receiver's hung up we still need to page through the rest of the messages
                // designated to it
                Ok(AsyncSink::Ready) | Err(_) => {
                    if !request_complete {
                        self.responses.push_front(response);
                    }
                }
                Ok(AsyncSink::NotReady(message)) => {
                    self.responses.push_front(response);
                    self.pending_response = Some(message);
                    trace!("poll_read: waiting on sender");
                    return Ok(None);
                }
            }
        }
    }

    fn poll_request(&mut self) -> Poll<Option<RequestMessages>, Error> {
        if let Some(message) = self.pending_request.take() {
            trace!("retrying pending request");
            return Ok(Async::Ready(Some(message)));
        }

        match try_ready_receive!(self.receiver.poll()) {
            Some(request) => {
                trace!("polled new request");
                self.responses.push_back(Response {
                    sender: request.sender,
                    _idle: request.idle,
                });
                Ok(Async::Ready(Some(request.messages)))
            }
            None => Ok(Async::Ready(None)),
        }
    }

    fn poll_write(&mut self) -> Result<bool, Error> {
        loop {
            if self.state == State::Closing {
                trace!("poll_write: done");
                return Ok(false);
            }

            let request = match self.poll_request()? {
                Async::Ready(Some(request)) => request,
                Async::Ready(None) if self.responses.is_empty() && self.state == State::Active => {
                    trace!("poll_write: at eof, terminating");
                    self.state = State::Terminating;
                    let mut request = vec![];
                    frontend::terminate(&mut request);
                    RequestMessages::Single(request)
                }
                Async::Ready(None) => {
                    trace!(
                        "poll_write: at eof, pending responses {}",
                        self.responses.len(),
                    );
                    return Ok(true);
                }
                Async::NotReady => {
                    trace!("poll_write: waiting on request");
                    return Ok(true);
                }
            };

            match request {
                RequestMessages::Single(request) => {
                    match self.stream.start_send(request).map_err(Error::io)? {
                        AsyncSink::Ready => {
                            if self.state == State::Terminating {
                                trace!("poll_write: sent eof, closing");
                                self.state = State::Closing;
                            }
                        }
                        AsyncSink::NotReady(request) => {
                            trace!("poll_write: waiting on socket");
                            self.pending_request = Some(RequestMessages::Single(request));
                            return Ok(false);
                        }
                    }
                }
                RequestMessages::CopyIn {
                    mut receiver,
                    mut pending_message,
                } => {
                    let message = match pending_message.take() {
                        Some(message) => message,
                        None => match receiver.poll() {
                            Ok(Async::Ready(Some(message))) => message,
                            Ok(Async::Ready(None)) => {
                                trace!("poll_write: finished copy_in request");
                                continue;
                            }
                            Ok(Async::NotReady) => {
                                trace!("poll_write: waiting on copy_in stream");
                                self.pending_request = Some(RequestMessages::CopyIn {
                                    receiver,
                                    pending_message,
                                });
                                return Ok(true);
                            }
                            Err(()) => unreachable!("mpsc::Receiver doesn't return errors"),
                        },
                    };

                    match self.stream.start_send(message).map_err(Error::io)? {
                        AsyncSink::Ready => {
                            self.pending_request = Some(RequestMessages::CopyIn {
                                receiver,
                                pending_message: None,
                            });
                        }
                        AsyncSink::NotReady(message) => {
                            trace!("poll_write: waiting on socket");
                            self.pending_request = Some(RequestMessages::CopyIn {
                                receiver,
                                pending_message: Some(message),
                            });
                            return Ok(false);
                        }
                    };
                }
            }
        }
    }

    fn poll_flush(&mut self) -> Result<(), Error> {
        match self.stream.poll_complete().map_err(Error::io)? {
            Async::Ready(()) => trace!("poll_flush: flushed"),
            Async::NotReady => trace!("poll_flush: waiting on socket"),
        }
        Ok(())
    }

    fn poll_shutdown(&mut self) -> Poll<(), Error> {
        if self.state != State::Closing {
            return Ok(Async::NotReady);
        }

        match self.stream.close().map_err(Error::io)? {
            Async::Ready(()) => {
                trace!("poll_shutdown: complete");
                Ok(Async::Ready(()))
            }
            Async::NotReady => {
                trace!("poll_shutdown: waiting on socket");
                Ok(Async::NotReady)
            }
        }
    }

    pub fn poll_message(&mut self) -> Poll<Option<AsyncMessage>, Error> {
        let message = self.poll_read()?;
        let want_flush = self.poll_write()?;
        if want_flush {
            self.poll_flush()?;
        }
        match message {
            Some(message) => Ok(Async::Ready(Some(message))),
            None => self.poll_shutdown().map(|r| r.map(|()| None)),
        }
    }
}

impl<S> Future for Connection<S>
where
    S: AsyncRead + AsyncWrite,
{
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        while let Some(_) = try_ready!(self.poll_message()) {}
        Ok(Async::Ready(()))
    }
}

use futures::sync::mpsc;
use futures::{Async, AsyncSink, Future, Poll, Sink, Stream};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::{HashMap, VecDeque};
use std::io;
use tokio_codec::Framed;

use error::{self, DbError, Error};
use proto::codec::PostgresCodec;
use tls::TlsStream;
use {bad_response, disconnected, AsyncMessage, CancelData, Notification};

pub struct Request {
    pub messages: Vec<u8>,
    pub sender: mpsc::Sender<Message>,
}

#[derive(PartialEq, Debug)]
enum State {
    Active,
    Terminating,
    Closing,
}

pub struct Connection {
    stream: Framed<Box<TlsStream>, PostgresCodec>,
    cancel_data: CancelData,
    parameters: HashMap<String, String>,
    receiver: mpsc::UnboundedReceiver<Request>,
    pending_request: Option<Vec<u8>>,
    pending_response: Option<Message>,
    responses: VecDeque<mpsc::Sender<Message>>,
    state: State,
}

impl Connection {
    pub fn new(
        stream: Framed<Box<TlsStream>, PostgresCodec>,
        cancel_data: CancelData,
        parameters: HashMap<String, String>,
        receiver: mpsc::UnboundedReceiver<Request>,
    ) -> Connection {
        Connection {
            stream,
            cancel_data,
            parameters,
            receiver,
            pending_request: None,
            pending_response: None,
            responses: VecDeque::new(),
            state: State::Active,
        }
    }

    pub fn cancel_data(&self) -> CancelData {
        self.cancel_data
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
            let message = match self.poll_response()? {
                Async::Ready(Some(message)) => message,
                Async::Ready(None) => {
                    return Err(disconnected());
                }
                Async::NotReady => {
                    trace!("poll_read: waiting on response");
                    return Ok(None);
                }
            };

            let message = match message {
                Message::NoticeResponse(body) => {
                    let error = DbError::new(&mut body.fields())?;
                    return Ok(Some(AsyncMessage::Notice(error)));
                }
                Message::NotificationResponse(body) => {
                    let notification = Notification {
                        process_id: body.process_id(),
                        channel: body.channel()?.to_string(),
                        payload: body.message()?.to_string(),
                    };
                    return Ok(Some(AsyncMessage::Notification(notification)));
                }
                Message::ParameterStatus(body) => {
                    self.parameters
                        .insert(body.name()?.to_string(), body.value()?.to_string());
                    continue;
                }
                m => m,
            };

            let mut sender = match self.responses.pop_front() {
                Some(sender) => sender,
                None => match message {
                    Message::ErrorResponse(error) => return Err(error::__db(error)),
                    _ => return Err(bad_response()),
                },
            };

            let request_complete = match message {
                Message::ReadyForQuery(_) => true,
                _ => false,
            };

            match sender.start_send(message) {
                // if the receiver's hung up we still need to page through the rest of the messages
                // designated to it
                Ok(AsyncSink::Ready) | Err(_) => {
                    if !request_complete {
                        self.responses.push_front(sender);
                    }
                }
                Ok(AsyncSink::NotReady(message)) => {
                    self.responses.push_front(sender);
                    self.pending_response = Some(message);
                    trace!("poll_read: waiting on sender");
                    return Ok(None);
                }
            }
        }
    }

    fn poll_request(&mut self) -> Poll<Option<Vec<u8>>, Error> {
        if let Some(message) = self.pending_request.take() {
            trace!("retrying pending request");
            return Ok(Async::Ready(Some(message)));
        }

        match try_receive!(self.receiver.poll()) {
            Some(request) => {
                trace!("polled new request");
                self.responses.push_back(request.sender);
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
                    request
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

            match self.stream.start_send(request)? {
                AsyncSink::Ready => {
                    if self.state == State::Terminating {
                        trace!("poll_write: sent eof, closing");
                        self.state = State::Closing;
                    }
                }
                AsyncSink::NotReady(request) => {
                    trace!("poll_write: waiting on socket");
                    self.pending_request = Some(request);
                    return Ok(false);
                }
            }
        }
    }

    fn poll_flush(&mut self) -> Result<(), Error> {
        match self.stream.poll_complete() {
            Ok(Async::Ready(())) => {
                trace!("poll_flush: flushed");
                Ok(())
            }
            Ok(Async::NotReady) => {
                trace!("poll_flush: waiting on socket");
                Ok(())
            }
            Err(e) => Err(Error::from(e)),
        }
    }

    fn poll_shutdown(&mut self) -> Poll<(), Error> {
        if self.state != State::Closing {
            return Ok(Async::NotReady);
        }

        match self.stream.close() {
            Ok(Async::Ready(())) => {
                trace!("poll_shutdown: complete");
                Ok(Async::Ready(()))
            }
            Ok(Async::NotReady) => {
                trace!("poll_shutdown: waiting on socket");
                Ok(Async::NotReady)
            }
            Err(e) => Err(Error::from(e)),
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

impl Future for Connection {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        while let Some(_) = try_ready!(self.poll_message()) {}
        Ok(Async::Ready(()))
    }
}

use futures::sync::mpsc;
use futures::{Async, AsyncSink, Future, Poll, Sink, Stream};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::{HashMap, VecDeque};
use std::io;
use tokio_codec::Framed;
use want::Taker;

use disconnected;
use error::{self, Error};
use proto::codec::PostgresCodec;
use proto::socket::Socket;
use {bad_response, CancelData};

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
    stream: Framed<Socket, PostgresCodec>,
    cancel_data: CancelData,
    parameters: HashMap<String, String>,
    receiver: mpsc::UnboundedReceiver<Request>,
    taker: Taker,
    pending_request: Option<Vec<u8>>,
    pending_response: Option<Message>,
    responses: VecDeque<mpsc::Sender<Message>>,
    state: State,
}

impl Connection {
    pub fn new(
        stream: Framed<Socket, PostgresCodec>,
        cancel_data: CancelData,
        parameters: HashMap<String, String>,
        receiver: mpsc::UnboundedReceiver<Request>,
        taker: Taker,
    ) -> Connection {
        Connection {
            stream,
            cancel_data,
            parameters,
            receiver,
            taker,
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

    fn poll_read(&mut self) -> Poll<(), Error> {
        if self.state != State::Active {
            trace!("poll_read: done");
            return Ok(Async::Ready(()));
        }

        loop {
            let message = match self.poll_response()? {
                Async::Ready(Some(message)) => message,
                Async::Ready(None) => {
                    return Err(disconnected());
                }
                Async::NotReady => {
                    trace!("poll_read: waiting on response");
                    return Ok(Async::NotReady);
                }
            };

            let message = match message {
                Message::NoticeResponse(_) | Message::NotificationResponse(_) => {
                    // FIXME handle these
                    continue;
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
                    trace!("poll_read: waiting on socket");
                    return Ok(Async::NotReady);
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

    fn poll_write(&mut self) -> Poll<(), Error> {
        loop {
            if self.state == State::Closing {
                trace!("poll_write: done");
                return Ok(Async::Ready(()));
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
                    return Ok(Async::Ready(()));
                }
                Async::NotReady => {
                    trace!("poll_write: waiting on request");
                    self.taker.want();
                    return Ok(Async::NotReady);
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
                    return Ok(Async::NotReady);
                }
            }
        }
    }

    fn poll_flush(&mut self) -> Poll<(), Error> {
        trace!("flushing");
        self.stream.poll_complete().map_err(Into::into)
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
}

impl Future for Connection {
    type Item = ();
    type Error = Error;

    fn poll(&mut self) -> Poll<(), Error> {
        self.poll_read()?;
        self.poll_write()?;
        self.poll_flush()?;
        self.poll_shutdown()
    }
}

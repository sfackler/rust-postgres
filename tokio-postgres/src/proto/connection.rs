use futures::sync::mpsc;
use futures::{Async, AsyncSink, Future, Poll, Sink, Stream};
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::{HashMap, VecDeque};
use std::io;
use tokio_codec::Framed;

use error::{self, Error};
use proto::codec::PostgresCodec;
use proto::socket::Socket;
use {bad_response, CancelData};

pub struct Request {
    pub messages: Vec<u8>,
    pub sender: mpsc::Sender<Message>,
}

#[derive(PartialEq)]
enum State {
    Active,
    Terminating,
    Closing,
}

pub struct Connection {
    stream: Framed<Socket, PostgresCodec>,
    cancel_data: CancelData,
    parameters: HashMap<String, String>,
    receiver: mpsc::Receiver<Request>,
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
        receiver: mpsc::Receiver<Request>,
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
            return Ok(Async::Ready(Some(message)));
        }

        self.stream.poll()
    }

    fn poll_read(&mut self) -> Result<(), Error> {
        loop {
            let message = match self.poll_response()? {
                Async::Ready(Some(message)) => message,
                Async::Ready(None) => {
                    return Err(Error::from(io::Error::from(io::ErrorKind::UnexpectedEof)));
                }
                Async::NotReady => return Ok(()),
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

            let ready = match message {
                Message::ReadyForQuery(_) => true,
                _ => false,
            };

            match sender.start_send(message) {
                // if the receiver's hung up we still need to page through the rest of the messages
                // designated to it
                Ok(AsyncSink::Ready) | Err(_) => {
                    if !ready {
                        self.responses.push_front(sender);
                    }
                }
                Ok(AsyncSink::NotReady(message)) => {
                    self.responses.push_front(sender);
                    self.pending_response = Some(message);
                    return Ok(());
                }
            }
        }
    }

    fn poll_request(&mut self) -> Poll<Option<Vec<u8>>, Error> {
        if let Some(message) = self.pending_request.take() {
            return Ok(Async::Ready(Some(message)));
        }

        match self.receiver.poll() {
            Ok(Async::Ready(Some(request))) => {
                self.responses.push_back(request.sender);
                Ok(Async::Ready(Some(request.messages)))
            }
            Ok(Async::Ready(None)) => Ok(Async::Ready(None)),
            Ok(Async::NotReady) => Ok(Async::NotReady),
            Err(()) => unreachable!("mpsc::Receiver doesn't return errors"),
        }
    }

    fn poll_write(&mut self) -> Result<(), Error> {
        loop {
            let request = match self.poll_request()? {
                Async::Ready(Some(request)) => request,
                Async::Ready(None) if self.responses.is_empty() && self.state == State::Active => {
                    self.state = State::Terminating;
                    let mut request = vec![];
                    frontend::terminate(&mut request);
                    request
                }
                Async::Ready(None) => return Ok(()),
                Async::NotReady => return Ok(()),
            };

            match self.stream.start_send(request)? {
                AsyncSink::Ready => {
                    if self.state == State::Terminating {
                        self.state = State::Closing;
                    }
                }
                AsyncSink::NotReady(request) => {
                    self.pending_request = Some(request);
                    return Ok(());
                }
            }
        }
    }

    fn poll_flush(&mut self) -> Result<(), Error> {
        self.stream.poll_complete()?;
        Ok(())
    }

    fn poll_shutdown(&mut self) -> Poll<(), Error> {
        match self.state {
            State::Active | State::Terminating => Ok(Async::NotReady),
            State::Closing => self.stream.close().map_err(Into::into),
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

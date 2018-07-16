use bytes::Bytes;
use futures::sync::mpsc;
use futures::{Async, Poll, Stream};
use postgres_protocol::message::backend::Message;
use std::mem;

use error::{self, Error};
use proto::client::{Client, PendingRequest};
use proto::statement::Statement;
use {bad_response, disconnected};

enum State {
    Start {
        client: Client,
        request: PendingRequest,
        statement: Statement,
    },
    ReadingCopyOutResponse {
        receiver: mpsc::Receiver<Message>,
    },
    ReadingCopyData {
        receiver: mpsc::Receiver<Message>,
    },
    Done,
}

pub struct CopyOutStream(State);

impl Stream for CopyOutStream {
    type Item = Bytes;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Bytes>, Error> {
        loop {
            match mem::replace(&mut self.0, State::Done) {
                State::Start {
                    client,
                    request,
                    statement,
                } => {
                    let receiver = client.send(request)?;
                    // it's ok for the statement to close now that we've queued the query
                    drop(statement);
                    self.0 = State::ReadingCopyOutResponse { receiver };
                }
                State::ReadingCopyOutResponse { mut receiver } => {
                    let message = match receiver.poll() {
                        Ok(Async::Ready(message)) => message,
                        Ok(Async::NotReady) => {
                            self.0 = State::ReadingCopyOutResponse { receiver };
                            break Ok(Async::NotReady);
                        }
                        Err(()) => unreachable!("mpsc::Receiver doesn't return errors"),
                    };

                    match message {
                        Some(Message::BindComplete) => {
                            self.0 = State::ReadingCopyOutResponse { receiver };
                        }
                        Some(Message::CopyOutResponse(_)) => {
                            self.0 = State::ReadingCopyData { receiver };
                        }
                        Some(Message::ErrorResponse(body)) => break Err(error::__db(body)),
                        Some(_) => break Err(bad_response()),
                        None => break Err(disconnected()),
                    }
                }
                State::ReadingCopyData { mut receiver } => {
                    let message = match receiver.poll() {
                        Ok(Async::Ready(message)) => message,
                        Ok(Async::NotReady) => {
                            self.0 = State::ReadingCopyData { receiver };
                            break Ok(Async::NotReady);
                        }
                        Err(()) => unreachable!("mpsc::Reciever doesn't return errors"),
                    };

                    match message {
                        Some(Message::CopyData(body)) => {
                            self.0 = State::ReadingCopyData { receiver };
                            break Ok(Async::Ready(Some(body.into_bytes())));
                        }
                        Some(Message::CopyDone) | Some(Message::CommandComplete(_)) => {
                            self.0 = State::ReadingCopyData { receiver };
                        }
                        Some(Message::ReadyForQuery(_)) => break Ok(Async::Ready(None)),
                        Some(Message::ErrorResponse(body)) => break Err(error::__db(body)),
                        Some(_) => break Err(bad_response()),
                        None => break Err(disconnected()),
                    }
                }
                State::Done => break Ok(Async::Ready(None)),
            }
        }
    }
}

impl CopyOutStream {
    pub fn new(client: Client, request: PendingRequest, statement: Statement) -> CopyOutStream {
        CopyOutStream(State::Start {
            client,
            request,
            statement,
        })
    }
}

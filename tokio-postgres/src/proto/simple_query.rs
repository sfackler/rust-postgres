use futures::sync::mpsc;
use futures::{Async, Poll, Stream};
use postgres_protocol::message::backend::{DataRowBody, Message};
use std::mem;

use crate::proto::client::{Client, PendingRequest};
use crate::Error;

pub enum State {
    Start {
        client: Client,
        request: PendingRequest,
    },
    ReadResponse {
        receiver: mpsc::Receiver<Message>,
    },
    Done,
}

pub struct SimpleQueryStream(State);

impl Stream for SimpleQueryStream {
    type Item = DataRowBody;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<DataRowBody>, Error> {
        loop {
            match mem::replace(&mut self.0, State::Done) {
                State::Start { client, request } => {
                    let receiver = client.send(request)?;
                    self.0 = State::ReadResponse { receiver };
                }
                State::ReadResponse { mut receiver } => {
                    let message = match receiver.poll() {
                        Ok(Async::Ready(message)) => message,
                        Ok(Async::NotReady) => {
                            self.0 = State::ReadResponse { receiver };
                            return Ok(Async::NotReady);
                        }
                        Err(()) => unreachable!("mpsc receiver can't panic"),
                    };

                    match message {
                        Some(Message::CommandComplete(_))
                        | Some(Message::RowDescription(_))
                        | Some(Message::EmptyQueryResponse) => {
                            self.0 = State::ReadResponse { receiver };
                        }
                        Some(Message::DataRow(body)) => {
                            self.0 = State::ReadResponse { receiver };
                            return Ok(Async::Ready(Some(body)));
                        }
                        Some(Message::ErrorResponse(body)) => return Err(Error::db(body)),
                        Some(Message::ReadyForQuery(_)) => return Ok(Async::Ready(None)),
                        Some(_) => return Err(Error::unexpected_message()),
                        None => return Err(Error::closed()),
                    }
                }
                State::Done => return Ok(Async::Ready(None)),
            }
        }
    }
}

impl SimpleQueryStream {
    pub fn new(client: Client, request: PendingRequest) -> SimpleQueryStream {
        SimpleQueryStream(State::Start { client, request })
    }
}

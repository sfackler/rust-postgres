use fallible_iterator::FallibleIterator;
use futures::sync::mpsc;
use futures::{Async, Poll, Stream};
use postgres_protocol::message::backend::Message;
use std::mem;
use std::sync::Arc;

use crate::proto::client::{Client, PendingRequest};
use crate::{Error, SimpleQueryMessage, SimpleQueryRow};

pub enum State {
    Start {
        client: Client,
        request: PendingRequest,
    },
    ReadResponse {
        columns: Option<Arc<[String]>>,
        receiver: mpsc::Receiver<Message>,
    },
    Done,
}

pub struct SimpleQueryStream(State);

impl Stream for SimpleQueryStream {
    type Item = SimpleQueryMessage;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<SimpleQueryMessage>, Error> {
        loop {
            match mem::replace(&mut self.0, State::Done) {
                State::Start { client, request } => {
                    let receiver = client.send(request)?;
                    self.0 = State::ReadResponse {
                        columns: None,
                        receiver,
                    };
                }
                State::ReadResponse {
                    columns,
                    mut receiver,
                } => {
                    let message = match receiver.poll() {
                        Ok(Async::Ready(message)) => message,
                        Ok(Async::NotReady) => {
                            self.0 = State::ReadResponse { columns, receiver };
                            return Ok(Async::NotReady);
                        }
                        Err(()) => unreachable!("mpsc receiver can't panic"),
                    };

                    match message {
                        Some(Message::CommandComplete(body)) => {
                            let rows = body
                                .tag()
                                .map_err(Error::parse)?
                                .rsplit(' ')
                                .next()
                                .unwrap()
                                .parse()
                                .unwrap_or(0);
                            self.0 = State::ReadResponse {
                                columns: None,
                                receiver,
                            };
                            return Ok(Async::Ready(Some(SimpleQueryMessage::CommandComplete(
                                rows,
                            ))));
                        }
                        Some(Message::EmptyQueryResponse) => {
                            self.0 = State::ReadResponse {
                                columns: None,
                                receiver,
                            };
                            return Ok(Async::Ready(Some(SimpleQueryMessage::CommandComplete(0))));
                        }
                        Some(Message::RowDescription(body)) => {
                            let columns = body
                                .fields()
                                .map(|f| Ok(f.name().to_string()))
                                .collect::<Vec<_>>()
                                .map_err(Error::parse)?
                                .into();
                            self.0 = State::ReadResponse {
                                columns: Some(columns),
                                receiver,
                            };
                        }
                        Some(Message::DataRow(body)) => {
                            let row = match &columns {
                                Some(columns) => SimpleQueryRow::new(columns.clone(), body)?,
                                None => return Err(Error::unexpected_message()),
                            };
                            self.0 = State::ReadResponse { columns, receiver };
                            return Ok(Async::Ready(Some(SimpleQueryMessage::Row(row))));
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

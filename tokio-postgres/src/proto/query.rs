use futures::sync::mpsc;
use futures::{Async, Poll, Stream};
use postgres_protocol::message::backend::Message;
use std::mem;

use proto::client::{Client, PendingRequest};
use proto::row::Row;
use proto::statement::Statement;
use Error;

enum State {
    Start {
        client: Client,
        request: PendingRequest,
        statement: Statement,
    },
    ReadingResponse {
        receiver: mpsc::Receiver<Message>,
        statement: Statement,
    },
    ReadingReadyForQuery {
        receiver: mpsc::Receiver<Message>,
    },
    Done,
}

pub struct QueryStream(State);

impl Stream for QueryStream {
    type Item = Row;
    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Row>, Error> {
        loop {
            match mem::replace(&mut self.0, State::Done) {
                State::Start {
                    client,
                    request,
                    statement,
                } => {
                    let receiver = client.send(request)?;
                    self.0 = State::ReadingResponse {
                        receiver,
                        statement,
                    };
                }
                State::ReadingResponse {
                    mut receiver,
                    statement,
                } => {
                    let message = match receiver.poll() {
                        Ok(Async::Ready(message)) => message,
                        Ok(Async::NotReady) => {
                            self.0 = State::ReadingResponse {
                                receiver,
                                statement,
                            };
                            break Ok(Async::NotReady);
                        }
                        Err(()) => unreachable!("mpsc::Receiver doesn't return errors"),
                    };

                    match message {
                        Some(Message::BindComplete) => {
                            self.0 = State::ReadingResponse {
                                receiver,
                                statement,
                            };
                        }
                        Some(Message::ErrorResponse(body)) => break Err(Error::db(body)),
                        Some(Message::DataRow(body)) => {
                            let row = Row::new(statement.clone(), body)?;
                            self.0 = State::ReadingResponse {
                                receiver,
                                statement,
                            };
                            break Ok(Async::Ready(Some(row)));
                        }
                        Some(Message::EmptyQueryResponse) | Some(Message::CommandComplete(_)) => {
                            self.0 = State::ReadingReadyForQuery { receiver };
                        }
                        Some(_) => break Err(Error::unexpected_message()),
                        None => break Err(Error::closed()),
                    }
                }
                State::ReadingReadyForQuery { mut receiver } => {
                    let message = match receiver.poll() {
                        Ok(Async::Ready(message)) => message,
                        Ok(Async::NotReady) => {
                            self.0 = State::ReadingReadyForQuery { receiver };
                            break Ok(Async::NotReady);
                        }
                        Err(()) => unreachable!("mpsc::Receiver doesn't return errors"),
                    };

                    match message {
                        Some(Message::ReadyForQuery(_)) => break Ok(Async::Ready(None)),
                        Some(_) => break Err(Error::unexpected_message()),
                        None => break Err(Error::closed()),
                    }
                }
                State::Done => break Ok(Async::Ready(None)),
            }
        }
    }
}

impl QueryStream {
    pub fn new(client: Client, request: PendingRequest, statement: Statement) -> QueryStream {
        QueryStream(State::Start {
            client,
            request,
            statement,
        })
    }
}

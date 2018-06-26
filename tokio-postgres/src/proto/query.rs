use futures::sync::mpsc;
use futures::{Async, Poll, Stream};
use postgres_protocol::message::backend::Message;
use std::mem;

use error::{self, Error};
use proto::client::PendingRequest;
use proto::row::Row;
use proto::statement::Statement;
use {bad_response, disconnected};

enum State {
    Start {
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
                State::Start { request, statement } => {
                    let receiver = request.send()?;
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
                        Some(Message::ErrorResponse(body)) => break Err(error::__db(body)),
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
                        Some(_) => break Err(bad_response()),
                        None => break Err(disconnected()),
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
                        Some(_) => break Err(bad_response()),
                        None => break Err(disconnected()),
                    }
                }
                State::Done => break Ok(Async::Ready(None)),
            }
        }
    }
}

impl QueryStream {
    pub fn new(request: PendingRequest, statement: Statement) -> QueryStream {
        QueryStream(State::Start { request, statement })
    }
}

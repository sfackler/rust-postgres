use futures::sync::mpsc;
use futures::{Async, Poll, Stream};
use postgres_protocol::message::backend::Message;
use std::mem;

use crate::proto::client::{Client, PendingRequest};
use crate::proto::portal::Portal;
use crate::proto::row::Row;
use crate::proto::statement::Statement;
use crate::Error;

pub trait StatementHolder {
    fn statement(&self) -> &Statement;
}

impl StatementHolder for Statement {
    fn statement(&self) -> &Statement {
        self
    }
}

impl StatementHolder for Portal {
    fn statement(&self) -> &Statement {
        self.statement()
    }
}

enum State<T> {
    Start {
        client: Client,
        request: PendingRequest,
        statement: T,
    },
    ReadingResponse {
        receiver: mpsc::Receiver<Message>,
        statement: T,
    },
    Done,
}

pub struct QueryStream<T>(State<T>);

impl<T> Stream for QueryStream<T>
where
    T: StatementHolder,
{
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
                            let row = Row::new(statement.statement().clone(), body)?;
                            self.0 = State::ReadingResponse {
                                receiver,
                                statement,
                            };
                            break Ok(Async::Ready(Some(row)));
                        }
                        Some(Message::EmptyQueryResponse)
                        | Some(Message::PortalSuspended)
                        | Some(Message::CommandComplete(_)) => {
                            break Ok(Async::Ready(None));
                        }
                        Some(_) => break Err(Error::unexpected_message()),
                        None => break Err(Error::closed()),
                    }
                }
                State::Done => break Ok(Async::Ready(None)),
            }
        }
    }
}

impl<T> QueryStream<T>
where
    T: StatementHolder,
{
    pub fn new(client: Client, request: PendingRequest, statement: T) -> QueryStream<T> {
        QueryStream(State::Start {
            client,
            request,
            statement,
        })
    }
}

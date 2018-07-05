use antidote::Mutex;
use futures::sync::mpsc;
use postgres_protocol;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::HashMap;
use std::sync::Arc;

use disconnected;
use error::{self, Error};
use proto::connection::Request;
use proto::execute::ExecuteFuture;
use proto::prepare::PrepareFuture;
use proto::query::QueryStream;
use proto::statement::Statement;
use types::{IsNull, Oid, ToSql, Type};

pub struct PendingRequest {
    sender: mpsc::UnboundedSender<Request>,
    messages: Result<Vec<u8>, Error>,
}

impl PendingRequest {
    pub fn send(self) -> Result<mpsc::Receiver<Message>, Error> {
        let messages = self.messages?;
        let (sender, receiver) = mpsc::channel(0);
        self.sender
            .unbounded_send(Request { messages, sender })
            .map(|_| receiver)
            .map_err(|_| disconnected())
    }
}

pub struct State {
    pub types: HashMap<Oid, Type>,
    pub typeinfo_query: Option<Statement>,
    pub typeinfo_enum_query: Option<Statement>,
    pub typeinfo_composite_query: Option<Statement>,
}

#[derive(Clone)]
pub struct Client {
    pub state: Arc<Mutex<State>>,
    sender: mpsc::UnboundedSender<Request>,
}

impl Client {
    pub fn new(sender: mpsc::UnboundedSender<Request>) -> Client {
        Client {
            state: Arc::new(Mutex::new(State {
                types: HashMap::new(),
                typeinfo_query: None,
                typeinfo_enum_query: None,
                typeinfo_composite_query: None,
            })),
            sender,
        }
    }

    pub fn prepare(&mut self, name: String, query: &str, param_types: &[Type]) -> PrepareFuture {
        let pending = self.pending(|buf| {
            frontend::parse(&name, query, param_types.iter().map(|t| t.oid()), buf)?;
            frontend::describe(b'S', &name, buf)?;
            frontend::sync(buf);
            Ok(())
        });

        PrepareFuture::new(pending, self.sender.clone(), name, self.clone())
    }

    pub fn execute(&mut self, statement: &Statement, params: &[&ToSql]) -> ExecuteFuture {
        let pending = self.pending_execute(statement, params);
        ExecuteFuture::new(pending, statement.clone())
    }

    pub fn query(&mut self, statement: &Statement, params: &[&ToSql]) -> QueryStream {
        let pending = self.pending_execute(statement, params);
        QueryStream::new(pending, statement.clone())
    }

    fn pending_execute(&self, statement: &Statement, params: &[&ToSql]) -> PendingRequest {
        self.pending(|buf| {
            let r = frontend::bind(
                "",
                statement.name(),
                Some(1),
                params.iter().zip(statement.params()),
                |(param, ty), buf| match param.to_sql_checked(ty, buf) {
                    Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
                    Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
                    Err(e) => Err(e),
                },
                Some(1),
                buf,
            );
            match r {
                Ok(()) => {}
                Err(frontend::BindError::Conversion(e)) => return Err(error::conversion(e)),
                Err(frontend::BindError::Serialization(e)) => return Err(Error::from(e)),
            }
            frontend::execute("", 0, buf)?;
            frontend::sync(buf);
            Ok(())
        })
    }

    fn pending<F>(&self, messages: F) -> PendingRequest
    where
        F: FnOnce(&mut Vec<u8>) -> Result<(), Error>,
    {
        let mut buf = vec![];
        PendingRequest {
            sender: self.sender.clone(),
            messages: messages(&mut buf).map(|()| buf),
        }
    }
}

use crate::codec::BackendMessages;
use crate::connection::{Request, RequestMessages};
use crate::prepare::prepare;
use crate::query::{self, Query};
use crate::types::{Oid, ToSql, Type};
use crate::{Error, Statement};
use fallible_iterator::FallibleIterator;
use futures::channel::mpsc;
use futures::future;
use futures::{ready, StreamExt};
use parking_lot::Mutex;
use postgres_protocol::message::backend::Message;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};

pub struct Responses {
    receiver: mpsc::Receiver<BackendMessages>,
    cur: BackendMessages,
}

impl Responses {
    pub fn poll_next(&mut self, cx: &mut Context<'_>) -> Poll<Result<Message, Error>> {
        loop {
            match self.cur.next().map_err(Error::parse)? {
                Some(Message::ErrorResponse(body)) => return Poll::Ready(Err(Error::db(body))),
                Some(message) => return Poll::Ready(Ok(message)),
                None => {}
            }

            match ready!(self.receiver.poll_next_unpin(cx)) {
                Some(messages) => self.cur = messages,
                None => return Poll::Ready(Err(Error::closed())),
            }
        }
    }

    pub async fn next(&mut self) -> Result<Message, Error> {
        future::poll_fn(|cx| self.poll_next(cx)).await
    }
}

struct State {
    typeinfo: Option<Statement>,
    typeinfo_composite: Option<Statement>,
    typeinfo_enum: Option<Statement>,
    types: HashMap<Oid, Type>,
}

pub struct InnerClient {
    sender: mpsc::UnboundedSender<Request>,
    state: Mutex<State>,
}

impl InnerClient {
    pub fn send(&self, messages: RequestMessages) -> Result<Responses, Error> {
        let (sender, receiver) = mpsc::channel(1);
        let request = Request { messages, sender };
        self.sender
            .unbounded_send(request)
            .map_err(|_| Error::closed())?;

        Ok(Responses {
            receiver,
            cur: BackendMessages::empty(),
        })
    }

    pub fn typeinfo(&self) -> Option<Statement> {
        self.state.lock().typeinfo.clone()
    }

    pub fn set_typeinfo(&self, statement: &Statement) {
        self.state.lock().typeinfo = Some(statement.clone());
    }

    pub fn typeinfo_composite(&self) -> Option<Statement> {
        self.state.lock().typeinfo_composite.clone()
    }

    pub fn set_typeinfo_composite(&self, statement: &Statement) {
        self.state.lock().typeinfo_composite = Some(statement.clone());
    }

    pub fn typeinfo_enum(&self) -> Option<Statement> {
        self.state.lock().typeinfo_enum.clone()
    }

    pub fn set_typeinfo_enum(&self, statement: &Statement) {
        self.state.lock().typeinfo_enum = Some(statement.clone());
    }

    pub fn type_(&self, oid: Oid) -> Option<Type> {
        self.state.lock().types.get(&oid).cloned()
    }

    pub fn set_type(&self, oid: Oid, type_: &Type) {
        self.state.lock().types.insert(oid, type_.clone());
    }
}

pub struct Client {
    inner: Arc<InnerClient>,
    process_id: i32,
    secret_key: i32,
}

impl Client {
    pub(crate) fn new(
        sender: mpsc::UnboundedSender<Request>,
        process_id: i32,
        secret_key: i32,
    ) -> Client {
        Client {
            inner: Arc::new(InnerClient {
                sender,
                state: Mutex::new(State {
                    typeinfo: None,
                    typeinfo_composite: None,
                    typeinfo_enum: None,
                    types: HashMap::new(),
                }),
            }),
            process_id,
            secret_key,
        }
    }

    pub(crate) fn inner(&self) -> Arc<InnerClient> {
        self.inner.clone()
    }

    pub fn prepare(&mut self, query: &str) -> impl Future<Output = Result<Statement, Error>> {
        self.prepare_typed(query, &[])
    }

    pub fn prepare_typed(
        &mut self,
        query: &str,
        parameter_types: &[Type],
    ) -> impl Future<Output = Result<Statement, Error>> {
        prepare(self.inner(), query, parameter_types)
    }

    pub fn query(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Future<Output = Result<Query, Error>> {
        let buf = query::encode(statement, params.iter().cloned());
        query::query(self.inner(), statement.clone(), buf)
    }

    pub fn query_iter<'a, I>(
        &mut self,
        statement: &Statement,
        params: I,
    ) -> impl Future<Output = Result<Query, Error>>
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let buf = query::encode(statement, params);
        query::query(self.inner(), statement.clone(), buf)
    }

    pub fn execute(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Future<Output = Result<u64, Error>> {
        let buf = query::encode(statement, params.iter().cloned());
        query::execute(self.inner(), buf)
    }

    pub fn execute_iter<'a, I>(
        &mut self,
        statement: &Statement,
        params: I,
    ) -> impl Future<Output = Result<u64, Error>>
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let buf = query::encode(statement, params);
        query::execute(self.inner(), buf)
    }
}

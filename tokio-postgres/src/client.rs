use crate::codec::BackendMessages;
use crate::connection::{Request, RequestMessages};
use crate::prepare::prepare;
use crate::query::{execute, query, Query};
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
    has_typeinfo: bool,
    has_typeinfo_composite: bool,
    has_typeinfo_enum: bool,
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

    pub fn has_typeinfo(&self) -> bool {
        self.state.lock().has_typeinfo
    }

    pub fn set_has_typeinfo(&self) {
        self.state.lock().has_typeinfo = true;
    }

    pub fn has_typeinfo_composite(&self) -> bool {
        self.state.lock().has_typeinfo_composite
    }

    pub fn set_has_typeinfo_composite(&self) {
        self.state.lock().has_typeinfo_composite = true;
    }

    pub fn has_typeinfo_enum(&self) -> bool {
        self.state.lock().has_typeinfo_enum
    }

    pub fn set_has_typeinfo_enum(&self) {
        self.state.lock().has_typeinfo_enum = true;
    }

    pub fn type_(&self, oid: Oid) -> Option<Type> {
        self.state.lock().types.get(&oid).cloned()
    }

    pub fn set_type(&self, oid: Oid, type_: Type) {
        self.state.lock().types.insert(oid, type_);
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
                    has_typeinfo: false,
                    has_typeinfo_composite: false,
                    has_typeinfo_enum: false,
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

    pub fn prepare<'a>(
        &mut self,
        query: &'a str,
    ) -> impl Future<Output = Result<Statement, Error>> + 'a {
        self.prepare_typed(query, &[])
    }

    pub fn prepare_typed<'a>(
        &mut self,
        query: &'a str,
        parameter_types: &'a [Type],
    ) -> impl Future<Output = Result<Statement, Error>> + 'a {
        prepare(self.inner(), query, parameter_types)
    }

    pub fn query<'a>(
        &mut self,
        statement: &'a Statement,
        params: &'a [&dyn ToSql],
    ) -> impl Future<Output = Result<Query, Error>> + 'a {
        self.query_iter(statement, params.iter().cloned())
    }

    pub fn query_iter<'a, I>(
        &mut self,
        statement: &'a Statement,
        params: I,
    ) -> impl Future<Output = Result<Query, Error>> + 'a
    where
        I: IntoIterator<Item = &'a dyn ToSql> + 'a,
        I::IntoIter: ExactSizeIterator,
    {
        query(self.inner(), statement, params)
    }

    pub fn execute<'a>(
        &mut self,
        statement: &'a Statement,
        params: &'a [&dyn ToSql],
    ) -> impl Future<Output = Result<u64, Error>> + 'a {
        self.execute_iter(statement, params.iter().cloned())
    }

    pub fn execute_iter<'a, I>(
        &mut self,
        statement: &'a Statement,
        params: I,
    ) -> impl Future<Output = Result<u64, Error>> + 'a
    where
        I: IntoIterator<Item = &'a dyn ToSql> + 'a,
        I::IntoIter: ExactSizeIterator,
    {
        execute(self.inner(), statement, params)
    }
}

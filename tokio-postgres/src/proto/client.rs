use antidote::Mutex;
use futures::sync::mpsc;
use postgres_protocol;
use postgres_protocol::message::backend::Message;
use postgres_protocol::message::frontend;
use std::collections::HashMap;
use std::sync::{Arc, Weak};

use disconnected;
use error::{self, Error};
use proto::connection::Request;
use proto::execute::ExecuteFuture;
use proto::prepare::PrepareFuture;
use proto::query::QueryStream;
use proto::simple_query::SimpleQueryFuture;
use proto::statement::Statement;
use types::{IsNull, Oid, ToSql, Type};

pub struct PendingRequest(Result<Vec<u8>, Error>);

pub struct WeakClient(Weak<Inner>);

impl WeakClient {
    pub fn upgrade(&self) -> Option<Client> {
        self.0.upgrade().map(Client)
    }
}

struct State {
    types: HashMap<Oid, Type>,
    typeinfo_query: Option<Statement>,
    typeinfo_enum_query: Option<Statement>,
    typeinfo_composite_query: Option<Statement>,
}

struct Inner {
    state: Mutex<State>,
    sender: mpsc::UnboundedSender<Request>,
}

#[derive(Clone)]
pub struct Client(Arc<Inner>);

impl Client {
    pub fn new(sender: mpsc::UnboundedSender<Request>) -> Client {
        Client(Arc::new(Inner {
            state: Mutex::new(State {
                types: HashMap::new(),
                typeinfo_query: None,
                typeinfo_enum_query: None,
                typeinfo_composite_query: None,
            }),
            sender,
        }))
    }

    pub fn downgrade(&self) -> WeakClient {
        WeakClient(Arc::downgrade(&self.0))
    }

    pub fn cached_type(&self, oid: Oid) -> Option<Type> {
        self.0.state.lock().types.get(&oid).cloned()
    }

    pub fn cache_type(&self, ty: &Type) {
        self.0.state.lock().types.insert(ty.oid(), ty.clone());
    }

    pub fn typeinfo_query(&self) -> Option<Statement> {
        self.0.state.lock().typeinfo_query.clone()
    }

    pub fn set_typeinfo_query(&self, statement: &Statement) {
        self.0.state.lock().typeinfo_query = Some(statement.clone());
    }

    pub fn typeinfo_enum_query(&self) -> Option<Statement> {
        self.0.state.lock().typeinfo_enum_query.clone()
    }

    pub fn set_typeinfo_enum_query(&self, statement: &Statement) {
        self.0.state.lock().typeinfo_enum_query = Some(statement.clone());
    }

    pub fn typeinfo_composite_query(&self) -> Option<Statement> {
        self.0.state.lock().typeinfo_composite_query.clone()
    }

    pub fn set_typeinfo_composite_query(&self, statement: &Statement) {
        self.0.state.lock().typeinfo_composite_query = Some(statement.clone());
    }

    pub fn send(&self, request: PendingRequest) -> Result<mpsc::Receiver<Message>, Error> {
        let messages = request.0?;
        let (sender, receiver) = mpsc::channel(0);
        self.0
            .sender
            .unbounded_send(Request { messages, sender })
            .map(|_| receiver)
            .map_err(|_| disconnected())
    }

    pub fn batch_execute(&self, query: &str) -> SimpleQueryFuture {
        let pending = self.pending(|buf| {
            frontend::query(query, buf)?;
            Ok(())
        });

        SimpleQueryFuture::new(self.clone(), pending)
    }

    pub fn prepare(&self, name: String, query: &str, param_types: &[Type]) -> PrepareFuture {
        let pending = self.pending(|buf| {
            frontend::parse(&name, query, param_types.iter().map(|t| t.oid()), buf)?;
            frontend::describe(b'S', &name, buf)?;
            frontend::sync(buf);
            Ok(())
        });

        PrepareFuture::new(self.clone(), pending, name)
    }

    pub fn execute(&self, statement: &Statement, params: &[&ToSql]) -> ExecuteFuture {
        let pending = self.pending_execute(statement, params);
        ExecuteFuture::new(self.clone(), pending, statement.clone())
    }

    pub fn query(&self, statement: &Statement, params: &[&ToSql]) -> QueryStream {
        let pending = self.pending_execute(statement, params);
        QueryStream::new(self.clone(), pending, statement.clone())
    }

    pub fn close_statement(&self, name: &str) {
        let mut buf = vec![];
        frontend::close(b'S', name, &mut buf).expect("statement name not valid");
        frontend::sync(&mut buf);
        let (sender, _) = mpsc::channel(0);
        let _ = self.0.sender.unbounded_send(Request {
            messages: buf,
            sender,
        });
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
        PendingRequest(messages(&mut buf).map(|()| buf))
    }
}

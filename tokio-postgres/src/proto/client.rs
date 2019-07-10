use antidote::Mutex;
use bytes::IntoBuf;
use futures::sync::mpsc;
use futures::{AsyncSink, Poll, Sink, Stream};
use postgres_protocol;
use postgres_protocol::message::frontend;
use std::collections::HashMap;
use std::error::Error as StdError;
use std::sync::{Arc, Weak};
use tokio_io::{AsyncRead, AsyncWrite};

use crate::proto::bind::BindFuture;
use crate::proto::codec::FrontendMessage;
use crate::proto::connection::{Request, RequestMessages};
use crate::proto::copy_in::{CopyInFuture, CopyInReceiver, CopyMessage};
use crate::proto::copy_out::CopyOutStream;
use crate::proto::execute::ExecuteFuture;
use crate::proto::idle::{IdleGuard, IdleState};
use crate::proto::portal::Portal;
use crate::proto::prepare::PrepareFuture;
use crate::proto::query::QueryStream;
use crate::proto::responses::{self, Responses};
use crate::proto::simple_query::SimpleQueryStream;
use crate::proto::statement::Statement;
#[cfg(feature = "runtime")]
use crate::proto::CancelQueryFuture;
use crate::proto::CancelQueryRawFuture;
use crate::types::{IsNull, Oid, ToSql, Type};
use crate::{Config, Error, TlsConnect};
#[cfg(feature = "runtime")]
use crate::{MakeTlsConnect, Socket};

pub struct PendingRequest(Result<(RequestMessages, IdleGuard), Error>);

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
    idle: IdleState,
    sender: mpsc::UnboundedSender<Request>,
    process_id: i32,
    secret_key: i32,
    #[cfg_attr(not(feature = "runtime"), allow(dead_code))]
    config: Config,
    #[cfg_attr(not(feature = "runtime"), allow(dead_code))]
    idx: Option<usize>,
}

#[derive(Clone)]
pub struct Client(Arc<Inner>);

impl Client {
    pub fn new(
        sender: mpsc::UnboundedSender<Request>,
        process_id: i32,
        secret_key: i32,
        config: Config,
        idx: Option<usize>,
    ) -> Client {
        Client(Arc::new(Inner {
            state: Mutex::new(State {
                types: HashMap::new(),
                typeinfo_query: None,
                typeinfo_enum_query: None,
                typeinfo_composite_query: None,
            }),
            idle: IdleState::new(),
            sender,
            process_id,
            secret_key,
            config,
            idx,
        }))
    }

    pub fn is_closed(&self) -> bool {
        self.0.sender.is_closed()
    }

    pub fn poll_idle(&self) -> Poll<(), Error> {
        self.0.idle.poll_idle()
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

    pub fn send(&self, request: PendingRequest) -> Result<Responses, Error> {
        let (messages, idle) = request.0?;
        let (sender, receiver) = responses::channel();
        self.0
            .sender
            .unbounded_send(Request {
                messages,
                sender,
                idle: Some(idle),
            })
            .map(|_| receiver)
            .map_err(|_| Error::closed())
    }

    pub fn simple_query(&self, query: &str) -> SimpleQueryStream {
        let pending = self.pending(|buf| {
            frontend::query(query, buf).map_err(Error::parse)?;
            Ok(())
        });

        SimpleQueryStream::new(self.clone(), pending)
    }

    pub fn prepare(&self, name: String, query: &str, param_types: &[Type]) -> PrepareFuture {
        let pending = self.pending(|buf| {
            frontend::parse(&name, query, param_types.iter().map(Type::oid), buf)
                .map_err(Error::parse)?;
            frontend::describe(b'S', &name, buf).map_err(Error::parse)?;
            frontend::sync(buf);
            Ok(())
        });

        PrepareFuture::new(self.clone(), pending, name)
    }

    pub fn execute<'a, I>(&self, statement: &Statement, params: I) -> ExecuteFuture
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let pending = PendingRequest(
            self.excecute_message(statement, params)
                .map(|m| (RequestMessages::Single(m), self.0.idle.guard())),
        );
        ExecuteFuture::new(self.clone(), pending, statement.clone())
    }

    pub fn query<'a, I>(&self, statement: &Statement, params: I) -> QueryStream<Statement>
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let pending = PendingRequest(
            self.excecute_message(statement, params)
                .map(|m| (RequestMessages::Single(m), self.0.idle.guard())),
        );
        QueryStream::new(self.clone(), pending, statement.clone())
    }

    pub fn bind<'a, I>(&self, statement: &Statement, name: String, params: I) -> BindFuture
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let mut buf = self.bind_message(statement, &name, params);
        if let Ok(ref mut buf) = buf {
            frontend::sync(buf);
        }
        let pending = PendingRequest(buf.map(|m| {
            (
                RequestMessages::Single(FrontendMessage::Raw(m)),
                self.0.idle.guard(),
            )
        }));
        BindFuture::new(self.clone(), pending, name, statement.clone())
    }

    pub fn query_portal(&self, portal: &Portal, rows: i32) -> QueryStream<Portal> {
        let pending = self.pending(|buf| {
            frontend::execute(portal.name(), rows, buf).map_err(Error::parse)?;
            frontend::sync(buf);
            Ok(())
        });
        QueryStream::new(self.clone(), pending, portal.clone())
    }

    pub fn copy_in<'a, S, I>(&self, statement: &Statement, params: I, stream: S) -> CopyInFuture<S>
    where
        S: Stream,
        S::Item: IntoBuf,
        <S::Item as IntoBuf>::Buf: 'static + Send,
        S::Error: Into<Box<dyn StdError + Sync + Send>>,
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let (mut sender, receiver) = mpsc::channel(1);
        let pending = PendingRequest(self.excecute_message(statement, params).map(|data| {
            match sender.start_send(CopyMessage::Message(data)) {
                Ok(AsyncSink::Ready) => {}
                _ => unreachable!("channel should have capacity"),
            }
            (
                RequestMessages::CopyIn {
                    receiver: CopyInReceiver::new(receiver),
                    pending_message: None,
                },
                self.0.idle.guard(),
            )
        }));
        CopyInFuture::new(self.clone(), pending, statement.clone(), stream, sender)
    }

    pub fn copy_out<'a, I>(&self, statement: &Statement, params: I) -> CopyOutStream
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let pending = PendingRequest(
            self.excecute_message(statement, params)
                .map(|m| (RequestMessages::Single(m), self.0.idle.guard())),
        );
        CopyOutStream::new(self.clone(), pending, statement.clone())
    }

    pub fn close_statement(&self, name: &str) {
        self.close(b'S', name)
    }

    pub fn close_portal(&self, name: &str) {
        self.close(b'P', name)
    }

    #[cfg(feature = "runtime")]
    pub fn cancel_query<T>(&self, make_tls_mode: T) -> CancelQueryFuture<T>
    where
        T: MakeTlsConnect<Socket>,
    {
        CancelQueryFuture::new(
            make_tls_mode,
            self.0.idx,
            self.0.config.clone(),
            self.0.process_id,
            self.0.secret_key,
        )
    }

    pub fn cancel_query_raw<S, T>(&self, stream: S, mode: T) -> CancelQueryRawFuture<S, T>
    where
        S: AsyncRead + AsyncWrite,
        T: TlsConnect<S>,
    {
        CancelQueryRawFuture::new(
            stream,
            self.0.config.0.ssl_mode,
            mode,
            self.0.process_id,
            self.0.secret_key,
        )
    }

    fn close(&self, ty: u8, name: &str) {
        let mut buf = vec![];
        frontend::close(ty, name, &mut buf).expect("statement name not valid");
        frontend::sync(&mut buf);
        let (sender, _) = mpsc::channel(0);
        let _ = self.0.sender.unbounded_send(Request {
            messages: RequestMessages::Single(FrontendMessage::Raw(buf)),
            sender,
            idle: None,
        });
    }

    fn bind_message<'a, I>(
        &self,
        statement: &Statement,
        name: &str,
        params: I,
    ) -> Result<Vec<u8>, Error>
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let params = params.into_iter();

        assert!(
            statement.params().len() == params.len(),
            "expected {} parameters but got {}",
            statement.params().len(),
            params.len()
        );

        let mut buf = vec![];
        let mut error_idx = 0;
        let r = frontend::bind(
            name,
            statement.name(),
            Some(1),
            params.zip(statement.params()).enumerate(),
            |(idx, (param, ty)), buf| match param.to_sql_checked(ty, buf) {
                Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
                Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
                Err(e) => {
                    error_idx = idx;
                    Err(e)
                }
            },
            Some(1),
            &mut buf,
        );
        match r {
            Ok(()) => Ok(buf),
            Err(frontend::BindError::Conversion(e)) => Err(Error::to_sql(e, error_idx)),
            Err(frontend::BindError::Serialization(e)) => Err(Error::encode(e)),
        }
    }

    fn excecute_message<'a, I>(
        &self,
        statement: &Statement,
        params: I,
    ) -> Result<FrontendMessage, Error>
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let mut buf = self.bind_message(statement, "", params)?;
        frontend::execute("", 0, &mut buf).map_err(Error::parse)?;
        frontend::sync(&mut buf);
        Ok(FrontendMessage::Raw(buf))
    }

    fn pending<F>(&self, messages: F) -> PendingRequest
    where
        F: FnOnce(&mut Vec<u8>) -> Result<(), Error>,
    {
        let mut buf = vec![];
        PendingRequest(messages(&mut buf).map(|()| {
            (
                RequestMessages::Single(FrontendMessage::Raw(buf)),
                self.0.idle.guard(),
            )
        }))
    }
}

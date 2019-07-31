use crate::codec::BackendMessages;
use crate::config::{Host, SslMode};
use crate::connection::{Request, RequestMessages};
#[cfg(feature = "runtime")]
use crate::tls::MakeTlsConnect;
use crate::tls::TlsConnect;
use crate::types::{Oid, ToSql, Type};
#[cfg(feature = "runtime")]
use crate::Socket;
use crate::{cancel_query, cancel_query_raw, query, Transaction};
use crate::{prepare, SimpleQueryMessage};
use crate::{simple_query, Row};
use crate::{Error, Statement};
use fallible_iterator::FallibleIterator;
use futures::channel::mpsc;
use futures::{future, Stream};
use futures::{ready, StreamExt};
use parking_lot::Mutex;
use postgres_protocol::message::backend::Message;
use std::collections::HashMap;
use std::future::Future;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Duration;
use tokio::io::{AsyncRead, AsyncWrite};

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

#[derive(Clone)]
pub(crate) struct SocketConfig {
    pub host: Host,
    pub port: u16,
    pub connect_timeout: Option<Duration>,
    pub keepalives: bool,
    pub keepalives_idle: Duration,
}

pub struct Client {
    inner: Arc<InnerClient>,
    socket_config: Option<SocketConfig>,
    ssl_mode: SslMode,
    process_id: i32,
    secret_key: i32,
}

impl Client {
    pub(crate) fn new(
        sender: mpsc::UnboundedSender<Request>,
        ssl_mode: SslMode,
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
            socket_config: None,
            ssl_mode,
            process_id,
            secret_key,
        }
    }

    pub(crate) fn inner(&self) -> Arc<InnerClient> {
        self.inner.clone()
    }

    pub(crate) fn set_socket_config(&mut self, socket_config: SocketConfig) {
        self.socket_config = Some(socket_config);
    }

    /// Creates a new prepared statement.
    ///
    /// Prepared statements can be executed repeatedly, and may contain query parameters (indicated by `$1`, `$2`, etc),
    /// which are set when executed. Prepared statements can only be used with the connection that created them.
    pub fn prepare(&mut self, query: &str) -> impl Future<Output = Result<Statement, Error>> {
        self.prepare_typed(query, &[])
    }

    /// Like `prepare`, but allows the types of query parameters to be explicitly specified.
    ///
    /// The list of types may be smaller than the number of parameters - the types of the remaining parameters will be
    /// inferred. For example, `client.prepare_typed(query, &[])` is equivalent to `client.prepare(query)`.
    pub fn prepare_typed(
        &mut self,
        query: &str,
        parameter_types: &[Type],
    ) -> impl Future<Output = Result<Statement, Error>> {
        prepare::prepare(self.inner(), query, parameter_types)
    }

    /// Executes a statement, returning a stream of the resulting rows.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn query(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Stream<Item = Result<Row, Error>> {
        let buf = query::encode(statement, params.iter().cloned());
        query::query(self.inner(), statement.clone(), buf)
    }

    /// Like [`query`], but takes an iterator of parameters rather than a slice.
    ///
    /// [`query`]: #method.query
    pub fn query_iter<'a, I>(
        &mut self,
        statement: &Statement,
        params: I,
    ) -> impl Stream<Item = Result<Row, Error>>
    where
        I: IntoIterator<Item = &'a dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let buf = query::encode(statement, params);
        query::query(self.inner(), statement.clone(), buf)
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// If the statement does not modify any rows (e.g. `SELECT`), 0 is returned.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn execute(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Future<Output = Result<u64, Error>> {
        let buf = query::encode(statement, params.iter().cloned());
        query::execute(self.inner(), buf)
    }

    /// Like [`execute`], but takes an iterator of parameters rather than a slice.
    ///
    /// [`execute`]: #method.execute
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

    /// Executes a sequence of SQL statements using the simple query protocol, returning the resulting rows.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. The simple query protocol returns the values in rows as strings rather than in their binary encodings,
    /// so the associated row type doesn't work with the `FromSql` trait. Rather than simply returning a stream over the
    /// rows, this method returns a stream over an enum which indicates either the completion of one of the commands,
    /// or a row of data. This preserves the framing between the separate statements in the request.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub fn simple_query(
        &mut self,
        query: &str,
    ) -> impl Stream<Item = Result<SimpleQueryMessage, Error>> {
        simple_query::simple_query(self.inner(), query)
    }

    /// Executes a sequence of SQL statements using the simple query protocol.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. This is intended for use when, for example, initializing a database schema.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely embed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub fn batch_execute(&mut self, query: &str) -> impl Future<Output = Result<(), Error>> {
        simple_query::batch_execute(self.inner(), query)
    }

    /// Begins a new database transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.batch_execute("BEGIN").await?;
        Ok(Transaction::new(self))
    }

    /// Attempts to cancel an in-progress query.
    ///
    /// The server provides no information about whether a cancellation attempt was successful or not. An error will
    /// only be returned if the client was unable to connect to the database.
    ///
    /// Requires the `runtime` Cargo feature (enabled by default).
    #[cfg(feature = "runtime")]
    pub fn cancel_query<T>(&mut self, tls: T) -> impl Future<Output = Result<(), Error>>
    where
        T: MakeTlsConnect<Socket>,
    {
        cancel_query::cancel_query(
            self.socket_config.clone(),
            self.ssl_mode,
            tls,
            self.process_id,
            self.secret_key,
        )
    }

    /// Like `cancel_query`, but uses a stream which is already connected to the server rather than opening a new
    /// connection itself.
    pub fn cancel_query_raw<S, T>(
        &mut self,
        stream: S,
        tls: T,
    ) -> impl Future<Output = Result<(), Error>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        T: TlsConnect<S>,
    {
        cancel_query_raw::cancel_query_raw(
            stream,
            self.ssl_mode,
            tls,
            self.process_id,
            self.secret_key,
        )
    }
}

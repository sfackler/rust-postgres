extern crate fallible_iterator;
extern crate futures;
extern crate postgres_shared;
extern crate postgres_protocol;
extern crate tokio_core;
extern crate tokio_dns;
extern crate tokio_uds;

use fallible_iterator::FallibleIterator;
use futures::{Future, IntoFuture, BoxFuture, Stream, Sink, Poll, StartSend};
use futures::future::Either;
use postgres_protocol::authentication;
use postgres_protocol::message::{backend, frontend};
use postgres_protocol::message::backend::{ErrorResponseBody, ErrorFields};
use postgres_shared::RowData;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::mem;
use std::sync::mpsc::{self, Sender, Receiver};
use tokio_core::reactor::Handle;

#[doc(inline)]
pub use postgres_shared::{params, types};

use error::{ConnectError, Error, DbError};
use params::{ConnectParams, IntoConnectParams};
use stream::PostgresStream;
use types::{Oid, Type, ToSql, SessionInfo, IsNull};

pub mod error;
mod stream;

#[cfg(test)]
mod test;

#[derive(Debug, Copy, Clone)]
pub struct CancelData {
    pub process_id: i32,
    pub secret_key: i32,
}

struct InnerConnection {
    stream: PostgresStream,
    close_receiver: Receiver<(u8, String)>,
    close_sender: Sender<(u8, String)>,
    parameters: HashMap<String, String>,
    cancel_data: CancelData,
    next_stmt_id: u32,
}

impl InnerConnection {
    fn read(self) -> BoxFuture<(backend::Message<Vec<u8>>, InnerConnection), io::Error> {
        self.into_future()
            .map_err(|e| e.0)
            .and_then(|(m, mut s)| {
                match m {
                    Some(backend::Message::ParameterStatus(body)) => {
                        let name = match body.name() {
                            Ok(name) => name.to_owned(),
                            Err(e) => return Either::A(Err(e).into_future()),
                        };
                        let value = match body.value() {
                            Ok(value) => value.to_owned(),
                            Err(e) => return Either::A(Err(e).into_future()),
                        };
                        s.parameters.insert(name, value);
                        Either::B(s.read())
                    }
                    Some(backend::Message::NoticeResponse(_)) => {
                        // TODO forward the error
                        Either::B(s.read())
                    }
                    Some(m) => Either::A(Ok((m, s)).into_future()),
                    None => {
                        let err = io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF");
                        Either::A(Err(err).into_future())
                    }
                }
            })
            .boxed()
    }
}

impl Stream for InnerConnection {
    type Item = backend::Message<Vec<u8>>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<backend::Message<Vec<u8>>>, io::Error> {
        self.stream.poll()
    }
}

impl Sink for InnerConnection {
    type SinkItem = Vec<u8>;
    type SinkError = io::Error;

    fn start_send(&mut self, item: Vec<u8>) -> StartSend<Vec<u8>, io::Error> {
        self.stream.start_send(item)
    }

    fn poll_complete(&mut self) -> Poll<(), io::Error> {
        self.stream.poll_complete()
    }
}

pub struct Connection(InnerConnection);

// FIXME fill out
impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Connection")
            .finish()
    }
}

impl Connection {
    pub fn connect<T>(params: T, handle: &Handle) -> BoxFuture<Connection, ConnectError>
        where T: IntoConnectParams
    {
        let params = match params.into_connect_params() {
            Ok(params) => params,
            Err(e) => return futures::failed(ConnectError::ConnectParams(e)).boxed(),
        };

        stream::connect(params.host(), params.port(), handle)
            .map_err(ConnectError::Io)
            .map(|s| {
                let (sender, receiver) = mpsc::channel();
                Connection(InnerConnection {
                    stream: s,
                    close_sender: sender,
                    close_receiver: receiver,
                    parameters: HashMap::new(),
                    cancel_data: CancelData {
                        process_id: 0,
                        secret_key: 0,
                    },
                    next_stmt_id: 0,
                })
            })
            .and_then(|s| s.startup(params))
            .and_then(|(s, params)| s.handle_auth(params))
            .and_then(|s| s.finish_startup())
            .boxed()
    }

    fn startup(self, params: ConnectParams) -> BoxFuture<(Connection, ConnectParams), ConnectError> {
        let mut buf = vec![];
        let result = {
            let options = [("client_encoding", "UTF8"), ("timezone", "GMT")];
            let options = options.iter().cloned();
            let options = options.chain(params.user().map(|u| ("user", u.name())));
            let options = options.chain(params.database().map(|d| ("database", d)));
            let options = options.chain(params.options().iter().map(|e| (&*e.0, &*e.1)));

            frontend::startup_message(options, &mut buf)
        };

        result
            .into_future()
            .and_then(move |()| self.0.send(buf))
            .and_then(|s| s.flush())
            .map_err(ConnectError::Io)
            .map(move |s| (Connection(s), params))
            .boxed()
    }

    fn handle_auth(self, params: ConnectParams) -> BoxFuture<Connection, ConnectError> {
        self.0.read()
            .map_err(ConnectError::Io)
            .and_then(move |(m, s)| {
                let response = match m {
                    backend::Message::AuthenticationOk => Ok(None),
                    backend::Message::AuthenticationCleartextPassword => {
                        match params.user().and_then(|u| u.password()) {
                            Some(pass) => {
                                let mut buf = vec![];
                                frontend::password_message(pass, &mut buf)
                                    .map(|()| Some(buf))
                                    .map_err(Into::into)
                            }
                            None => {
                                Err(ConnectError::ConnectParams(
                                    "password was required but not provided".into()))
                            }
                        }
                    }
                    backend::Message::AuthenticationMd5Password(body) => {
                        match params.user().and_then(|u| u.password().map(|p| (u.name(), p))) {
                            Some((user, pass)) => {
                                let pass = authentication::md5_hash(user.as_bytes(),
                                                                    pass.as_bytes(),
                                                                    body.salt());
                                let mut buf = vec![];
                                frontend::password_message(&pass, &mut buf)
                                    .map(|()| Some(buf))
                                    .map_err(Into::into)
                            }
                            None => {
                                Err(ConnectError::ConnectParams(
                                    "password was required but not provided".into()))
                            }
                        }
                    }
                    backend::Message::ErrorResponse(body) => Err(connect_err(&mut body.fields())),
                    _ => Err(bad_message()),
                };

                response.map(|m| (m, Connection(s)))
            })
            .and_then(|(m, s)| {
                match m {
                    Some(m) => Either::A(s.handle_auth_response(m)),
                    None => Either::B(Ok(s).into_future())
                }
            })
            .boxed()
    }

    fn handle_auth_response(self, message: Vec<u8>) -> BoxFuture<Connection, ConnectError> {
        self.0.send(message)
            .and_then(|s| s.flush())
            .and_then(|s| s.read())
            .map_err(ConnectError::Io)
            .and_then(|(m, s)| {
                match m {
                    backend::Message::AuthenticationOk => Ok(Connection(s)),
                    backend::Message::ErrorResponse(body) => Err(connect_err(&mut body.fields())),
                    _ => Err(bad_message()),
                }
            })
            .boxed()
    }

    fn finish_startup(self) -> BoxFuture<Connection, ConnectError> {
        self.0.read()
            .map_err(ConnectError::Io)
            .and_then(|(m, mut s)| {
                match m {
                    backend::Message::BackendKeyData(body) => {
                        s.cancel_data.process_id = body.process_id();
                        s.cancel_data.secret_key = body.secret_key();
                        Either::A(Connection(s).finish_startup())
                    }
                    backend::Message::ReadyForQuery(_) => Either::B(Ok(Connection(s)).into_future()),
                    backend::Message::ErrorResponse(body) => {
                        Either::B(Err(connect_err(&mut body.fields())).into_future())
                    }
                    _ => Either::B(Err(bad_message()).into_future()),
                }
            })
            .boxed()
    }

    fn simple_query(self, query: &str) -> BoxFuture<(Vec<RowData>, Connection), Error> {
        let mut buf = vec![];
        frontend::query(query, &mut buf)
            .map(|()| buf)
            .into_future()
            .and_then(move |buf| self.0.send(buf))
            .and_then(|s| s.flush())
            .map_err(Error::Io)
            .and_then(|s| Connection(s).simple_read_rows(vec![]))
            .boxed()
    }

    // This has its own read_rows since it will need to handle multiple query completions
    fn simple_read_rows(self, mut rows: Vec<RowData>) -> BoxFuture<(Vec<RowData>, Connection), Error> {
        self.0.read()
            .map_err(Error::Io)
            .and_then(|(m, s)| {
                match m {
                    backend::Message::ReadyForQuery(_) => {
                        Ok((rows, Connection(s))).into_future().boxed()
                    }
                    backend::Message::DataRow(body) => {
                        match body.values().collect() {
                            Ok(row) => {
                                rows.push(row);
                                Connection(s).simple_read_rows(rows)
                            }
                            Err(e) => Err(Error::Io(e)).into_future().boxed(),
                        }
                    }
                    backend::Message::EmptyQueryResponse |
                    backend::Message::CommandComplete(_) |
                    backend::Message::RowDescription(_) => Connection(s).simple_read_rows(rows),
                    backend::Message::ErrorResponse(body) => Connection(s).ready_err(body),
                    _ => Err(bad_message()).into_future().boxed(),
                }
            })
            .boxed()
    }

    #[allow(dead_code)]
    fn read_rows(self, mut rows: Vec<RowData>) -> BoxFuture<(Vec<RowData>, Connection), Error> {
        self.0.read()
            .map_err(Error::Io)
            .and_then(|(m, s)| {
                match m {
                    backend::Message::EmptyQueryResponse |
                    backend::Message::CommandComplete(_) => Connection(s).ready(rows).boxed(),
                    backend::Message::DataRow(body) => {
                        match body.values().collect() {
                            Ok(row) => {
                                rows.push(row);
                                Connection(s).read_rows(rows)
                            }
                            Err(e) => Err(Error::Io(e)).into_future().boxed(),
                        }
                    }
                    backend::Message::ErrorResponse(body) => Connection(s).ready_err(body),
                    _ => Err(bad_message()).into_future().boxed(),
                }
            })
            .boxed()
    }

    fn ready<T>(self, t: T) -> BoxFuture<(T, Connection), Error>
        where T: 'static + Send
    {
        self.0.read()
            .map_err(Error::Io)
            .and_then(|(m, s)| {
                match m {
                    backend::Message::ReadyForQuery(_) => Ok((t, Connection(s))),
                    _ => Err(bad_message())
                }
            })
            .and_then(|(t, s)| s.close_gc().map(|s| (t, s)))
            .boxed()
    }

    fn close_gc(self) -> BoxFuture<Connection, Error> {
        let mut messages = vec![];
        while let Ok((type_, name)) = self.0.close_receiver.try_recv() {
            let mut buf = vec![];
            frontend::close(type_, &name, &mut buf).unwrap(); // this can only fail on bad names
            messages.push(buf);
        }
        if messages.is_empty() {
            return Ok(self).into_future().boxed();
        }

        let mut buf = vec![];
        frontend::sync(&mut buf);
        messages.push(buf);
        self.0.send_all(futures::stream::iter(messages.into_iter().map(Ok::<_, io::Error>)))
            .map_err(Error::Io)
            .and_then(|s| Connection(s.0).finish_close_gc())
            .boxed()
    }

    fn finish_close_gc(self) -> BoxFuture<Connection, Error> {
        self.0.read()
            .map_err(Error::Io)
            .and_then(|(m, s)| {
                match m {
                    backend::Message::ReadyForQuery(_) => {
                        Either::A(Ok(Connection(s)).into_future())
                    }
                    backend::Message::CloseComplete => Either::B(Connection(s).finish_close_gc()),
                    backend::Message::ErrorResponse(body) => {
                        Either::B(Connection(s).ready_err(body))
                    }
                    _ => Either::A(Err(bad_message()).into_future()),
                }
            })
            .boxed()
    }

    fn ready_err<T>(self, body: ErrorResponseBody<Vec<u8>>) -> BoxFuture<T, Error>
        where T: 'static + Send
    {
        DbError::new(&mut body.fields())
            .map_err(Error::Io)
            .into_future()
            .and_then(|e| self.ready(e))
            .and_then(|(e, s)| Err(Error::Db(Box::new(e), s)))
            .boxed()
    }

    pub fn batch_execute(self, query: &str) -> BoxFuture<Connection, Error> {
        self.simple_query(query)
            .map(|r| r.1)
            .boxed()
    }

    fn raw_prepare(self,
                   name: &str,
                   query: &str)
                   -> BoxFuture<(Vec<Type>, Vec<Column>, Connection), Error> {
        let mut parse = vec![];
        let mut describe = vec![];
        let mut sync = vec![];
        frontend::sync(&mut sync);
        frontend::parse(name, query, None, &mut parse)
            .and_then(|()| frontend::describe(b'S', name, &mut describe))
            .into_future()
            .and_then(move |()| {
                let it = Some(parse).into_iter()
                    .chain(Some(describe))
                    .chain(Some(sync))
                    .map(Ok::<_, io::Error>);
                self.0.send_all(futures::stream::iter(it))
            })
            .and_then(|s| s.0.read())
            .map_err(Error::Io)
            .boxed() // work around nonlinear trans blowup
            .and_then(|(m, s)| {
                match m {
                    backend::Message::ParseComplete => Either::A(Ok(s).into_future()),
                    backend::Message::ErrorResponse(body) => {
                        Either::B(Connection(s).ready_err(body))
                    }
                    _ => Either::A(Err(bad_message()).into_future()),
                }
            })
            .and_then(|s| s.read().map_err(Error::Io))
            .and_then(|(m, s)| {
                match m {
                    backend::Message::ParameterDescription(body) => {
                        body.parameters().collect::<Vec<_>>()
                            .map(|p| (p, s))
                            .map_err(Error::Io)
                    }
                    _ => Err(bad_message()),
                }
            })
            .and_then(|(p, s)| s.read().map(|(m, s)| (p, m, s)).map_err(Error::Io))
            .boxed() // work around nonlinear trans blowup
            .and_then(|(p, m, s)| {
                match m {
                    backend::Message::RowDescription(body) => {
                        body.fields()
                            .map(|f| (f.name().to_owned(), f.type_oid()))
                            .collect::<Vec<_>>()
                            .map(|d| (p, d, s))
                            .map_err(Error::Io)
                    }
                    backend::Message::NoData => Ok((p, vec![], s)),
                    _ => Err(bad_message()),
                }
            })
            .and_then(|(p, r, s)| Connection(s).ready((p, r)))
            .and_then(|((p, r), s)| {
                s.get_types(p.into_iter(), vec![], |&p| p, |_, t| t)
                    .map(|(p, s)| (p, r, s))
            })
            .and_then(|(p, r, s)| {
                s.get_types(r.into_iter(),
                            vec![],
                            |f| f.1,
                            |f, t| Column { name: f.0, type_: t })
                    .map(|(r, s)| (p, r, s))
            })
            .boxed()
    }

    fn get_types<T, U, I, F, G>(self,
                                mut raw: I,
                                mut out: Vec<U>,
                                mut get_oid: F,
                                mut build: G)
                                -> BoxFuture<(Vec<U>, Connection), Error>
        where T: 'static + Send,
              U: 'static + Send,
              I: 'static + Send + Iterator<Item = T>,
              F: 'static + Send + FnMut(&T) -> Oid,
              G: 'static + Send + FnMut(T, Type) -> U
    {
        match raw.next() {
            Some(v) => {
                let oid = get_oid(&v);
                self.get_type(oid)
                    .and_then(move |(ty, s)| {
                        out.push(build(v, ty));
                        s.get_types(raw, out, get_oid, build)
                    })
                    .boxed()
            }
            None => Ok((out, self)).into_future().boxed(),
        }
    }

    fn get_type(self, oid: Oid) -> BoxFuture<(Type, Connection), Error> {
        if let Some(type_) = Type::from_oid(oid) {
            return Ok((type_, self)).into_future().boxed();
        };
        unimplemented!()
    }

    fn raw_execute(self,
                   stmt: &str,
                   portal: &str,
                   param_types: &[Type],
                   params: &[&ToSql])
                   -> BoxFuture<Connection, Error> {
        assert!(param_types.len() == params.len(),
                "expected {} parameters but got {}",
                param_types.len(),
                params.len());

        let mut bind = vec![];
        let mut execute = vec![];
        let mut sync = vec![];
        frontend::sync(&mut sync);
        let r = frontend::bind(portal,
                               stmt,
                               Some(1),
                               params.iter().zip(param_types),
                               |(param, ty), buf| {
                                   let info = SessionInfo::new(&self.0.parameters);
                                   match param.to_sql_checked(ty, buf, &info) {
                                       Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
                                       Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
                                       Err(e) => Err(e),
                                   }
                               },
                               Some(1),
                               &mut bind);
        let r = match r {
            Ok(()) => Ok(self),
            Err(frontend::BindError::Conversion(e)) => Err(Error::Conversion(e, self)),
            Err(frontend::BindError::Serialization(e)) => Err(Error::Io(e)),
        };

        r.and_then(|s| {
                frontend::execute(portal, 0, &mut execute)
                    .map(|()| s)
                    .map_err(Error::Io)
            })
            .into_future()
            .and_then(|s| {
                let it = Some(bind).into_iter()
                    .chain(Some(execute))
                    .chain(Some(sync))
                    .map(Ok::<_, io::Error>);
                s.0.send_all(futures::stream::iter(it)).map_err(Error::Io)
            })
            .and_then(|s| s.0.read().map_err(Error::Io))
            .and_then(|(m, s)| {
                match m {
                    backend::Message::BindComplete => Either::A(Ok(Connection(s)).into_future()),
                    backend::Message::ErrorResponse(body) => {
                        Either::B(Connection(s).ready_err(body))
                    }
                    _ => Either::A(Err(bad_message()).into_future()),
                }
            })
            .boxed()
    }

    fn finish_execute(self) -> BoxFuture<(u64, Connection), Error> {
        self.0.read()
            .map_err(Error::Io)
            .and_then(|(m, s)| {
                match m {
                    backend::Message::DataRow(_) => Either::B(Connection(s).finish_execute()),
                    backend::Message::CommandComplete(body) => {
                        Either::A(body.tag()
                            .map(|tag| {
                                let num = tag.split_whitespace()
                                    .last()
                                    .unwrap()
                                    .parse()
                                    .unwrap_or(0);
                                (num, Connection(s))
                            })
                            .map_err(Error::Io)
                            .into_future())
                    }
                    backend::Message::EmptyQueryResponse => {
                        Either::A(Ok((0, Connection(s))).into_future())
                    }
                    backend::Message::ErrorResponse(body) => {
                        Either::B(Connection(s).ready_err(body))
                    }
                    _ => Either::A(Err(bad_message()).into_future()),
                }
            })
            .and_then(|(n, s)| s.ready(n))
            .boxed()
    }

    pub fn prepare(mut self, query: &str) -> BoxFuture<(Statement, Connection), Error> {
        let id = self.0.next_stmt_id;
        self.0.next_stmt_id += 1;
        let name = format!("s{}", id);
        self.raw_prepare(&name, query)
            .map(|(params, columns, conn)| {
                let stmt = Statement {
                    close_sender: conn.0.close_sender.clone(),
                    name: name,
                    params: params,
                    columns: columns,
                };
                (stmt, conn)
            })
            .boxed()
    }

    pub fn close(self) -> BoxFuture<(), Error> {
        let mut terminate = vec![];
        frontend::terminate(&mut terminate);
        self.0.send(terminate)
            .map(|_| ())
            .map_err(Error::Io)
            .boxed()
    }

    pub fn cancel_data(&self) -> CancelData {
        self.0.cancel_data
    }
}

pub struct Column {
    name: String,
    type_: Type,
}

impl Column {
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_(&self) -> &Type {
        &self.type_
    }
}

pub struct Statement {
    close_sender: Sender<(u8, String)>,
    name: String,
    params: Vec<Type>,
    columns: Vec<Column>,
}

impl Drop for Statement {
    fn drop(&mut self) {
        let name = mem::replace(&mut self.name, String::new());
        let _ = self.close_sender.send((b'S', name));
    }
}

impl Statement {
    pub fn parameters(&self) -> &[Type] {
        &self.params
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn execute(self,
                   params: &[&ToSql],
                   conn: Connection)
                   -> BoxFuture<(u64, Statement, Connection), Error> {
        conn.raw_execute(&self.name, "", &self.params, params)
            .and_then(|conn| conn.finish_execute())
            .map(|(n, conn)| (n, self, conn))
            .boxed()
    }
}

fn connect_err(fields: &mut ErrorFields) -> ConnectError {
    match DbError::new(fields) {
        Ok(err) => ConnectError::Db(Box::new(err)),
        Err(err) => ConnectError::Io(err),
    }
}

fn bad_message<T>() -> T
    where T: From<io::Error>
{
    io::Error::new(io::ErrorKind::InvalidInput, "unexpected message").into()
}

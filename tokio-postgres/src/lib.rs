//! An asynchronous Postgres driver using Tokio.
//!
//! # Example
//!
//! ```rust,no_run
//! extern crate futures;
//! extern crate futures_state_stream;
//! extern crate tokio_core;
//! extern crate tokio_postgres;
//!
//! use futures::Future;
//! use futures_state_stream::StateStream;
//! use tokio_core::reactor::Core;
//! use tokio_postgres::{Connection, TlsMode};
//!
//! struct Person {
//!     id: i32,
//!     name: String,
//!     data: Option<Vec<u8>>
//! }
//!
//! fn main() {
//!     let mut l = Core::new().unwrap();
//!     let done = Connection::connect("postgresql://postgres@localhost", TlsMode::None, &l.handle())
//!         .then(|c| {
//!             c.unwrap()
//!                 .batch_execute("CREATE TABLE person (
//!                                     id              SERIAL PRIMARY KEY,
//!                                     name            VARCHAR NOT NULL,
//!                                     data            BYTEA
//!                                 )")
//!         })
//!         .and_then(|c| c.prepare("INSERT INTO person (name, data) VALUES ($1, $2)"))
//!         .and_then(|(s, c)| c.execute(&s, &[&"Steven", &None::<Vec<u8>>]))
//!         .and_then(|(_, c)| c.prepare("SELECT id, name, data FROM person"))
//!         .and_then(|(s, c)| {
//!             c.query(&s, &[])
//!                 .for_each(|row| {
//!                     let person = Person {
//!                         id: row.get(0),
//!                         name: row.get(1),
//!                         data: row.get(2),
//!                     };
//!                     println!("Found person {}", person.name);
//!                     Ok(())
//!                 })
//!         });
//!
//!     l.run(done).unwrap();
//! }
//! ```
#![doc(html_root_url="https://docs.rs/tokio-postgres/0.1.1")]
#![warn(missing_docs)]

extern crate fallible_iterator;
extern crate futures;
extern crate futures_state_stream;
extern crate postgres_shared;
extern crate postgres_protocol;
extern crate tokio_core;
extern crate tokio_dns;
extern crate tokio_uds;

#[cfg(feature = "tokio-openssl")]
extern crate tokio_openssl;
#[cfg(feature = "openssl")]
extern crate openssl;

use fallible_iterator::FallibleIterator;
use futures::{Future, IntoFuture, BoxFuture, Stream, Sink, Poll, StartSend};
use futures::future::Either;
use futures_state_stream::{StreamEvent, StateStream, BoxStateStream, FutureExt};
use postgres_protocol::authentication;
use postgres_protocol::message::{backend, frontend};
use postgres_protocol::message::backend::{ErrorResponseBody, ErrorFields};
use postgres_shared::rows::RowData;
use std::collections::HashMap;
use std::fmt;
use std::io;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, ATOMIC_USIZE_INIT, Ordering};
use std::sync::mpsc::{self, Sender, Receiver};
use tokio_core::reactor::Handle;

#[doc(inline)]
pub use postgres_shared::{params, CancelData};

use error::{ConnectError, Error, DbError, SqlState};
use params::{ConnectParams, IntoConnectParams};
use stmt::{Statement, Column};
use stream::PostgresStream;
use tls::Handshake;
use transaction::Transaction;
use types::{Oid, Type, ToSql, IsNull, FromSql, Other, Kind, Field};
use rows::Row;

pub mod error;
pub mod rows;
pub mod stmt;
mod stream;
pub mod tls;
pub mod transaction;
#[macro_use]
pub mod types;

#[cfg(test)]
mod test;

const TYPEINFO_QUERY: &'static str = "__typeinfo";
const TYPEINFO_ENUM_QUERY: &'static str = "__typeinfo_enum";
const TYPEINFO_COMPOSITE_QUERY: &'static str = "__typeinfo_composite";

static NEXT_STMT_ID: AtomicUsize = ATOMIC_USIZE_INIT;

/// Specifies the TLS support required for a new connection.
pub enum TlsMode {
    /// The connection must use TLS.
    Require(Box<Handshake>),
    /// The connection will use TLS if available.
    Prefer(Box<Handshake>),
    /// The connection will not use TLS.
    None,
}

/// Attempts to cancel an in-progress query.
///
/// The backend provides no information about whether a cancellation attempt
/// was successful or not. An error will only be returned if the driver was
/// unable to connect to the database.
///
/// A `CancelData` object can be created via `Connection::cancel_data`. The
/// object can cancel any query made on that connection.
///
/// Only the host and port of the connection info are used. See
/// `Connection::connect` for details of the `params` argument.
pub fn cancel_query<T>(params: T,
                       tls_mode: TlsMode,
                       cancel_data: CancelData,
                       handle: &Handle)
                       -> BoxFuture<(), ConnectError>
    where T: IntoConnectParams
{
    let params = match params.into_connect_params() {
        Ok(params) => {
            Either::A(stream::connect(params.host().clone(),
                                      params.port(),
                                      tls_mode,
                                      handle))
        }
        Err(e) => Either::B(Err(ConnectError::ConnectParams(e)).into_future())
    };

    params.and_then(move |c| {
            let mut buf = vec![];
            frontend::cancel_request(cancel_data.process_id, cancel_data.secret_key, &mut buf);
            c.send(buf).map_err(ConnectError::Io)
        })
        .map(|_| ())
        .boxed()
}

struct InnerConnection {
    stream: PostgresStream,
    close_receiver: Receiver<(u8, String)>,
    close_sender: Sender<(u8, String)>,
    parameters: HashMap<String, String>,
    types: HashMap<Oid, Other>,
    cancel_data: CancelData,
    has_typeinfo_query: bool,
    has_typeinfo_enum_query: bool,
    has_typeinfo_composite_query: bool,
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

/// A connection to a Postgres database.
pub struct Connection(InnerConnection);

// FIXME fill out
impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Connection")
            .finish()
    }
}

impl Connection {
    /// Creates a new connection to a Postgres database.
    ///
    /// Most applications can use a URL string in the normal format:
    ///
    /// ```notrust
    /// postgresql://user[:password]@host[:port][/database][?param1=val1[[&param2=val2]...]]
    /// ```
    ///
    /// The password may be omitted if not required. The default Postgres port
    /// (5432) is used if none is specified. The database name defaults to the
    /// username if not specified.
    ///
    /// To connect to the server via Unix sockets, `host` should be set to the
    /// absolute path of the directory containing the socket file.  Since `/` is
    /// a reserved character in URLs, the path should be URL encoded. If the
    /// path contains non-UTF 8 characters, a `ConnectParams` struct should be
    /// created manually and passed in. Note that Postgres does not support TLS
    /// over Unix sockets.
    pub fn connect<T>(params: T,
                      tls_mode: TlsMode,
                      handle: &Handle)
                      -> BoxFuture<Connection, ConnectError>
        where T: IntoConnectParams
    {
        let fut = match params.into_connect_params() {
            Ok(params) => {
                Either::A(stream::connect(params.host().clone(),
                                          params.port(),
                                          tls_mode,
                                          handle)
                    .map(|s| (s, params)))
            }
            Err(e) => Either::B(Err(ConnectError::ConnectParams(e)).into_future())
        };

        fut.map(|(s, params)| {
                let (sender, receiver) = mpsc::channel();
                (Connection(InnerConnection {
                    stream: s,
                    close_sender: sender,
                    close_receiver: receiver,
                    parameters: HashMap::new(),
                    types: HashMap::new(),
                    cancel_data: CancelData {
                        process_id: 0,
                        secret_key: 0,
                    },
                    has_typeinfo_query: false,
                    has_typeinfo_enum_query: false,
                    has_typeinfo_composite_query: false,
                }), params)
            })
            .and_then(|(s, params)| s.startup(params))
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
            .into_future()
            .and_then(move |()| self.0.send(buf))
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

    /// Execute a sequence of SQL statements.
    ///
    /// Statements should be separated by `;` characters. If an error occurs,
    /// execution of the sequence will stop at that point. This is intended for
    /// execution of batches of non-dynamic statements - for example, creation
    /// of a schema for a fresh database.
    ///
    /// # Warning
    ///
    /// Prepared statements should be used for any SQL statement which contains
    /// user-specified data, as it provides functionality to safely embed that
    /// data in the statement. Do not form statements via string concatenation
    /// and feed them into this method.
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
                            |f, t| Column::new(f.0, t))
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

        let other = self.0.types.get(&oid).map(Clone::clone);
        if let Some(other) = other {
            return Ok((Type::Other(other), self)).into_future().boxed();
        }

        self.get_unknown_type(oid)
            .map(move |(ty, mut c)| {
                c.0.types.insert(oid, ty.clone());
                (Type::Other(ty), c)
            })
            .boxed()
    }

    fn get_unknown_type(self, oid: Oid) -> BoxFuture<(Other, Connection), Error> {
        self.setup_typeinfo_query()
            .and_then(move |c| c.raw_execute(TYPEINFO_QUERY, "", &[Type::Oid], &[&oid]))
            .and_then(|c| c.read_rows().collect())
            .and_then(move |(r, c)| {
                let get = |idx| r.get(0).and_then(|r| r.get(idx));

                let name = match String::from_sql_nullable(&Type::Name, get(0)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err(Error::Conversion(e, c)).into_future()),
                };
                let type_ = match i8::from_sql_nullable(&Type::Char, get(1)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err(Error::Conversion(e, c)).into_future()),
                };
                let elem_oid = match Oid::from_sql_nullable(&Type::Oid, get(2)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err(Error::Conversion(e, c)).into_future()),
                };
                let rngsubtype = match Option::<Oid>::from_sql_nullable(&Type::Oid, get(3)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err(Error::Conversion(e, c)).into_future()),
                };
                let basetype = match Oid::from_sql_nullable(&Type::Oid, get(4)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err(Error::Conversion(e, c)).into_future()),
                };
                let schema = match String::from_sql_nullable(&Type::Name, get(5)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err(Error::Conversion(e, c)).into_future()),
                };
                let relid = match Oid::from_sql_nullable(&Type::Oid, get(6)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err(Error::Conversion(e, c)).into_future()),
                };

                let kind = if type_ == b'p' as i8 {
                    Either::A(Ok((Kind::Pseudo, c)).into_future())
                } else if type_ == b'e' as i8 {
                    Either::B(c.get_enum_variants(oid).map(|(v, c)| (Kind::Enum(v), c)).boxed())
                } else if basetype != 0 {
                    Either::B(c.get_type(basetype).map(|(t, c)| (Kind::Domain(t), c)).boxed())
                } else if elem_oid != 0 {
                    Either::B(c.get_type(elem_oid).map(|(t, c)| (Kind::Array(t), c)).boxed())
                } else if relid != 0 {
                    Either::B(c.get_composite_fields(relid).map(|(f, c)| (Kind::Composite(f), c)).boxed())
                } else if let Some(rngsubtype) = rngsubtype {
                    Either::B(c.get_type(rngsubtype).map(|(t, c)| (Kind::Range(t), c)).boxed())
                } else {
                    Either::A(Ok((Kind::Simple, c)).into_future())
                };

                Either::B(kind.map(move |(k, c)| (Other::new(name, oid, k, schema), c)))
            })
            .boxed()
    }

    fn setup_typeinfo_query(self) -> BoxFuture<Connection, Error> {
        if self.0.has_typeinfo_query {
            return Ok(self).into_future().boxed();
        }

        self.raw_prepare(TYPEINFO_QUERY,
                         "SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, \
                                 t.typbasetype, n.nspname, t.typrelid \
                          FROM pg_catalog.pg_type t \
                          LEFT OUTER JOIN pg_catalog.pg_range r ON \
                              r.rngtypid = t.oid \
                          INNER JOIN pg_catalog.pg_namespace n ON \
                              t.typnamespace = n.oid \
                          WHERE t.oid = $1")
            .or_else(|e| {
                match e {
                    // Range types weren't added until Postgres 9.2, so pg_range may not exist
                    Error::Db(e, c) => {
                        if e.code != SqlState::UndefinedTable {
                            return Either::B(Err(Error::Db(e, c)).into_future());
                        }

                        Either::A(c.raw_prepare(TYPEINFO_QUERY,
                                                "SELECT t.typname, t.typtype, t.typelem, \
                                                     NULL::OID, t.typbasetype, n.nspname, \
                                                     t.typrelid \
                                                 FROM pg_catalog.pg_type t \
                                                 INNER JOIN pg_catalog.pg_namespace n \
                                                     ON t.typnamespace = n.oid \
                                                 WHERE t.oid = $1"))
                    }
                    e => Either::B(Err(e).into_future()),
                }
            })
            .map(|(_, _, mut c)| {
                c.0.has_typeinfo_query = true;
                c
            })
            .boxed()
    }

    fn get_enum_variants(self, oid: Oid) -> BoxFuture<(Vec<String>, Connection), Error> {
        self.setup_typeinfo_enum_query()
            .and_then(move |c| c.raw_execute(TYPEINFO_ENUM_QUERY, "", &[Type::Oid], &[&oid]))
            .and_then(|c| c.read_rows().collect())
            .and_then(|(r, c)| {
                let mut variants = vec![];
                for row in r {
                    let variant = match String::from_sql_nullable(&Type::Name, row.get(0)) {
                        Ok(v) => v,
                        Err(e) => return Err(Error::Conversion(e, c)),
                    };
                    variants.push(variant);
                }
                Ok((variants, c))
            })
            .boxed()
    }

    fn setup_typeinfo_enum_query(self) -> BoxFuture<Connection, Error> {
        if self.0.has_typeinfo_enum_query {
            return Ok(self).into_future().boxed();
        }

        self.raw_prepare(TYPEINFO_ENUM_QUERY,
                         "SELECT enumlabel \
                          FROM pg_catalog.pg_enum \
                          WHERE enumtypid = $1 \
                          ORDER BY enumsortorder")
            .or_else(|e| {
                match e {
                    Error::Db(e, c) => {
                        if e.code != SqlState::UndefinedColumn {
                            return Either::B(Err(Error::Db(e, c)).into_future());
                        }

                        Either::A(c.raw_prepare(TYPEINFO_ENUM_QUERY,
                                                "SELECT enumlabel \
                                                 FROM pg_catalog.pg_enum \
                                                 WHERE enumtypid = $1 \
                                                 ORDER BY oid"))
                    }
                    e => Either::B(Err(e).into_future()),
                }
            })
            .map(|(_, _, mut c)| {
                c.0.has_typeinfo_enum_query = true;
                c
            })
            .boxed()
    }

    fn get_composite_fields(self, oid: Oid) -> BoxFuture<(Vec<Field>, Connection), Error> {
        self.setup_typeinfo_composite_query()
            .and_then(move |c| c.raw_execute(TYPEINFO_COMPOSITE_QUERY, "", &[Type::Oid], &[&oid]))
            .and_then(|c| c.read_rows().collect())
            .and_then(|(r, c)| {
                futures::stream::iter(r.into_iter().map(Ok))
                    .fold((vec![], c), |(mut fields, c), row| {
                        let name = match String::from_sql_nullable(&Type::Name, row.get(0)) {
                            Ok(name) => name,
                            Err(e) => return Either::A(Err(Error::Conversion(e, c)).into_future()),
                        };
                        let oid = match Oid::from_sql_nullable(&Type::Oid, row.get(1)) {
                            Ok(oid) => oid,
                            Err(e) => return Either::A(Err(Error::Conversion(e, c)).into_future()),
                        };
                        Either::B(c.get_type(oid)
                            .map(move |(ty, c)| {
                                fields.push(Field::new(name, ty));
                                (fields, c)
                            }))
                    })
            })
            .boxed()
    }

    fn setup_typeinfo_composite_query(self) -> BoxFuture<Connection, Error> {
        if self.0.has_typeinfo_composite_query {
            return Ok(self).into_future().boxed();
        }

        self.raw_prepare(TYPEINFO_COMPOSITE_QUERY,
                         "SELECT attname, atttypid \
                          FROM pg_catalog.pg_attribute \
                          WHERE attrelid = $1 \
                              AND NOT attisdropped \
                              AND attnum > 0 \
                          ORDER BY attnum")
            .map(|(_, _, mut c)| {
                c.0.has_typeinfo_composite_query = true;
                c
            })
            .boxed()
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
                                   match param.to_sql_checked(ty, buf) {
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

    fn read_rows(self) -> BoxStateStream<RowData, Connection, Error> {
        futures_state_stream::unfold(self, |c| {
            c.read_row()
                .and_then(|(r, c)| {
                    match r {
                        Some(data) => {
                            let event = StreamEvent::Next((data, c));
                            Either::A(Ok(event).into_future())
                        },
                        None => Either::B(c.ready(()).map(|((), c)| StreamEvent::Done(c))),
                    }
                })
        }).boxed()
    }

    fn read_row(self) -> BoxFuture<(Option<RowData>, Connection), Error> {
        self.0.read()
            .map_err(Error::Io)
            .and_then(|(m, s)| {
                let c = Connection(s);
                match m {
                    backend::Message::DataRow(body) => {
                        Either::A(body.values()
                            .collect()
                            .map(|r| (Some(r), c))
                            .map_err(Error::Io)
                            .into_future())
                    }
                    backend::Message::EmptyQueryResponse |
                    backend::Message::CommandComplete(_) => Either::A(Ok((None, c)).into_future()),
                    backend::Message::ErrorResponse(body) => {
                        Either::B(c.ready_err(body))
                    }
                    _ => Either::A(Err(bad_message()).into_future()),
                }
            })
            .boxed()
    }

    /// Creates a new prepared statement.
    pub fn prepare(self, query: &str) -> BoxFuture<(Statement, Connection), Error> {
        let id = NEXT_STMT_ID.fetch_add(1, Ordering::SeqCst);
        let name = format!("s{}", id);
        self.raw_prepare(&name, query)
            .map(|(params, columns, conn)| {
                let stmt = Statement::new(conn.0.close_sender.clone(),
                                          name,
                                          params,
                                          Arc::new(columns));
                (stmt, conn)
            })
            .boxed()
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    pub fn execute(self, statement: &Statement, params: &[&ToSql]) -> BoxFuture<(u64, Connection), Error> {
        self.raw_execute(statement.name(), "", statement.parameters(), params)
            .and_then(|conn| conn.finish_execute())
            .boxed()
    }

    /// Executes a statement, returning a stream over the resulting rows.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    pub fn query(self,
                 statement: &Statement,
                 params: &[&ToSql])
                 -> BoxStateStream<Row, Connection, Error> {
        let columns = statement.columns_arc().clone();
        self.raw_execute(statement.name(), "", statement.parameters(), params)
            .map(|c| c.read_rows().map(move |r| Row::new(columns.clone(), r)))
            .flatten_state_stream()
            .boxed()
    }

    /// Starts a new transaction.
    pub fn transaction(self) -> BoxFuture<Transaction, Error> {
        self.simple_query("BEGIN")
            .map(|(_, c)| Transaction::new(c))
            .boxed()
    }

    /// Returns information used to cancel pending queries.
    ///
    /// Used with the `cancel_query` function. The object returned can be used
    /// to cancel any query executed by the connection it was created from.
    pub fn cancel_data(&self) -> CancelData {
        self.0.cancel_data
    }

    /// Returns the value of the specified Postgres backend parameter, such as
    /// `timezone` or `server_version`.
    pub fn parameter(&self, param: &str) -> Option<&str> {
        self.0.parameters.get(param).map(|s| &**s)
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

trait RowNew {
    fn new(columns: Arc<Vec<Column>>, data: RowData) -> Row;
}

trait StatementNew {
    fn new(close_sender: Sender<(u8, String)>,
           name: String,
           params: Vec<Type>,
           columns: Arc<Vec<Column>>)
           -> Statement;

    fn columns_arc(&self) -> &Arc<Vec<Column>>;

    fn name(&self) -> &str;
}

trait TransactionNew {
    fn new(c: Connection) -> Transaction;
}

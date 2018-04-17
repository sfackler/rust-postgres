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
//!     let done = Connection::connect("postgresql://postgres@localhost:5433",
//!                                    TlsMode::None,
//!                                    &l.handle())
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
//!                 })
//!         });
//!
//!     l.run(done).unwrap();
//! }
//! ```
#![doc(html_root_url = "https://docs.rs/tokio-postgres/0.3")]
#![warn(missing_docs)]

extern crate bytes;
extern crate fallible_iterator;
extern crate futures_state_stream;
extern crate postgres_protocol;
extern crate postgres_shared;
extern crate tokio_core;
extern crate tokio_dns;
extern crate tokio_io;

#[macro_use]
extern crate futures;

#[cfg(unix)]
extern crate tokio_uds;

use fallible_iterator::FallibleIterator;
use futures::future::Either;
use futures::{Async, Future, IntoFuture, Poll, Sink, StartSend, Stream};
use futures_state_stream::{FutureExt, StateStream, StreamEvent};
use postgres_protocol::authentication;
use postgres_protocol::message::backend::{ErrorFields, ErrorResponseBody};
use postgres_protocol::message::{backend, frontend};
use postgres_shared::rows::RowData;
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io;
use std::sync::atomic::{AtomicUsize, Ordering, ATOMIC_USIZE_INIT};
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::Arc;
use tokio_core::reactor::Handle;

#[doc(inline)]
pub use error::Error;
#[doc(inline)]
pub use postgres_shared::{error, params, types, CancelData, Notification};

use error::{DbError, UNDEFINED_COLUMN, UNDEFINED_TABLE};
use params::{ConnectParams, IntoConnectParams};
use rows::Row;
use sink::SinkExt;
use stmt::{Column, Statement};
use stream::PostgresStream;
use tls::Handshake;
use transaction::Transaction;
use types::{Field, FromSql, IsNull, Kind, Oid, ToSql, Type, CHAR, NAME, OID};

#[macro_use]
mod macros;
pub mod rows;
mod sink;
pub mod stmt;
mod stream;
pub mod tls;
pub mod transaction;

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
pub fn cancel_query<T>(
    params: T,
    tls_mode: TlsMode,
    cancel_data: CancelData,
    handle: &Handle,
) -> Box<Future<Item = (), Error = Error> + Send>
where
    T: IntoConnectParams,
{
    let params = match params.into_connect_params() {
        Ok(params) => Either::A(stream::connect(
            params.host().clone(),
            params.port(),
            params.keepalive(),
            tls_mode,
            handle,
        )),
        Err(e) => Either::B(Err(error::connect(e)).into_future()),
    };

    params
        .and_then(move |c| {
            let mut buf = vec![];
            frontend::cancel_request(cancel_data.process_id, cancel_data.secret_key, &mut buf);
            c.send(buf).map_err(error::io)
        })
        .map(|_| ())
        .boxed2()
}

struct InnerConnection {
    stream: PostgresStream,
    close_receiver: Receiver<(u8, String)>,
    close_sender: Sender<(u8, String)>,
    parameters: HashMap<String, String>,
    types: HashMap<Oid, Type>,
    notifications: VecDeque<Notification>,
    cancel_data: CancelData,
    has_typeinfo_query: bool,
    has_typeinfo_enum_query: bool,
    has_typeinfo_composite_query: bool,
    desynchronized: bool,
}

impl InnerConnection {
    fn read(
        self,
    ) -> Box<
        Future<Item = (backend::Message, InnerConnection), Error = (io::Error, InnerConnection)>
            + Send,
    > {
        if self.desynchronized {
            let e = io::Error::new(
                io::ErrorKind::Other,
                "connection desynchronized due to earlier IO error",
            );
            return Err((e, self)).into_future().boxed2();
        }

        self.into_future()
            .and_then(|(m, mut s)| match m {
                Some(backend::Message::NotificationResponse(body)) => {
                    let process_id = body.process_id();
                    let channel = match body.channel() {
                        Ok(channel) => channel.to_owned(),
                        Err(e) => return Either::A(Err((e, s)).into_future()),
                    };
                    let message = match body.message() {
                        Ok(channel) => channel.to_owned(),
                        Err(e) => return Either::A(Err((e, s)).into_future()),
                    };
                    let notification = Notification {
                        process_id: process_id,
                        channel: channel,
                        payload: message,
                    };
                    s.notifications.push_back(notification);
                    Either::B(s.read())
                }
                Some(m) => Either::A(Ok((m, s)).into_future()),
                None => {
                    let err = io::Error::new(io::ErrorKind::UnexpectedEof, "unexpected EOF");
                    Either::A(Err((err, s)).into_future())
                }
            })
            .map_err(|(e, mut s)| {
                s.desynchronized = true;
                (e, s)
            })
            .boxed2()
    }
}

impl Stream for InnerConnection {
    type Item = backend::Message;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<backend::Message>, io::Error> {
        loop {
            match try_ready!(self.stream.poll()) {
                Some(backend::Message::ParameterStatus(body)) => {
                    let name = body.name()?.to_owned();
                    let value = body.value()?.to_owned();
                    self.parameters.insert(name, value);
                }
                // TODO forward to a handler
                Some(backend::Message::NoticeResponse(_)) => {}
                msg => return Ok(Async::Ready(msg)),
            }
        }
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
        fmt.debug_struct("Connection").finish()
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
    pub fn connect<T>(
        params: T,
        tls_mode: TlsMode,
        handle: &Handle,
    ) -> Box<Future<Item = Connection, Error = Error> + Send>
    where
        T: IntoConnectParams,
    {
        let fut = match params.into_connect_params() {
            Ok(params) => Either::A(
                stream::connect(
                    params.host().clone(),
                    params.port(),
                    params.keepalive(),
                    tls_mode,
                    handle,
                ).map(|s| (s, params)),
            ),
            Err(e) => Either::B(Err(error::connect(e)).into_future()),
        };

        fut.map(|(s, params)| {
            let (sender, receiver) = mpsc::channel();
            (
                Connection(InnerConnection {
                    stream: s,
                    close_sender: sender,
                    close_receiver: receiver,
                    parameters: HashMap::new(),
                    types: HashMap::new(),
                    notifications: VecDeque::new(),
                    cancel_data: CancelData {
                        process_id: 0,
                        secret_key: 0,
                    },
                    has_typeinfo_query: false,
                    has_typeinfo_enum_query: false,
                    has_typeinfo_composite_query: false,
                    desynchronized: false,
                }),
                params,
            )
        }).and_then(|(s, params)| s.startup(params))
            .and_then(|(s, params)| s.handle_auth(params))
            .and_then(|s| s.finish_startup())
            .boxed2()
    }

    fn startup(
        self,
        params: ConnectParams,
    ) -> Box<Future<Item = (Connection, ConnectParams), Error = Error> + Send> {
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
            .map_err(error::io)
            .map(move |s| (Connection(s), params))
            .boxed2()
    }

    fn handle_auth(
        self,
        params: ConnectParams,
    ) -> Box<Future<Item = Connection, Error = Error> + Send> {
        self.0
            .read()
            .map_err(|(e, _)| error::io(e))
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
                            None => Err(error::connect(
                                "password was required but not provided".into(),
                            )),
                        }
                    }
                    backend::Message::AuthenticationMd5Password(body) => match params
                        .user()
                        .and_then(|u| u.password().map(|p| (u.name(), p)))
                    {
                        Some((user, pass)) => {
                            let pass = authentication::md5_hash(
                                user.as_bytes(),
                                pass.as_bytes(),
                                body.salt(),
                            );
                            let mut buf = vec![];
                            frontend::password_message(&pass, &mut buf)
                                .map(|()| Some(buf))
                                .map_err(Into::into)
                        }
                        None => Err(error::connect(
                            "password was required but not provided".into(),
                        )),
                    },
                    backend::Message::ErrorResponse(body) => Err(err(&mut body.fields())),
                    _ => Err(bad_message()),
                };

                response.map(|m| (m, Connection(s)))
            })
            .and_then(|(m, s)| match m {
                Some(m) => Either::A(s.handle_auth_response(m)),
                None => Either::B(Ok(s).into_future()),
            })
            .boxed2()
    }

    fn handle_auth_response(
        self,
        message: Vec<u8>,
    ) -> Box<Future<Item = Connection, Error = Error> + Send> {
        self.0
            .send(message)
            .and_then(|s| s.read().map_err(|(e, _)| e))
            .map_err(error::io)
            .and_then(|(m, s)| match m {
                backend::Message::AuthenticationOk => Ok(Connection(s)),
                backend::Message::ErrorResponse(body) => Err(err(&mut body.fields())),
                _ => Err(bad_message()),
            })
            .boxed2()
    }

    fn finish_startup(self) -> Box<Future<Item = Connection, Error = Error> + Send> {
        self.0
            .read()
            .map_err(|(e, _)| error::io(e))
            .and_then(|(m, mut s)| match m {
                backend::Message::BackendKeyData(body) => {
                    s.cancel_data.process_id = body.process_id();
                    s.cancel_data.secret_key = body.secret_key();
                    Either::A(Connection(s).finish_startup())
                }
                backend::Message::ReadyForQuery(_) => Either::B(Ok(Connection(s)).into_future()),
                backend::Message::ErrorResponse(body) => {
                    Either::B(Err(err(&mut body.fields())).into_future())
                }
                _ => Either::B(Err(bad_message()).into_future()),
            })
            .boxed2()
    }

    fn simple_query(
        self,
        query: &str,
    ) -> Box<Future<Item = (Vec<RowData>, Connection), Error = (Error, Connection)> + Send> {
        let mut buf = vec![];
        if let Err(e) = frontend::query(query, &mut buf) {
            return Err((error::io(e), self)).into_future().boxed2();
        }

        self.0
            .send2(buf)
            .map_err(|(e, s)| (error::io(e), Connection(s)))
            .and_then(|s| Connection(s).simple_read_rows(vec![]))
            .boxed2()
    }

    // This has its own read_rows since it will need to handle multiple query completions
    fn simple_read_rows(
        self,
        mut rows: Vec<RowData>,
    ) -> Box<Future<Item = (Vec<RowData>, Connection), Error = (Error, Connection)> + Send> {
        self.0
            .read()
            .map_err(|(e, s)| (error::io(e), Connection(s)))
            .and_then(|(m, s)| match m {
                backend::Message::ReadyForQuery(_) => {
                    Ok((rows, Connection(s))).into_future().boxed2()
                }
                backend::Message::DataRow(body) => match RowData::new(body) {
                    Ok(row) => {
                        rows.push(row);
                        Connection(s).simple_read_rows(rows)
                    }
                    Err(e) => Err((error::io(e), Connection(s))).into_future().boxed2(),
                },
                backend::Message::EmptyQueryResponse
                | backend::Message::CommandComplete(_)
                | backend::Message::RowDescription(_) => Connection(s).simple_read_rows(rows),
                backend::Message::ErrorResponse(body) => Connection(s).ready_err(body),
                _ => Err((bad_message(), Connection(s))).into_future().boxed2(),
            })
            .boxed2()
    }

    fn ready<T>(
        self,
        t: T,
    ) -> Box<Future<Item = (T, Connection), Error = (Error, Connection)> + Send>
    where
        T: 'static + Send,
    {
        self.0
            .read()
            .map_err(|(e, s)| (error::io(e), Connection(s)))
            .and_then(|(m, s)| match m {
                backend::Message::ReadyForQuery(_) => Ok(s),
                _ => Err((bad_message(), Connection(s))),
            })
            .and_then(|s| Connection(s).close_gc().map(|s| (t, s)))
            .boxed2()
    }

    fn close_gc(self) -> Box<Future<Item = Connection, Error = (Error, Connection)> + Send> {
        let mut messages = vec![];
        while let Ok((type_, name)) = self.0.close_receiver.try_recv() {
            let mut buf = vec![];
            frontend::close(type_, &name, &mut buf).unwrap(); // this can only fail on bad names
            messages.push(buf);
        }
        if messages.is_empty() {
            return Ok(self).into_future().boxed2();
        }

        let mut buf = vec![];
        frontend::sync(&mut buf);
        messages.push(buf);
        self.0
            .send_all2(futures::stream::iter_ok::<_, Error>(messages))
            .map_err(|(e, s, _)| (error::io(e), Connection(s)))
            .and_then(|s| Connection(s.0).finish_close_gc())
            .boxed2()
    }

    fn finish_close_gc(self) -> Box<Future<Item = Connection, Error = (Error, Connection)> + Send> {
        self.0
            .read()
            .map_err(|(e, s)| (error::io(e), Connection(s)))
            .and_then(|(m, s)| match m {
                backend::Message::ReadyForQuery(_) => Either::A(Ok(Connection(s)).into_future()),
                backend::Message::CloseComplete => Either::B(Connection(s).finish_close_gc()),
                backend::Message::ErrorResponse(body) => Either::B(Connection(s).ready_err(body)),
                _ => Either::A(Err((bad_message(), Connection(s))).into_future()),
            })
            .boxed2()
    }

    fn ready_err<T>(
        self,
        body: ErrorResponseBody,
    ) -> Box<Future<Item = T, Error = (Error, Connection)> + Send>
    where
        T: 'static + Send,
    {
        let e = match DbError::new(&mut body.fields()) {
            Ok(e) => e,
            Err(e) => return Err((error::io(e), self)).into_future().boxed2(),
        };
        self.ready(e)
            .and_then(|(e, s)| Err((error::db(e), s)))
            .boxed2()
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
    pub fn batch_execute(
        self,
        query: &str,
    ) -> Box<Future<Item = Connection, Error = (Error, Connection)> + Send> {
        self.simple_query(query).map(|r| r.1).boxed2()
    }

    fn raw_prepare(
        self,
        name: &str,
        query: &str,
    ) -> Box<Future<Item = (Vec<Type>, Vec<Column>, Connection), Error = (Error, Connection)> + Send>
    {
        let mut parse = vec![];
        let mut describe = vec![];
        let mut sync = vec![];
        frontend::sync(&mut sync);
        if let Err(e) = frontend::parse(name, query, None, &mut parse)
            .and_then(|()| frontend::describe(b'S', name, &mut describe))
        {
            return Err((error::io(e), self)).into_future().boxed2();
        }

        let it = Some(parse)
            .into_iter()
            .chain(Some(describe))
            .chain(Some(sync));
        self.0.send_all2(futures::stream::iter_ok::<_, io::Error>(it))
            .map_err(|(e, s, _)| (e, s))
            .and_then(|s| s.0.read())
            .map_err(|(e, s)| (error::io(e), Connection(s)))
            .boxed2() // work around nonlinear trans blowup
            .and_then(|(m, s)| {
                match m {
                    backend::Message::ParseComplete => Either::A(Ok(s).into_future()),
                    backend::Message::ErrorResponse(body) => {
                        Either::B(Connection(s).ready_err(body))
                    }
                    _ => Either::A(Err((bad_message(), Connection(s))).into_future()),
                }
            })
            .and_then(|s| s.read().map_err(|(e, s)| (error::io(e), Connection(s))))
            .and_then(|(m, s)| {
                match m {
                    backend::Message::ParameterDescription(body) => {
                        match body.parameters().collect::<Vec<_>>() {
                            Ok(p) => Ok((p, s)),
                            Err(e) => Err((error::io(e), Connection(s))),
                        }
                    }
                    _ => Err((bad_message(), Connection(s))),
                }
            })
            .and_then(|(p, s)| s.read().map(|(m, s)| (p, m, s)).map_err(|(e, s)| (error::io(e), Connection(s))))
            .boxed2() // work around nonlinear trans blowup
            .and_then(|(p, m, s)| {
                match m {
                    backend::Message::RowDescription(body) => {
                        match body.fields()
                            .map(|f| (f.name().to_owned(), f.type_oid()))
                            .collect::<Vec<_>>() {
                                Ok(d) => Ok((p, d, s)),
                                Err(e) => Err((error::io(e), Connection(s))),
                            }
                    }
                    backend::Message::NoData => Ok((p, vec![], s)),
                    _ => Err((bad_message(), Connection(s))),
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
            .boxed2()
    }

    fn get_types<T, U, I, F, G>(
        self,
        mut raw: I,
        mut out: Vec<U>,
        mut get_oid: F,
        mut build: G,
    ) -> Box<Future<Item = (Vec<U>, Connection), Error = (Error, Connection)> + Send>
    where
        T: 'static + Send,
        U: 'static + Send,
        I: 'static + Send + Iterator<Item = T>,
        F: 'static + Send + FnMut(&T) -> Oid,
        G: 'static + Send + FnMut(T, Type) -> U,
    {
        match raw.next() {
            Some(v) => {
                let oid = get_oid(&v);
                self.get_type(oid)
                    .and_then(move |(ty, s)| {
                        out.push(build(v, ty));
                        s.get_types(raw, out, get_oid, build)
                    })
                    .boxed2()
            }
            None => Ok((out, self)).into_future().boxed2(),
        }
    }

    fn get_type(
        self,
        oid: Oid,
    ) -> Box<Future<Item = (Type, Connection), Error = (Error, Connection)> + Send> {
        if let Some(type_) = Type::from_oid(oid) {
            return Ok((type_, self)).into_future().boxed2();
        };

        let ty = self.0.types.get(&oid).map(Clone::clone);
        if let Some(ty) = ty {
            return Ok((ty, self)).into_future().boxed2();
        }

        self.get_unknown_type(oid)
            .map(move |(ty, mut c)| {
                c.0.types.insert(oid, ty.clone());
                (ty, c)
            })
            .boxed2()
    }

    fn get_unknown_type(
        self,
        oid: Oid,
    ) -> Box<Future<Item = (Type, Connection), Error = (Error, Connection)> + Send> {
        self.setup_typeinfo_query()
            .and_then(move |c| c.raw_execute(TYPEINFO_QUERY, "", &[OID], &[&oid]))
            .and_then(|c| c.read_rows().collect())
            .and_then(move |(r, c)| {
                let get = |idx| r.get(0).and_then(|r| r.get(idx));

                let name = match String::from_sql_nullable(&NAME, get(0)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err((error::conversion(e), c)).into_future()),
                };
                let type_ = match i8::from_sql_nullable(&CHAR, get(1)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err((error::conversion(e), c)).into_future()),
                };
                let elem_oid = match Oid::from_sql_nullable(&OID, get(2)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err((error::conversion(e), c)).into_future()),
                };
                let rngsubtype = match Option::<Oid>::from_sql_nullable(&OID, get(3)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err((error::conversion(e), c)).into_future()),
                };
                let basetype = match Oid::from_sql_nullable(&OID, get(4)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err((error::conversion(e), c)).into_future()),
                };
                let schema = match String::from_sql_nullable(&NAME, get(5)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err((error::conversion(e), c)).into_future()),
                };
                let relid = match Oid::from_sql_nullable(&OID, get(6)) {
                    Ok(v) => v,
                    Err(e) => return Either::A(Err((error::conversion(e), c)).into_future()),
                };

                let kind = if type_ == b'p' as i8 {
                    Either::A(Ok((Kind::Pseudo, c)).into_future())
                } else if type_ == b'e' as i8 {
                    Either::B(
                        c.get_enum_variants(oid)
                            .map(|(v, c)| (Kind::Enum(v), c))
                            .boxed2(),
                    )
                } else if basetype != 0 {
                    Either::B(
                        c.get_type(basetype)
                            .map(|(t, c)| (Kind::Domain(t), c))
                            .boxed2(),
                    )
                } else if elem_oid != 0 {
                    Either::B(
                        c.get_type(elem_oid)
                            .map(|(t, c)| (Kind::Array(t), c))
                            .boxed2(),
                    )
                } else if relid != 0 {
                    Either::B(
                        c.get_composite_fields(relid)
                            .map(|(f, c)| (Kind::Composite(f), c))
                            .boxed2(),
                    )
                } else if let Some(rngsubtype) = rngsubtype {
                    Either::B(
                        c.get_type(rngsubtype)
                            .map(|(t, c)| (Kind::Range(t), c))
                            .boxed2(),
                    )
                } else {
                    Either::A(Ok((Kind::Simple, c)).into_future())
                };

                Either::B(kind.map(move |(k, c)| (Type::_new(name, oid, k, schema), c)))
            })
            .boxed2()
    }

    fn setup_typeinfo_query(
        self,
    ) -> Box<Future<Item = Connection, Error = (Error, Connection)> + Send> {
        if self.0.has_typeinfo_query {
            return Ok(self).into_future().boxed2();
        }

        self.raw_prepare(
            TYPEINFO_QUERY,
            "SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, \
             t.typbasetype, n.nspname, t.typrelid \
             FROM pg_catalog.pg_type t \
             LEFT OUTER JOIN pg_catalog.pg_range r ON \
             r.rngtypid = t.oid \
             INNER JOIN pg_catalog.pg_namespace n ON \
             t.typnamespace = n.oid \
             WHERE t.oid = $1",
        ).or_else(|(e, c)| {
                // Range types weren't added until Postgres 9.2, so pg_range may not exist
                if e.code() == Some(&UNDEFINED_TABLE) {
                    Either::A(c.raw_prepare(
                        TYPEINFO_QUERY,
                        "SELECT t.typname, t.typtype, t.typelem, \
                         NULL::OID, t.typbasetype, n.nspname, \
                         t.typrelid \
                         FROM pg_catalog.pg_type t \
                         INNER JOIN pg_catalog.pg_namespace n \
                         ON t.typnamespace = n.oid \
                         WHERE t.oid = $1",
                    ))
                } else {
                    Either::B(Err((e, c)).into_future())
                }
            })
            .map(|(_, _, mut c)| {
                c.0.has_typeinfo_query = true;
                c
            })
            .boxed2()
    }

    fn get_enum_variants(
        self,
        oid: Oid,
    ) -> Box<Future<Item = (Vec<String>, Connection), Error = (Error, Connection)> + Send> {
        self.setup_typeinfo_enum_query()
            .and_then(move |c| c.raw_execute(TYPEINFO_ENUM_QUERY, "", &[OID], &[&oid]))
            .and_then(|c| c.read_rows().collect())
            .and_then(|(r, c)| {
                let mut variants = vec![];
                for row in r {
                    let variant = match String::from_sql_nullable(&NAME, row.get(0)) {
                        Ok(v) => v,
                        Err(e) => return Err((error::conversion(e), c)),
                    };
                    variants.push(variant);
                }
                Ok((variants, c))
            })
            .boxed2()
    }

    fn setup_typeinfo_enum_query(
        self,
    ) -> Box<Future<Item = Connection, Error = (Error, Connection)> + Send> {
        if self.0.has_typeinfo_enum_query {
            return Ok(self).into_future().boxed2();
        }

        self.raw_prepare(
            TYPEINFO_ENUM_QUERY,
            "SELECT enumlabel \
             FROM pg_catalog.pg_enum \
             WHERE enumtypid = $1 \
             ORDER BY enumsortorder",
        ).or_else(|(e, c)| {
                if e.code() == Some(&UNDEFINED_COLUMN) {
                    Either::A(c.raw_prepare(
                        TYPEINFO_ENUM_QUERY,
                        "SELECT enumlabel FROM pg_catalog.pg_enum WHERE \
                         enumtypid = $1 ORDER BY oid",
                    ))
                } else {
                    Either::B(Err((e, c)).into_future())
                }
            })
            .map(|(_, _, mut c)| {
                c.0.has_typeinfo_enum_query = true;
                c
            })
            .boxed2()
    }

    fn get_composite_fields(
        self,
        oid: Oid,
    ) -> Box<Future<Item = (Vec<Field>, Connection), Error = (Error, Connection)> + Send> {
        self.setup_typeinfo_composite_query()
            .and_then(move |c| c.raw_execute(TYPEINFO_COMPOSITE_QUERY, "", &[OID], &[&oid]))
            .and_then(|c| c.read_rows().collect())
            .and_then(|(r, c)| {
                futures::stream::iter_ok(r).fold((vec![], c), |(mut fields, c), row| {
                    let name = match String::from_sql_nullable(&NAME, row.get(0)) {
                        Ok(name) => name,
                        Err(e) => return Either::A(Err((error::conversion(e), c)).into_future()),
                    };
                    let oid = match Oid::from_sql_nullable(&OID, row.get(1)) {
                        Ok(oid) => oid,
                        Err(e) => return Either::A(Err((error::conversion(e), c)).into_future()),
                    };
                    Either::B(c.get_type(oid).map(move |(ty, c)| {
                        fields.push(Field::new(name, ty));
                        (fields, c)
                    }))
                })
            })
            .boxed2()
    }

    fn setup_typeinfo_composite_query(
        self,
    ) -> Box<Future<Item = Connection, Error = (Error, Connection)> + Send> {
        if self.0.has_typeinfo_composite_query {
            return Ok(self).into_future().boxed2();
        }

        self.raw_prepare(
            TYPEINFO_COMPOSITE_QUERY,
            "SELECT attname, atttypid \
             FROM pg_catalog.pg_attribute \
             WHERE attrelid = $1 \
             AND NOT attisdropped \
             AND attnum > 0 \
             ORDER BY attnum",
        ).map(|(_, _, mut c)| {
                c.0.has_typeinfo_composite_query = true;
                c
            })
            .boxed2()
    }

    fn raw_execute(
        self,
        stmt: &str,
        portal: &str,
        param_types: &[Type],
        params: &[&ToSql],
    ) -> Box<Future<Item = Connection, Error = (Error, Connection)> + Send> {
        assert!(
            param_types.len() == params.len(),
            "expected {} parameters but got {}",
            param_types.len(),
            params.len()
        );

        let mut bind = vec![];
        let mut execute = vec![];
        let mut sync = vec![];
        frontend::sync(&mut sync);
        let r = frontend::bind(
            portal,
            stmt,
            Some(1),
            params.iter().zip(param_types),
            |(param, ty), buf| match param.to_sql_checked(ty, buf) {
                Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
                Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
                Err(e) => Err(e),
            },
            Some(1),
            &mut bind,
        );
        let r = match r {
            Ok(()) => Ok(self),
            Err(frontend::BindError::Conversion(e)) => Err((error::conversion(e), self)),
            Err(frontend::BindError::Serialization(e)) => Err((error::io(e), self)),
        };

        r.and_then(|s| match frontend::execute(portal, 0, &mut execute) {
            Ok(()) => Ok(s),
            Err(e) => Err((error::io(e), s)),
        }).into_future()
            .and_then(|s| {
                let it = Some(bind)
                    .into_iter()
                    .chain(Some(execute))
                    .chain(Some(sync));
                s.0
                    .send_all2(futures::stream::iter_ok::<_, io::Error>(it))
                    .map_err(|(e, s, _)| (error::io(e), Connection(s)))
            })
            .and_then(|s| s.0.read().map_err(|(e, s)| (error::io(e), Connection(s))))
            .and_then(|(m, s)| match m {
                backend::Message::BindComplete => Either::A(Ok(Connection(s)).into_future()),
                backend::Message::ErrorResponse(body) => Either::B(Connection(s).ready_err(body)),
                _ => Either::A(Err((bad_message(), Connection(s))).into_future()),
            })
            .boxed2()
    }

    fn finish_execute(
        self,
    ) -> Box<Future<Item = (u64, Connection), Error = (Error, Connection)> + Send> {
        self.0
            .read()
            .map_err(|(e, s)| (error::io(e), Connection(s)))
            .and_then(|(m, s)| match m {
                backend::Message::DataRow(_) => Connection(s).finish_execute().boxed2(),
                backend::Message::CommandComplete(body) => {
                    let r = body.tag()
                        .map(|tag| tag.split_whitespace().last().unwrap().parse().unwrap_or(0));

                    match r {
                        Ok(n) => Connection(s).ready(n).boxed2(),
                        Err(e) => Err((error::io(e), Connection(s))).into_future().boxed2(),
                    }
                }
                backend::Message::EmptyQueryResponse => Connection(s).ready(0).boxed2(),
                backend::Message::ErrorResponse(body) => Connection(s).ready_err(body).boxed2(),
                _ => Err((bad_message(), Connection(s))).into_future().boxed2(),
            })
            .boxed2()
    }

    fn read_rows(
        self,
    ) -> Box<StateStream<Item = RowData, State = Connection, Error = Error> + Send> {
        futures_state_stream::unfold(self, |c| {
            c.read_row().and_then(|(r, c)| match r {
                Some(data) => {
                    let event = StreamEvent::Next((data, c));
                    Either::A(Ok(event).into_future())
                }
                None => Either::B(c.ready(()).map(|((), c)| StreamEvent::Done(c))),
            })
        }).boxed2()
    }

    fn read_row(
        self,
    ) -> Box<Future<Item = (Option<RowData>, Connection), Error = (Error, Connection)> + Send> {
        self.0
            .read()
            .map_err(|(e, s)| (error::io(e), Connection(s)))
            .and_then(|(m, s)| {
                let c = Connection(s);
                match m {
                    backend::Message::DataRow(body) => {
                        let r = match RowData::new(body) {
                            Ok(r) => Ok((Some(r), c)),
                            Err(e) => Err((error::io(e), c)),
                        };
                        Either::A(r.into_future())
                    }
                    backend::Message::EmptyQueryResponse | backend::Message::CommandComplete(_) => {
                        Either::A(Ok((None, c)).into_future())
                    }
                    backend::Message::ErrorResponse(body) => Either::B(c.ready_err(body)),
                    _ => Either::A(Err((bad_message(), c)).into_future()),
                }
            })
            .boxed2()
    }

    /// Creates a new prepared statement.
    pub fn prepare(
        self,
        query: &str,
    ) -> Box<Future<Item = (Statement, Connection), Error = (Error, Connection)> + Send> {
        let id = NEXT_STMT_ID.fetch_add(1, Ordering::SeqCst);
        let name = format!("s{}", id);
        self.raw_prepare(&name, query)
            .map(|(params, columns, conn)| {
                let stmt =
                    Statement::new(conn.0.close_sender.clone(), name, params, Arc::new(columns));
                (stmt, conn)
            })
            .boxed2()
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    pub fn execute(
        self,
        statement: &Statement,
        params: &[&ToSql],
    ) -> Box<Future<Item = (u64, Connection), Error = (Error, Connection)> + Send> {
        self.raw_execute(statement.name(), "", statement.parameters(), params)
            .and_then(|conn| conn.finish_execute())
            .boxed2()
    }

    /// Executes a statement, returning a stream over the resulting rows.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    pub fn query(
        self,
        statement: &Statement,
        params: &[&ToSql],
    ) -> Box<StateStream<Item = Row, State = Connection, Error = Error> + Send> {
        let columns = statement.columns_arc().clone();
        self.raw_execute(statement.name(), "", statement.parameters(), params)
            .map(|c| c.read_rows().map(move |r| Row::new(columns.clone(), r)))
            .flatten_state_stream()
            .boxed2()
    }

    /// Starts a new transaction.
    pub fn transaction(
        self,
    ) -> Box<Future<Item = Transaction, Error = (Error, Connection)> + Send> {
        self.simple_query("BEGIN")
            .map(|(_, c)| Transaction::new(c))
            .boxed2()
    }

    /// Returns a stream of asynchronus notifications receieved from the server.
    pub fn notifications(self) -> Notifications {
        Notifications(self)
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

    /// Returns whether or not the stream has been desynchronized due to an
    /// error in the communication channel with the server.
    ///
    /// If this has occurred, all further queries will immediately return an
    /// error.
    pub fn is_desynchronized(&self) -> bool {
        self.0.desynchronized
    }
}

/// A stream of asynchronous Postgres notifications.
pub struct Notifications(Connection);

impl Notifications {
    /// Consumes the `Notifications`, returning the inner `Connection`.
    pub fn into_inner(self) -> Connection {
        self.0
    }
}

impl Stream for Notifications {
    type Item = Notification;

    type Error = Error;

    fn poll(&mut self) -> Poll<Option<Notification>, Error> {
        if let Some(notification) = (self.0).0.notifications.pop_front() {
            return Ok(Async::Ready(Some(notification)));
        }

        match try_ready!((self.0).0.poll()) {
            Some(backend::Message::NotificationResponse(body)) => {
                let notification = Notification {
                    process_id: body.process_id(),
                    channel: body.channel()?.to_owned(),
                    payload: body.message()?.to_owned(),
                };
                Ok(Async::Ready(Some(notification)))
            }
            Some(_) => Err(bad_message()),
            None => Ok(Async::Ready(None)),
        }
    }
}

fn err(fields: &mut ErrorFields) -> Error {
    match DbError::new(fields) {
        Ok(err) => error::db(err),
        Err(err) => error::io(err),
    }
}

fn bad_message<T>() -> T
where
    T: From<io::Error>,
{
    io::Error::new(io::ErrorKind::InvalidInput, "unexpected message").into()
}

trait BoxedFuture<I, E> {
    fn boxed2(self) -> Box<Future<Item = I, Error = E> + Send>;
}

impl<F, I, E> BoxedFuture<I, E> for F
where
    F: Future<Item = I, Error = E> + Send + 'static,
{
    fn boxed2(self) -> Box<Future<Item = I, Error = E> + Send> {
        Box::new(self)
    }
}

trait BoxedStateStream<I, S, E> {
    fn boxed2(self) -> Box<StateStream<Item = I, State = S, Error = E> + Send>;
}

impl<T, I, S, E> BoxedStateStream<I, S, E> for T
where
    T: StateStream<Item = I, State = S, Error = E> + Send + 'static,
{
    fn boxed2(self) -> Box<StateStream<Item = I, State = S, Error = E> + Send> {
        Box::new(self)
    }
}

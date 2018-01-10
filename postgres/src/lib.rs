//! A pure-Rust frontend for the popular PostgreSQL database.
//!
//! ```rust,no_run
//! extern crate postgres;
//!
//! use postgres::{Connection, TlsMode};
//!
//! struct Person {
//!     id: i32,
//!     name: String,
//!     data: Option<Vec<u8>>
//! }
//!
//! fn main() {
//!     let conn = Connection::connect("postgresql://postgres@localhost:5433", TlsMode::None)
//!             .unwrap();
//!
//!     conn.execute("CREATE TABLE person (
//!                     id              SERIAL PRIMARY KEY,
//!                     name            VARCHAR NOT NULL,
//!                     data            BYTEA
//!                   )", &[]).unwrap();
//!     let me = Person {
//!         id: 0,
//!         name: "Steven".to_owned(),
//!         data: None
//!     };
//!     conn.execute("INSERT INTO person (name, data) VALUES ($1, $2)",
//!                  &[&me.name, &me.data]).unwrap();
//!
//!     for row in &conn.query("SELECT id, name, data FROM person", &[]).unwrap() {
//!         let person = Person {
//!             id: row.get(0),
//!             name: row.get(1),
//!             data: row.get(2)
//!         };
//!         println!("Found person {}", person.name);
//!     }
//! }
//! ```
//!
//! # SSL/TLS
//!
//! This crate supports TLS secured connections. The `TlsMode` enum is passed to connection methods
//! and indicates if the connection will not, may, or must be secured by TLS. The TLS implementation
//! is pluggable through the `TlsHandshake` trait. Implementations for OpenSSL, Secure Transport,
//! SChannel, and the `native-tls` crate are provided behind the `with-openssl`,
//! `with-security-framework`, `with-schannel`, and `with-native-tls` feature flags respectively.
//!
//! ## Examples
//!
//! Connecting using `native-tls`:
//!
//! ```no_run
//! extern crate postgres;
//!
//! use postgres::{Connection, TlsMode};
//! # #[cfg(feature = "with-native-tls")]
//! use postgres::tls::native_tls::NativeTls;
//!
//! # #[cfg(not(feature = "with-native-tls"))] fn main() {}
//! # #[cfg(feature = "with-native-tls")]
//! fn main() {
//!     let negotiator = NativeTls::new().unwrap();
//!     let conn = Connection::connect("postgres://postgres@localhost:5433", TlsMode::Require(&negotiator))
//!         .unwrap();
//! }
//! ```
#![doc(html_root_url = "https://docs.rs/postgres/0.15.1")]
#![warn(missing_docs)]
#![allow(unknown_lints, needless_lifetimes, doc_markdown)] // for clippy

extern crate bytes;
extern crate fallible_iterator;
#[cfg(not(feature = "no-logging"))]
#[macro_use]
extern crate log;
extern crate postgres_protocol;
extern crate postgres_shared;
extern crate socket2;

use fallible_iterator::FallibleIterator;
use std::cell::{Cell, RefCell};
use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::io;
use std::mem;
use std::result;
use std::sync::Arc;
use std::time::Duration;
use postgres_protocol::authentication;
use postgres_protocol::authentication::sasl::{self, ScramSha256};
use postgres_protocol::message::backend::{self, ErrorFields};
use postgres_protocol::message::frontend;
use postgres_shared::rows::RowData;

use error::{DbError, UNDEFINED_COLUMN, UNDEFINED_TABLE};
use tls::TlsHandshake;
use notification::{Notification, Notifications};
use params::{IntoConnectParams, User};
use priv_io::MessageStream;
use rows::Rows;
use stmt::{Column, Statement};
use transaction::{IsolationLevel, Transaction};
use types::{Field, FromSql, IsNull, Kind, Oid, ToSql, Type, CHAR, NAME, OID};

#[doc(inline)]
pub use postgres_shared::CancelData;
#[doc(inline)]
pub use postgres_shared::{error, types};
#[doc(inline)]
pub use error::Error;

#[macro_use]
mod macros;

mod feature_check;
mod priv_io;
pub mod tls;
pub mod notification;
pub mod params;
pub mod rows;
pub mod stmt;
pub mod transaction;

const TYPEINFO_QUERY: &'static str = "__typeinfo";
const TYPEINFO_ENUM_QUERY: &'static str = "__typeinfo_enum";
const TYPEINFO_COMPOSITE_QUERY: &'static str = "__typeinfo_composite";

/// A type alias of the result returned by many methods.
pub type Result<T> = result::Result<T, Error>;

/// A trait implemented by types that can handle Postgres notice messages.
///
/// It is implemented for all `Send + FnMut(DbError)` closures.
pub trait HandleNotice: Send {
    /// Handle a Postgres notice message
    fn handle_notice(&mut self, notice: DbError);
}

impl<F: Send + FnMut(DbError)> HandleNotice for F {
    fn handle_notice(&mut self, notice: DbError) {
        self(notice)
    }
}

/// A notice handler which logs at the `info` level.
///
/// This is the default handler used by a `Connection`.
#[derive(Copy, Clone, Debug)]
pub struct LoggingNoticeHandler;

impl HandleNotice for LoggingNoticeHandler {
    fn handle_notice(&mut self, _notice: DbError) {
        info!("{}: {}", _notice.severity, _notice.message);
    }
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
///
/// # Example
///
/// ```rust,no_run
/// # use postgres::{Connection, TlsMode};
/// # use std::thread;
/// # let url = "";
/// let conn = Connection::connect(url, TlsMode::None).unwrap();
/// let cancel_data = conn.cancel_data();
/// thread::spawn(move || {
///     conn.execute("SOME EXPENSIVE QUERY", &[]).unwrap();
/// });
/// postgres::cancel_query(url, TlsMode::None, &cancel_data).unwrap();
/// ```
pub fn cancel_query<T>(params: T, tls: TlsMode, data: &CancelData) -> Result<()>
where
    T: IntoConnectParams,
{
    let params = params.into_connect_params().map_err(error::connect)?;
    let mut socket = priv_io::initialize_stream(&params, tls)?;

    let mut buf = vec![];
    frontend::cancel_request(data.process_id, data.secret_key, &mut buf);
    socket.write_all(&buf)?;
    socket.flush()?;

    Ok(())
}

fn bad_response() -> io::Error {
    io::Error::new(
        io::ErrorKind::InvalidInput,
        "the server returned an unexpected response",
    )
}

fn desynchronized() -> io::Error {
    io::Error::new(
        io::ErrorKind::Other,
        "communication with the server has desynchronized due to an earlier IO error",
    )
}

/// Specifies the TLS support requested for a new connection.
#[derive(Debug)]
pub enum TlsMode<'a> {
    /// The connection will not use TLS.
    None,
    /// The connection will use TLS if the backend supports it.
    Prefer(&'a TlsHandshake),
    /// The connection must use TLS.
    Require(&'a TlsHandshake),
}

#[derive(Debug)]
struct StatementInfo {
    name: String,
    param_types: Vec<Type>,
    columns: Vec<Column>,
}

struct InnerConnection {
    stream: MessageStream,
    notice_handler: Box<HandleNotice>,
    notifications: VecDeque<Notification>,
    cancel_data: CancelData,
    unknown_types: HashMap<Oid, Type>,
    cached_statements: HashMap<String, Arc<StatementInfo>>,
    parameters: HashMap<String, String>,
    next_stmt_id: u32,
    trans_depth: u32,
    desynchronized: bool,
    finished: bool,
    has_typeinfo_query: bool,
    has_typeinfo_enum_query: bool,
    has_typeinfo_composite_query: bool,
}

impl Drop for InnerConnection {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish_inner();
        }
    }
}

impl InnerConnection {
    fn connect<T>(params: T, tls: TlsMode) -> Result<InnerConnection>
    where
        T: IntoConnectParams,
    {
        let params = params.into_connect_params().map_err(error::connect)?;
        let stream = priv_io::initialize_stream(&params, tls)?;

        let user = match params.user() {
            Some(user) => user,
            None => {
                return Err(error::connect(
                    "user missing from connection parameters".into(),
                ));
            }
        };

        let mut conn = InnerConnection {
            stream: MessageStream::new(stream),
            next_stmt_id: 0,
            notice_handler: Box::new(LoggingNoticeHandler),
            notifications: VecDeque::new(),
            cancel_data: CancelData {
                process_id: 0,
                secret_key: 0,
            },
            unknown_types: HashMap::new(),
            cached_statements: HashMap::new(),
            parameters: HashMap::new(),
            desynchronized: false,
            finished: false,
            trans_depth: 0,
            has_typeinfo_query: false,
            has_typeinfo_enum_query: false,
            has_typeinfo_composite_query: false,
        };

        let mut options = params.options().to_owned();
        options.push(("client_encoding".to_owned(), "UTF8".to_owned()));
        // Postgres uses the value of TimeZone as the time zone for TIMESTAMP
        // WITH TIME ZONE values. Timespec converts to GMT internally.
        options.push(("timezone".to_owned(), "GMT".to_owned()));
        // We have to clone here since we need the user again for auth
        options.push(("user".to_owned(), user.name().to_owned()));
        if let Some(database) = params.database() {
            options.push(("database".to_owned(), database.to_owned()));
        }

        let options = options.iter().map(|&(ref a, ref b)| (&**a, &**b));
        conn.stream
            .write_message(|buf| frontend::startup_message(options, buf))?;
        conn.stream.flush()?;

        conn.handle_auth(user)?;

        loop {
            match conn.read_message()? {
                backend::Message::BackendKeyData(body) => {
                    conn.cancel_data.process_id = body.process_id();
                    conn.cancel_data.secret_key = body.secret_key();
                }
                backend::Message::ReadyForQuery(_) => break,
                backend::Message::ErrorResponse(body) => {
                    return Err(err(&mut body.fields()));
                }
                _ => return Err(bad_response().into()),
            }
        }

        Ok(conn)
    }

    fn read_message_with_notification(&mut self) -> io::Result<backend::Message> {
        debug_assert!(!self.desynchronized);
        loop {
            match try_desync!(self, self.stream.read_message()) {
                backend::Message::NoticeResponse(body) => {
                    if let Ok(err) = DbError::new(&mut body.fields()) {
                        self.notice_handler.handle_notice(err);
                    }
                }
                backend::Message::ParameterStatus(body) => {
                    self.parameters
                        .insert(body.name()?.to_owned(), body.value()?.to_owned());
                }
                val => return Ok(val),
            }
        }
    }

    fn read_message_with_notification_timeout(
        &mut self,
        timeout: Duration,
    ) -> io::Result<Option<backend::Message>> {
        debug_assert!(!self.desynchronized);
        loop {
            match try_desync!(self, self.stream.read_message_timeout(timeout)) {
                Some(backend::Message::NoticeResponse(body)) => {
                    if let Ok(err) = Err(err(&mut body.fields())) {
                        self.notice_handler.handle_notice(err);
                    }
                }
                Some(backend::Message::ParameterStatus(body)) => {
                    self.parameters
                        .insert(body.name()?.to_owned(), body.value()?.to_owned());
                }
                val => return Ok(val),
            }
        }
    }

    fn read_message_with_notification_nonblocking(
        &mut self,
    ) -> io::Result<Option<backend::Message>> {
        debug_assert!(!self.desynchronized);
        loop {
            match try_desync!(self, self.stream.read_message_nonblocking()) {
                Some(backend::Message::NoticeResponse(body)) => {
                    if let Ok(err) = Err(err(&mut body.fields())) {
                        self.notice_handler.handle_notice(err);
                    }
                }
                Some(backend::Message::ParameterStatus(body)) => {
                    self.parameters
                        .insert(body.name()?.to_owned(), body.value()?.to_owned());
                }
                val => return Ok(val),
            }
        }
    }

    fn read_message(&mut self) -> io::Result<backend::Message> {
        loop {
            match self.read_message_with_notification()? {
                backend::Message::NotificationResponse(body) => {
                    self.notifications.push_back(Notification {
                        process_id: body.process_id(),
                        channel: body.channel()?.to_owned(),
                        payload: body.message()?.to_owned(),
                    })
                }
                val => return Ok(val),
            }
        }
    }

    fn handle_auth(&mut self, user: &User) -> Result<()> {
        match self.read_message()? {
            backend::Message::AuthenticationOk => return Ok(()),
            backend::Message::AuthenticationCleartextPassword => {
                let pass = user.password().ok_or_else(|| {
                    error::connect("a password was requested but not provided".into())
                })?;
                self.stream
                    .write_message(|buf| frontend::password_message(pass, buf))?;
                self.stream.flush()?;
            }
            backend::Message::AuthenticationMd5Password(body) => {
                let pass = user.password().ok_or_else(|| {
                    error::connect("a password was requested but not provided".into())
                })?;
                let output =
                    authentication::md5_hash(user.name().as_bytes(), pass.as_bytes(), body.salt());
                self.stream
                    .write_message(|buf| frontend::password_message(&output, buf))?;
                self.stream.flush()?;
            }
            backend::Message::AuthenticationSasl(body) => {
                // count to validate the entire message body.
                if body.mechanisms()
                    .filter(|m| *m == sasl::SCRAM_SHA_256)
                    .count()? == 0
                {
                    return Err(
                        io::Error::new(io::ErrorKind::Other, "unsupported authentication").into(),
                    );
                }

                let pass = user.password().ok_or_else(|| {
                    error::connect("a password was requested but not provided".into())
                })?;

                let mut scram = ScramSha256::new(pass.as_bytes())?;

                self.stream.write_message(|buf| {
                    frontend::sasl_initial_response(sasl::SCRAM_SHA_256, scram.message(), buf)
                })?;
                self.stream.flush()?;

                let body = match self.read_message()? {
                    backend::Message::AuthenticationSaslContinue(body) => body,
                    backend::Message::ErrorResponse(body) => return Err(err(&mut body.fields())),
                    _ => return Err(bad_response().into()),
                };

                scram.update(body.data())?;

                self.stream
                    .write_message(|buf| frontend::sasl_response(scram.message(), buf))?;
                self.stream.flush()?;

                let body = match self.read_message()? {
                    backend::Message::AuthenticationSaslFinal(body) => body,
                    backend::Message::ErrorResponse(body) => return Err(err(&mut body.fields())),
                    _ => return Err(bad_response().into()),
                };

                scram.finish(body.data())?;
            }
            backend::Message::AuthenticationKerberosV5
            | backend::Message::AuthenticationScmCredential
            | backend::Message::AuthenticationGss
            | backend::Message::AuthenticationSspi => {
                return Err(
                    io::Error::new(io::ErrorKind::Other, "unsupported authentication").into(),
                )
            }
            backend::Message::ErrorResponse(body) => return Err(err(&mut body.fields())),
            _ => return Err(bad_response().into()),
        }

        match self.read_message()? {
            backend::Message::AuthenticationOk => Ok(()),
            backend::Message::ErrorResponse(body) => Err(err(&mut body.fields())),
            _ => Err(bad_response().into()),
        }
    }

    fn set_notice_handler(&mut self, handler: Box<HandleNotice>) -> Box<HandleNotice> {
        mem::replace(&mut self.notice_handler, handler)
    }

    fn raw_prepare(&mut self, stmt_name: &str, query: &str) -> Result<(Vec<Type>, Vec<Column>)> {
        debug!("preparing query with name `{}`: {}", stmt_name, query);

        self.stream
            .write_message(|buf| frontend::parse(stmt_name, query, None, buf))?;
        self.stream
            .write_message(|buf| frontend::describe(b'S', stmt_name, buf))?;
        self.stream
            .write_message(|buf| Ok::<(), io::Error>(frontend::sync(buf)))?;
        self.stream.flush()?;

        match self.read_message()? {
            backend::Message::ParseComplete => {}
            backend::Message::ErrorResponse(body) => {
                self.wait_for_ready()?;
                return Err(err(&mut body.fields()));
            }
            _ => bad_response!(self),
        }

        let raw_param_types = match self.read_message()? {
            backend::Message::ParameterDescription(body) => body,
            _ => bad_response!(self),
        };

        let raw_columns = match self.read_message()? {
            backend::Message::RowDescription(body) => Some(body),
            backend::Message::NoData => None,
            _ => bad_response!(self),
        };

        self.wait_for_ready()?;

        let param_types = raw_param_types
            .parameters()
            .map_err(Into::into)
            .and_then(|oid| self.get_type(oid))
            .collect()?;

        let columns = match raw_columns {
            Some(body) => body.fields()
                .and_then(|field| {
                    Ok(Column::new(
                        field.name().to_owned(),
                        self.get_type(field.type_oid())?,
                    ))
                })
                .collect()?,
            None => vec![],
        };

        Ok((param_types, columns))
    }

    fn read_rows<F>(&mut self, mut consumer: F) -> Result<bool>
    where
        F: FnMut(RowData),
    {
        let more_rows;
        loop {
            match self.read_message()? {
                backend::Message::EmptyQueryResponse | backend::Message::CommandComplete(_) => {
                    more_rows = false;
                    break;
                }
                backend::Message::PortalSuspended => {
                    more_rows = true;
                    break;
                }
                backend::Message::DataRow(body) => consumer(RowData::new(body)?),
                backend::Message::ErrorResponse(body) => {
                    self.wait_for_ready()?;
                    return Err(err(&mut body.fields()));
                }
                backend::Message::CopyInResponse(_) => {
                    self.stream.write_message(|buf| {
                        frontend::copy_fail("COPY queries cannot be directly executed", buf)
                    })?;
                    self.stream
                        .write_message(|buf| Ok::<(), io::Error>(frontend::sync(buf)))?;
                    self.stream.flush()?;
                }
                backend::Message::CopyOutResponse(_) => {
                    loop {
                        if let backend::Message::ReadyForQuery(_) = self.read_message()? {
                            break;
                        }
                    }
                    return Err(
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "COPY queries cannot be directly \
                             executed",
                        ).into(),
                    );
                }
                _ => {
                    self.desynchronized = true;
                    return Err(bad_response().into());
                }
            }
        }
        self.wait_for_ready()?;
        Ok(more_rows)
    }

    fn raw_execute(
        &mut self,
        stmt_name: &str,
        portal_name: &str,
        row_limit: i32,
        param_types: &[Type],
        params: &[&ToSql],
    ) -> Result<()> {
        assert!(
            param_types.len() == params.len(),
            "expected {} parameters but got {}",
            param_types.len(),
            params.len()
        );
        debug!(
            "executing statement {} with parameters: {:?}",
            stmt_name,
            params
        );

        {
            let r = self.stream.write_message(|buf| {
                frontend::bind(
                    portal_name,
                    stmt_name,
                    Some(1),
                    params.iter().zip(param_types),
                    |(param, ty), buf| match param.to_sql_checked(ty, buf) {
                        Ok(IsNull::Yes) => Ok(postgres_protocol::IsNull::Yes),
                        Ok(IsNull::No) => Ok(postgres_protocol::IsNull::No),
                        Err(e) => Err(e),
                    },
                    Some(1),
                    buf,
                )
            });
            match r {
                Ok(()) => {}
                Err(frontend::BindError::Conversion(e)) => {
                    return Err(error::conversion(e));
                }
                Err(frontend::BindError::Serialization(e)) => return Err(e.into()),
            }
        }

        self.stream
            .write_message(|buf| frontend::execute(portal_name, row_limit, buf))?;
        self.stream
            .write_message(|buf| Ok::<(), io::Error>(frontend::sync(buf)))?;
        self.stream.flush()?;

        match self.read_message()? {
            backend::Message::BindComplete => Ok(()),
            backend::Message::ErrorResponse(body) => {
                self.wait_for_ready()?;
                Err(err(&mut body.fields()))
            }
            _ => {
                self.desynchronized = true;
                Err(bad_response().into())
            }
        }
    }

    fn make_stmt_name(&mut self) -> String {
        let stmt_name = format!("s{}", self.next_stmt_id);
        self.next_stmt_id += 1;
        stmt_name
    }

    fn prepare<'a>(&mut self, query: &str, conn: &'a Connection) -> Result<Statement<'a>> {
        let stmt_name = self.make_stmt_name();
        let (param_types, columns) = self.raw_prepare(&stmt_name, query)?;
        let info = Arc::new(StatementInfo {
            name: stmt_name,
            param_types: param_types,
            columns: columns,
        });
        Ok(Statement::new(conn, info, Cell::new(0), false))
    }

    fn prepare_cached<'a>(&mut self, query: &str, conn: &'a Connection) -> Result<Statement<'a>> {
        let info = self.cached_statements.get(query).cloned();

        let info = match info {
            Some(info) => info,
            None => {
                let stmt_name = self.make_stmt_name();
                let (param_types, columns) = self.raw_prepare(&stmt_name, query)?;
                let info = Arc::new(StatementInfo {
                    name: stmt_name,
                    param_types: param_types,
                    columns: columns,
                });
                self.cached_statements
                    .insert(query.to_owned(), info.clone());
                info
            }
        };

        Ok(Statement::new(conn, info, Cell::new(0), true))
    }

    fn close_statement(&mut self, name: &str, type_: u8) -> Result<()> {
        self.stream
            .write_message(|buf| frontend::close(type_, name, buf))?;
        self.stream
            .write_message(|buf| Ok::<(), io::Error>(frontend::sync(buf)))?;
        self.stream.flush()?;
        let resp = match self.read_message()? {
            backend::Message::CloseComplete => Ok(()),
            backend::Message::ErrorResponse(body) => Err(err(&mut body.fields())),
            _ => bad_response!(self),
        };
        self.wait_for_ready()?;
        resp
    }

    fn get_type(&mut self, oid: Oid) -> Result<Type> {
        if let Some(ty) = Type::from_oid(oid) {
            return Ok(ty);
        }

        if let Some(ty) = self.unknown_types.get(&oid) {
            return Ok(ty.clone());
        }

        let ty = self.read_type(oid)?;
        self.unknown_types.insert(oid, ty.clone());
        Ok(ty)
    }

    fn setup_typeinfo_query(&mut self) -> Result<()> {
        if self.has_typeinfo_query {
            return Ok(());
        }

        match self.raw_prepare(
            TYPEINFO_QUERY,
            "SELECT t.typname, t.typtype, t.typelem, r.rngsubtype, \
             t.typbasetype, n.nspname, t.typrelid \
             FROM pg_catalog.pg_type t \
             LEFT OUTER JOIN pg_catalog.pg_range r ON \
             r.rngtypid = t.oid \
             INNER JOIN pg_catalog.pg_namespace n ON \
             t.typnamespace = n.oid \
             WHERE t.oid = $1",
        ) {
            Ok(..) => {}
            // Range types weren't added until Postgres 9.2, so pg_range may not exist
            Err(ref e) if e.code() == Some(&UNDEFINED_TABLE) => {
                self.raw_prepare(
                    TYPEINFO_QUERY,
                    "SELECT t.typname, t.typtype, t.typelem, NULL::OID, \
                     t.typbasetype, n.nspname, t.typrelid \
                     FROM pg_catalog.pg_type t \
                     INNER JOIN pg_catalog.pg_namespace n \
                     ON t.typnamespace = n.oid \
                     WHERE t.oid = $1",
                )?;
            }
            Err(e) => return Err(e),
        }

        self.has_typeinfo_query = true;
        Ok(())
    }

    #[allow(if_not_else)]
    fn read_type(&mut self, oid: Oid) -> Result<Type> {
        self.setup_typeinfo_query()?;
        self.raw_execute(TYPEINFO_QUERY, "", 0, &[OID], &[&oid])?;
        let mut row = None;
        self.read_rows(|r| row = Some(r))?;

        let get_raw = |i: usize| row.as_ref().and_then(|r| r.get(i));

        let (name, type_, elem_oid, rngsubtype, basetype, schema, relid) = {
            let name = String::from_sql_nullable(&NAME, get_raw(0)).map_err(error::conversion)?;
            let type_ = i8::from_sql_nullable(&CHAR, get_raw(1)).map_err(error::conversion)?;
            let elem_oid = Oid::from_sql_nullable(&OID, get_raw(2)).map_err(error::conversion)?;
            let rngsubtype =
                Option::<Oid>::from_sql_nullable(&OID, get_raw(3)).map_err(error::conversion)?;
            let basetype = Oid::from_sql_nullable(&OID, get_raw(4)).map_err(error::conversion)?;
            let schema = String::from_sql_nullable(&NAME, get_raw(5)).map_err(error::conversion)?;
            let relid = Oid::from_sql_nullable(&OID, get_raw(6)).map_err(error::conversion)?;
            (name, type_, elem_oid, rngsubtype, basetype, schema, relid)
        };

        let kind = if type_ == b'e' as i8 {
            Kind::Enum(self.read_enum_variants(oid)?)
        } else if type_ == b'p' as i8 {
            Kind::Pseudo
        } else if basetype != 0 {
            Kind::Domain(self.get_type(basetype)?)
        } else if elem_oid != 0 {
            Kind::Array(self.get_type(elem_oid)?)
        } else if relid != 0 {
            Kind::Composite(self.read_composite_fields(relid)?)
        } else {
            match rngsubtype {
                Some(oid) => Kind::Range(self.get_type(oid)?),
                None => Kind::Simple,
            }
        };

        Ok(Type::_new(name, oid, kind, schema))
    }

    fn setup_typeinfo_enum_query(&mut self) -> Result<()> {
        if self.has_typeinfo_enum_query {
            return Ok(());
        }

        match self.raw_prepare(
            TYPEINFO_ENUM_QUERY,
            "SELECT enumlabel \
             FROM pg_catalog.pg_enum \
             WHERE enumtypid = $1 \
             ORDER BY enumsortorder",
        ) {
            Ok(..) => {}
            // Postgres 9.0 doesn't have enumsortorder
            Err(ref e) if e.code() == Some(&UNDEFINED_COLUMN) => {
                self.raw_prepare(
                    TYPEINFO_ENUM_QUERY,
                    "SELECT enumlabel \
                     FROM pg_catalog.pg_enum \
                     WHERE enumtypid = $1 \
                     ORDER BY oid",
                )?;
            }
            Err(e) => return Err(e),
        }

        self.has_typeinfo_enum_query = true;
        Ok(())
    }

    fn read_enum_variants(&mut self, oid: Oid) -> Result<Vec<String>> {
        self.setup_typeinfo_enum_query()?;
        self.raw_execute(TYPEINFO_ENUM_QUERY, "", 0, &[OID], &[&oid])?;
        let mut rows = vec![];
        self.read_rows(|row| rows.push(row))?;

        let mut variants = vec![];
        for row in rows {
            variants.push(String::from_sql_nullable(&NAME, row.get(0))
                .map_err(error::conversion)?);
        }

        Ok(variants)
    }

    fn setup_typeinfo_composite_query(&mut self) -> Result<()> {
        if self.has_typeinfo_composite_query {
            return Ok(());
        }

        self.raw_prepare(
            TYPEINFO_COMPOSITE_QUERY,
            "SELECT attname, atttypid \
             FROM pg_catalog.pg_attribute \
             WHERE attrelid = $1 \
             AND NOT attisdropped \
             AND attnum > 0 \
             ORDER BY attnum",
        )?;

        self.has_typeinfo_composite_query = true;
        Ok(())
    }

    fn read_composite_fields(&mut self, relid: Oid) -> Result<Vec<Field>> {
        self.setup_typeinfo_composite_query()?;
        self.raw_execute(TYPEINFO_COMPOSITE_QUERY, "", 0, &[OID], &[&relid])?;
        let mut rows = vec![];
        self.read_rows(|row| rows.push(row))?;

        let mut fields = vec![];
        for row in rows {
            let (name, type_) = {
                let name = String::from_sql_nullable(&NAME, row.get(0)).map_err(error::conversion)?;
                let type_ = Oid::from_sql_nullable(&OID, row.get(1)).map_err(error::conversion)?;
                (name, type_)
            };
            let type_ = self.get_type(type_)?;
            fields.push(Field::new(name, type_));
        }

        Ok(fields)
    }

    fn is_desynchronized(&self) -> bool {
        self.desynchronized
    }

    #[allow(needless_return)]
    fn wait_for_ready(&mut self) -> Result<()> {
        match self.read_message()? {
            backend::Message::ReadyForQuery(_) => Ok(()),
            _ => bad_response!(self),
        }
    }

    fn quick_query(&mut self, query: &str) -> Result<Vec<Vec<Option<String>>>> {
        check_desync!(self);
        debug!("executing query: {}", query);
        self.stream
            .write_message(|buf| frontend::query(query, buf))?;
        self.stream.flush()?;

        let mut result = vec![];
        loop {
            match self.read_message()? {
                backend::Message::ReadyForQuery(_) => break,
                backend::Message::DataRow(body) => {
                    let row = body.ranges()
                        .map(|r| {
                            r.map(|r| String::from_utf8_lossy(&body.buffer()[r]).into_owned())
                        })
                        .collect()?;
                    result.push(row);
                }
                backend::Message::CopyInResponse(_) => {
                    self.stream.write_message(|buf| {
                        frontend::copy_fail("COPY queries cannot be directly executed", buf)
                    })?;
                    self.stream
                        .write_message(|buf| Ok::<(), io::Error>(frontend::sync(buf)))?;
                    self.stream.flush()?;
                }
                backend::Message::ErrorResponse(body) => {
                    self.wait_for_ready()?;
                    return Err(err(&mut body.fields()));
                }
                _ => {}
            }
        }
        Ok(result)
    }

    fn finish_inner(&mut self) -> Result<()> {
        check_desync!(self);
        self.stream
            .write_message(|buf| Ok::<(), io::Error>(frontend::terminate(buf)))?;
        self.stream.flush()?;
        Ok(())
    }
}

fn _ensure_send() {
    fn _is_send<T: Send>() {}
    _is_send::<Connection>();
}

/// A connection to a Postgres database.
pub struct Connection(RefCell<InnerConnection>);

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let conn = self.0.borrow();
        fmt.debug_struct("Connection")
            .field("stream", &conn.stream.get_ref())
            .field("cancel_data", &conn.cancel_data)
            .field("notifications", &conn.notifications.len())
            .field("transaction_depth", &conn.trans_depth)
            .field("desynchronized", &conn.desynchronized)
            .field("cached_statements", &conn.cached_statements.len())
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
    ///
    /// # Examples
    ///
    /// To connect over TCP:
    ///
    /// ```rust,no_run
    /// use postgres::{Connection, TlsMode};
    ///
    /// let url = "postgresql://postgres:hunter2@localhost:5433:2994/foodb";
    /// let conn = Connection::connect(url, TlsMode::None).unwrap();
    /// ```
    ///
    /// To connect over a Unix socket located in `/run/postgres`:
    ///
    /// ```rust,no_run
    /// use postgres::{Connection, TlsMode};
    ///
    /// let url = "postgresql://postgres@%2Frun%2Fpostgres";
    /// let conn = Connection::connect(url, TlsMode::None).unwrap();
    /// ```
    ///
    /// To connect with a manually constructed `ConnectParams`:
    ///
    /// ```rust,no_run
    /// use postgres::{Connection, TlsMode};
    /// use postgres::params::{ConnectParams, Host};
    /// # use std::path::PathBuf;
    ///
    /// # #[cfg(unix)]
    /// # fn f() {
    /// # let some_crazy_path = PathBuf::new();
    /// let params = ConnectParams::builder()
    ///     .user("postgres", None)
    ///     .build(Host::Unix(some_crazy_path));
    /// let conn = Connection::connect(params, TlsMode::None).unwrap();
    /// # }
    /// ```
    pub fn connect<T>(params: T, tls: TlsMode) -> Result<Connection>
    where
        T: IntoConnectParams,
    {
        InnerConnection::connect(params, tls).map(|conn| Connection(RefCell::new(conn)))
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// A statement may contain parameters, specified by `$n` where `n` is the
    /// index of the parameter in the list provided, 1-indexed.
    ///
    /// If the statement does not modify any rows (e.g. SELECT), 0 is returned.
    ///
    /// If the same statement will be repeatedly executed (perhaps with
    /// different query parameters), consider using the `prepare` and
    /// `prepare_cached` methods.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, TlsMode};
    /// # let conn = Connection::connect("", TlsMode::None).unwrap();
    /// # let bar = 1i32;
    /// # let baz = true;
    /// let rows_updated = conn.execute("UPDATE foo SET bar = $1 WHERE baz = $2", &[&bar, &baz])
    ///                        .unwrap();
    /// println!("{} rows updated", rows_updated);
    /// ```
    pub fn execute(&self, query: &str, params: &[&ToSql]) -> Result<u64> {
        let (param_types, columns) = self.0.borrow_mut().raw_prepare("", query)?;
        let info = Arc::new(StatementInfo {
            name: String::new(),
            param_types: param_types,
            columns: columns,
        });
        let stmt = Statement::new(self, info, Cell::new(0), true);
        stmt.execute(params)
    }

    /// Executes a statement, returning the resulting rows.
    ///
    /// A statement may contain parameters, specified by `$n` where `n` is the
    /// index of the parameter in the list provided, 1-indexed.
    ///
    /// If the same statement will be repeatedly executed (perhaps with
    /// different query parameters), consider using the `prepare` and
    /// `prepare_cached` methods.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, TlsMode};
    /// # let conn = Connection::connect("", TlsMode::None).unwrap();
    /// # let baz = true;
    /// for row in &conn.query("SELECT foo FROM bar WHERE baz = $1", &[&baz]).unwrap() {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// ```
    pub fn query(&self, query: &str, params: &[&ToSql]) -> Result<Rows> {
        let (param_types, columns) = self.0.borrow_mut().raw_prepare("", query)?;
        let info = Arc::new(StatementInfo {
            name: String::new(),
            param_types: param_types,
            columns: columns,
        });
        let stmt = Statement::new(self, info, Cell::new(0), true);
        stmt.into_query(params)
    }

    /// Begins a new transaction.
    ///
    /// Returns a `Transaction` object which should be used instead of
    /// the connection for the duration of the transaction. The transaction
    /// is active until the `Transaction` object falls out of scope.
    ///
    /// # Note
    /// A transaction will roll back by default. The `set_commit`,
    /// `set_rollback`, and `commit` methods alter this behavior.
    ///
    /// # Panics
    ///
    /// Panics if a transaction is already active.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, TlsMode};
    /// # let conn = Connection::connect("", TlsMode::None).unwrap();
    /// let trans = conn.transaction().unwrap();
    /// trans.execute("UPDATE foo SET bar = 10", &[]).unwrap();
    /// // ...
    ///
    /// trans.commit().unwrap();
    /// ```
    pub fn transaction<'a>(&'a self) -> Result<Transaction<'a>> {
        self.transaction_with(&transaction::Config::new())
    }

    /// Begins a new transaction with the specified configuration.
    pub fn transaction_with<'a>(&'a self, config: &transaction::Config) -> Result<Transaction<'a>> {
        let mut conn = self.0.borrow_mut();
        check_desync!(conn);
        assert!(
            conn.trans_depth == 0,
            "`transaction` must be called on the active transaction"
        );
        let mut query = "BEGIN".to_owned();
        config.build_command(&mut query);
        conn.quick_query(&query)?;
        conn.trans_depth += 1;
        Ok(Transaction::new(self, 1))
    }

    /// Creates a new prepared statement.
    ///
    /// If the same statement will be executed repeatedly, explicitly preparing
    /// it can improve performance.
    ///
    /// The statement is associated with the connection that created it and may
    /// not outlive that connection.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, TlsMode};
    /// # let x = 10i32;
    /// # let conn = Connection::connect("", TlsMode::None).unwrap();
    /// # let (a, b) = (0i32, 1i32);
    /// # let updates = vec![(&a, &b)];
    /// let stmt = conn.prepare("UPDATE foo SET bar = $1 WHERE baz = $2").unwrap();
    /// for (bar, baz) in updates {
    ///     stmt.execute(&[bar, baz]).unwrap();
    /// }
    /// ```
    pub fn prepare<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        self.0.borrow_mut().prepare(query, self)
    }

    /// Creates a cached prepared statement.
    ///
    /// Like `prepare`, except that the statement is only prepared once over
    /// the lifetime of the connection and then cached. If the same statement
    /// is going to be prepared frequently, caching it can improve performance
    /// by reducing the number of round trips to the Postgres backend.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, TlsMode};
    /// # let x = 10i32;
    /// # let conn = Connection::connect("", TlsMode::None).unwrap();
    /// # let (a, b) = (0i32, 1i32);
    /// # let updates = vec![(&a, &b)];
    /// let stmt = conn.prepare_cached("UPDATE foo SET bar = $1 WHERE baz = $2").unwrap();
    /// for (bar, baz) in updates {
    ///     stmt.execute(&[bar, baz]).unwrap();
    /// }
    /// ```
    pub fn prepare_cached<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        self.0.borrow_mut().prepare_cached(query, self)
    }

    /// Returns the isolation level which will be used for future transactions.
    ///
    /// This is a simple wrapper around `SHOW TRANSACTION ISOLATION LEVEL`.
    pub fn transaction_isolation(&self) -> Result<IsolationLevel> {
        let mut conn = self.0.borrow_mut();
        check_desync!(conn);
        let result = conn.quick_query("SHOW TRANSACTION ISOLATION LEVEL")?;
        IsolationLevel::new(result[0][0].as_ref().unwrap())
    }

    /// Sets the configuration that will be used for future transactions.
    pub fn set_transaction_config(&self, config: &transaction::Config) -> Result<()> {
        let mut command = "SET SESSION CHARACTERISTICS AS TRANSACTION".to_owned();
        config.build_command(&mut command);
        self.batch_execute(&command)
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
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, TlsMode, Result};
    /// # let conn = Connection::connect("", TlsMode::None).unwrap();
    /// conn.batch_execute("
    ///     CREATE TABLE person (
    ///         id SERIAL PRIMARY KEY,
    ///         name NOT NULL
    ///     );
    ///
    ///     CREATE TABLE purchase (
    ///         id SERIAL PRIMARY KEY,
    ///         person INT NOT NULL REFERENCES person (id),
    ///         time TIMESTAMPTZ NOT NULL,
    ///     );
    ///
    ///     CREATE INDEX ON purchase (time);
    ///     ").unwrap();
    /// ```
    pub fn batch_execute(&self, query: &str) -> Result<()> {
        self.0.borrow_mut().quick_query(query).map(|_| ())
    }

    /// Returns a structure providing access to asynchronous notifications.
    ///
    /// Use the `LISTEN` command to register this connection for notifications.
    pub fn notifications<'a>(&'a self) -> Notifications<'a> {
        Notifications::new(self)
    }

    /// Returns information used to cancel pending queries.
    ///
    /// Used with the `cancel_query` function. The object returned can be used
    /// to cancel any query executed by the connection it was created from.
    pub fn cancel_data(&self) -> CancelData {
        self.0.borrow().cancel_data
    }

    /// Returns the value of the specified Postgres backend parameter, such as
    /// `timezone` or `server_version`.
    pub fn parameter(&self, param: &str) -> Option<String> {
        self.0.borrow().parameters.get(param).cloned()
    }

    /// Sets the notice handler for the connection, returning the old handler.
    pub fn set_notice_handler(&self, handler: Box<HandleNotice>) -> Box<HandleNotice> {
        self.0.borrow_mut().set_notice_handler(handler)
    }

    /// Returns whether or not the stream has been desynchronized due to an
    /// error in the communication channel with the server.
    ///
    /// If this has occurred, all further queries will immediately return an
    /// error.
    pub fn is_desynchronized(&self) -> bool {
        self.0.borrow().is_desynchronized()
    }

    /// Determines if the `Connection` is currently "active", that is, if there
    /// are no active transactions.
    ///
    /// The `transaction` method can only be called on the active `Connection`
    /// or `Transaction`.
    pub fn is_active(&self) -> bool {
        self.0.borrow().trans_depth == 0
    }

    /// Consumes the connection, closing it.
    ///
    /// Functionally equivalent to the `Drop` implementation for `Connection`
    /// except that it returns any error encountered to the caller.
    pub fn finish(self) -> Result<()> {
        let mut conn = self.0.borrow_mut();
        conn.finished = true;
        conn.finish_inner()
    }
}

/// A trait allowing abstraction over connections and transactions
pub trait GenericConnection {
    /// Like `Connection::execute`.
    fn execute(&self, query: &str, params: &[&ToSql]) -> Result<u64>;

    /// Like `Connection::query`.
    fn query<'a>(&'a self, query: &str, params: &[&ToSql]) -> Result<Rows>;

    /// Like `Connection::prepare`.
    fn prepare<'a>(&'a self, query: &str) -> Result<Statement<'a>>;

    /// Like `Connection::prepare_cached`.
    fn prepare_cached<'a>(&'a self, query: &str) -> Result<Statement<'a>>;

    /// Like `Connection::transaction`.
    fn transaction<'a>(&'a self) -> Result<Transaction<'a>>;

    /// Like `Connection::batch_execute`.
    fn batch_execute(&self, query: &str) -> Result<()>;

    /// Like `Connection::is_active`.
    fn is_active(&self) -> bool;
}

impl GenericConnection for Connection {
    fn execute(&self, query: &str, params: &[&ToSql]) -> Result<u64> {
        self.execute(query, params)
    }

    fn query<'a>(&'a self, query: &str, params: &[&ToSql]) -> Result<Rows> {
        self.query(query, params)
    }

    fn prepare<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        self.prepare(query)
    }

    fn prepare_cached<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        self.prepare_cached(query)
    }

    fn transaction<'a>(&'a self) -> Result<Transaction<'a>> {
        self.transaction()
    }

    fn batch_execute(&self, query: &str) -> Result<()> {
        self.batch_execute(query)
    }

    fn is_active(&self) -> bool {
        self.is_active()
    }
}

impl<'a> GenericConnection for Transaction<'a> {
    fn execute(&self, query: &str, params: &[&ToSql]) -> Result<u64> {
        self.execute(query, params)
    }

    fn query<'b>(&'b self, query: &str, params: &[&ToSql]) -> Result<Rows> {
        self.query(query, params)
    }

    fn prepare<'b>(&'b self, query: &str) -> Result<Statement<'b>> {
        self.prepare(query)
    }

    fn prepare_cached<'b>(&'b self, query: &str) -> Result<Statement<'b>> {
        self.prepare_cached(query)
    }

    fn transaction<'b>(&'b self) -> Result<Transaction<'b>> {
        self.transaction()
    }

    fn batch_execute(&self, query: &str) -> Result<()> {
        self.batch_execute(query)
    }

    fn is_active(&self) -> bool {
        self.is_active()
    }
}

fn err(fields: &mut ErrorFields) -> Error {
    match DbError::new(fields) {
        Ok(err) => error::db(err),
        Err(err) => err.into(),
    }
}

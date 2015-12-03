//! Rust-Postgres is a pure-Rust frontend for the popular PostgreSQL database. It
//! exposes a high level interface in the vein of JDBC or Go's `database/sql`
//! package.
//!
//! ```rust,no_run
//! extern crate postgres;
//!
//! use postgres::{Connection, SslMode};
//!
//! struct Person {
//!     id: i32,
//!     name: String,
//!     data: Option<Vec<u8>>
//! }
//!
//! fn main() {
//!     let conn = Connection::connect("postgresql://postgres@localhost", &SslMode::None)
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
//!     let stmt = conn.prepare("SELECT id, name, data FROM person").unwrap();
//!     for row in stmt.query(&[]).unwrap() {
//!         let person = Person {
//!             id: row.get(0),
//!             name: row.get(1),
//!             data: row.get(2)
//!         };
//!         println!("Found person {}", person.name);
//!     }
//! }
//! ```
#![doc(html_root_url="https://sfackler.github.io/rust-postgres/doc/v0.10.2")]
#![warn(missing_docs)]

extern crate bufstream;
extern crate byteorder;
#[macro_use]
extern crate log;
extern crate phf;
extern crate rustc_serialize as serialize;
#[cfg(feature = "unix_socket")]
extern crate unix_socket;
extern crate net2;

use bufstream::BufStream;
use md5::Md5;
use std::ascii::AsciiExt;
use std::borrow::ToOwned;
use std::cell::{Cell, RefCell};
use std::collections::{VecDeque, HashMap};
use std::error::Error as StdError;
use std::fmt;
use std::iter::IntoIterator;
use std::io as std_io;
use std::io::prelude::*;
use std::marker::Sync as StdSync;
use std::mem;
use std::result;
use std::time::Duration;
#[cfg(feature = "unix_socket")]
use std::path::PathBuf;

use error::{Error, ConnectError, SqlState, DbError};
use io::{StreamWrapper, NegotiateSsl};
use message::BackendMessage::*;
use message::FrontendMessage::*;
use message::{FrontendMessage, BackendMessage, RowDescriptionEntry};
use message::{WriteMessage, ReadMessage};
use notification::{Notifications, Notification};
use rows::{Rows, LazyRows};
use stmt::{Statement, Column};
use types::{IsNull, Kind, Type, SessionInfo, Oid, Other};
use types::{ToSql, FromSql};
use url::Url;

#[macro_use]
mod macros;

mod md5;
mod message;
mod priv_io;
mod url;
mod util;
pub mod error;
pub mod io;
pub mod rows;
pub mod stmt;
pub mod types;
pub mod notification;

const TYPEINFO_QUERY: &'static str = "t";

/// A type alias of the result returned by many methods.
pub type Result<T> = result::Result<T, Error>;

/// Specifies the target server to connect to.
#[derive(Clone, Debug)]
pub enum ConnectTarget {
    /// Connect via TCP to the specified host.
    Tcp(String),
    /// Connect via a Unix domain socket in the specified directory.
    ///
    /// Only available on Unix platforms with the `unix_socket` feature.
    #[cfg(feature = "unix_socket")]
    Unix(PathBuf),
}

/// Authentication information.
#[derive(Clone, Debug)]
pub struct UserInfo {
    /// The username
    pub user: String,
    /// An optional password
    pub password: Option<String>,
}

/// Information necessary to open a new connection to a Postgres server.
#[derive(Clone, Debug)]
pub struct ConnectParams {
    /// The target server
    pub target: ConnectTarget,
    /// The target port.
    ///
    /// Defaults to 5432 if not specified.
    pub port: Option<u16>,
    /// The user to login as.
    ///
    /// `Connection::connect` requires a user but `cancel_query` does not.
    pub user: Option<UserInfo>,
    /// The database to connect to. Defaults the value of `user`.
    pub database: Option<String>,
    /// Runtime parameters to be passed to the Postgres backend.
    pub options: Vec<(String, String)>,
}

/// A trait implemented by types that can be converted into a `ConnectParams`.
pub trait IntoConnectParams {
    /// Converts the value of `self` into a `ConnectParams`.
    fn into_connect_params(self) -> result::Result<ConnectParams, Box<StdError + StdSync + Send>>;
}

impl IntoConnectParams for ConnectParams {
    fn into_connect_params(self) -> result::Result<ConnectParams, Box<StdError + StdSync + Send>> {
        Ok(self)
    }
}

impl<'a> IntoConnectParams for &'a str {
    fn into_connect_params(self) -> result::Result<ConnectParams, Box<StdError + StdSync + Send>> {
        match Url::parse(self) {
            Ok(url) => url.into_connect_params(),
            Err(err) => return Err(err.into()),
        }
    }
}

impl IntoConnectParams for Url {
    fn into_connect_params(self) -> result::Result<ConnectParams, Box<StdError + StdSync + Send>> {
        let Url {
            host,
            port,
            user,
            path: url::Path { mut path, query: options, .. },
            ..
        } = self;

        #[cfg(feature = "unix_socket")]
        fn make_unix(maybe_path: String)
                     -> result::Result<ConnectTarget, Box<StdError + StdSync + Send>> {
            Ok(ConnectTarget::Unix(PathBuf::from(maybe_path)))
        }
        #[cfg(not(feature = "unix_socket"))]
        fn make_unix(_: String) -> result::Result<ConnectTarget, Box<StdError + StdSync + Send>> {
            Err("unix socket support requires the `unix_socket` feature".into())
        }

        let maybe_path = try!(url::decode_component(&host));
        let target = if maybe_path.starts_with("/") {
            try!(make_unix(maybe_path))
        } else {
            ConnectTarget::Tcp(host)
        };

        let user = user.map(|url::UserInfo { user, pass }| {
            UserInfo {
                user: user,
                password: pass,
            }
        });

        let database = if path.is_empty() {
            None
        } else {
            // path contains the leading /
            path.remove(0);
            Some(path)
        };

        Ok(ConnectParams {
            target: target,
            port: port,
            user: user,
            database: database,
            options: options,
        })
    }
}

/// Trait for types that can handle Postgres notice messages
pub trait HandleNotice: Send {
    /// Handle a Postgres notice message
    fn handle_notice(&mut self, notice: DbError);
}

/// A notice handler which logs at the `info` level.
///
/// This is the default handler used by a `Connection`.
#[derive(Copy, Clone, Debug)]
pub struct LoggingNoticeHandler;

impl HandleNotice for LoggingNoticeHandler {
    fn handle_notice(&mut self, notice: DbError) {
        info!("{}: {}", notice.severity(), notice.message());
    }
}

/// Contains information necessary to cancel queries for a session.
#[derive(Copy, Clone, Debug)]
pub struct CancelData {
    /// The process ID of the session.
    pub process_id: u32,
    /// The secret key for the session.
    pub secret_key: u32,
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
/// # use postgres::{Connection, SslMode};
/// # use std::thread;
/// # let url = "";
/// let conn = Connection::connect(url, &SslMode::None).unwrap();
/// let cancel_data = conn.cancel_data();
/// thread::spawn(move || {
///     conn.execute("SOME EXPENSIVE QUERY", &[]).unwrap();
/// });
/// postgres::cancel_query(url, &SslMode::None, cancel_data).unwrap();
/// ```
pub fn cancel_query<T>(params: T,
                       ssl: &SslMode,
                       data: CancelData)
                       -> result::Result<(), ConnectError>
    where T: IntoConnectParams
{
    let params = try!(params.into_connect_params().map_err(ConnectError::BadConnectParams));
    let mut socket = try!(priv_io::initialize_stream(&params, ssl));

    try!(socket.write_message(&CancelRequest {
        code: message::CANCEL_CODE,
        process_id: data.process_id,
        secret_key: data.secret_key,
    }));
    try!(socket.flush());

    Ok(())
}

fn bad_response() -> std_io::Error {
    std_io::Error::new(std_io::ErrorKind::InvalidInput,
                       "the server returned an unexpected response")
}

fn desynchronized() -> std_io::Error {
    std_io::Error::new(std_io::ErrorKind::Other,
                       "communication with the server has desynchronized due to an earlier IO \
                        error")
}

/// An enumeration of transaction isolation levels.
///
/// See the [Postgres documentation](http://www.postgresql.org/docs/9.4/static/transaction-iso.html)
/// for full details on the semantics of each level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsolationLevel {
    /// The "read uncommitted" level.
    ///
    /// In current versions of Postgres, this behaves identically to
    /// `ReadCommitted`.
    ReadUncommitted,
    /// The "read committed" level.
    ///
    /// This is the default isolation level in Postgres.
    ReadCommitted,
    /// The "repeatable read" level.
    RepeatableRead,
    /// The "serializable" level.
    Serializable,
}

impl IsolationLevel {
    fn to_set_query(&self) -> &'static str {
        match *self {
            IsolationLevel::ReadUncommitted => {
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ UNCOMMITTED"
            }
            IsolationLevel::ReadCommitted => {
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL READ COMMITTED"
            }
            IsolationLevel::RepeatableRead => {
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL REPEATABLE READ"
            }
            IsolationLevel::Serializable => {
                "SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE"
            }
        }
    }

    fn parse(raw: &str) -> Result<IsolationLevel> {
        if raw.eq_ignore_ascii_case("READ UNCOMMITTED") {
            Ok(IsolationLevel::ReadUncommitted)
        } else if raw.eq_ignore_ascii_case("READ COMMITTED") {
            Ok(IsolationLevel::ReadCommitted)
        } else if raw.eq_ignore_ascii_case("REPEATABLE READ") {
            Ok(IsolationLevel::RepeatableRead)
        } else if raw.eq_ignore_ascii_case("SERIALIZABLE") {
            Ok(IsolationLevel::Serializable)
        } else {
            Err(Error::IoError(bad_response()))
        }
    }
}

/// Specifies the SSL support requested for a new connection.
pub enum SslMode {
    /// The connection will not use SSL.
    None,
    /// The connection will use SSL if the backend supports it.
    Prefer(Box<NegotiateSsl + std::marker::Sync + Send>),
    /// The connection must use SSL.
    Require(Box<NegotiateSsl + std::marker::Sync + Send>),
}

impl fmt::Debug for SslMode {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            SslMode::None => fmt.write_str("None"),
            SslMode::Prefer(..) => fmt.write_str("Prefer"),
            SslMode::Require(..) => fmt.write_str("Require"),
        }
    }
}

#[derive(Clone)]
struct CachedStatement {
    name: String,
    param_types: Vec<Type>,
    columns: Vec<Column>,
}

struct InnerConnection {
    stream: BufStream<Box<StreamWrapper>>,
    notice_handler: Box<HandleNotice>,
    notifications: VecDeque<Notification>,
    cancel_data: CancelData,
    unknown_types: HashMap<Oid, Type>,
    cached_statements: HashMap<String, CachedStatement>,
    parameters: HashMap<String, String>,
    next_stmt_id: u32,
    trans_depth: u32,
    desynchronized: bool,
    finished: bool,
}

impl Drop for InnerConnection {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish_inner();
        }
    }
}

impl InnerConnection {
    fn connect<T>(params: T, ssl: &SslMode) -> result::Result<InnerConnection, ConnectError>
        where T: IntoConnectParams
    {
        let params = try!(params.into_connect_params().map_err(ConnectError::BadConnectParams));
        let stream = try!(priv_io::initialize_stream(&params, ssl));

        let ConnectParams { user, database, mut options, .. } = params;

        let user = try!(user.ok_or(ConnectError::MissingUser));

        let mut conn = InnerConnection {
            stream: BufStream::new(stream),
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
        };

        options.push(("client_encoding".to_owned(), "UTF8".to_owned()));
        // Postgres uses the value of TimeZone as the time zone for TIMESTAMP
        // WITH TIME ZONE values. Timespec converts to GMT internally.
        options.push(("timezone".to_owned(), "GMT".to_owned()));
        // We have to clone here since we need the user again for auth
        options.push(("user".to_owned(), user.user.clone()));
        if let Some(database) = database {
            options.push(("database".to_owned(), database));
        }

        try!(conn.write_messages(&[StartupMessage {
                                       version: message::PROTOCOL_VERSION,
                                       parameters: &options,
                                   }]));

        try!(conn.handle_auth(user));

        loop {
            match try!(conn.read_message()) {
                BackendKeyData { process_id, secret_key } => {
                    conn.cancel_data.process_id = process_id;
                    conn.cancel_data.secret_key = secret_key;
                }
                ReadyForQuery { .. } => break,
                ErrorResponse { fields } => return DbError::new_connect(fields),
                _ => return Err(ConnectError::IoError(bad_response())),
            }
        }

        try!(conn.setup_typeinfo_query());

        Ok(conn)
    }

    #[cfg_attr(rustfmt, rustfmt_skip)]
    fn setup_typeinfo_query(&mut self) -> result::Result<(), ConnectError> {
        match self.raw_prepare(TYPEINFO_QUERY,
                               "SELECT t.typname, t.typelem, r.rngsubtype, n.nspname \
                                FROM pg_catalog.pg_type t \
                                LEFT OUTER JOIN pg_catalog.pg_range r ON \
                                    r.rngtypid = t.oid \
                                INNER JOIN pg_catalog.pg_namespace n ON \
                                    t.typnamespace = n.oid \
                                WHERE t.oid = $1") {
            Ok(..) => return Ok(()),
            Err(Error::IoError(e)) => return Err(ConnectError::IoError(e)),
            // Range types weren't added until Postgres 9.2, so pg_range may not exist
            Err(Error::DbError(ref e)) if e.code() == &SqlState::UndefinedTable => {}
            Err(Error::DbError(e)) => return Err(ConnectError::DbError(e)),
            _ => unreachable!(),
        }

        match self.raw_prepare(TYPEINFO_QUERY,
                               "SELECT t.typname, t.typelem, NULL::OID, n.nspname \
                                FROM pg_catalog.pg_type t \
                                INNER JOIN pg_catalog.pg_namespace n \
                                    ON t.typnamespace = n.oid \
                                WHERE t.oid = $1") {
            Ok(..) => Ok(()),
            Err(Error::IoError(e)) => Err(ConnectError::IoError(e)),
            Err(Error::DbError(e)) => Err(ConnectError::DbError(e)),
            _ => unreachable!(),
        }
    }

    fn write_messages(&mut self, messages: &[FrontendMessage]) -> std_io::Result<()> {
        debug_assert!(!self.desynchronized);
        for message in messages {
            try_desync!(self, self.stream.write_message(message));
        }
        Ok(try_desync!(self, self.stream.flush()))
    }

    fn read_message_with_notification(&mut self) -> std_io::Result<BackendMessage> {
        debug_assert!(!self.desynchronized);
        loop {
            match try_desync!(self, self.stream.read_message()) {
                NoticeResponse { fields } => {
                    if let Ok(err) = DbError::new_raw(fields) {
                        self.notice_handler.handle_notice(err);
                    }
                }
                ParameterStatus { parameter, value } => {
                    self.parameters.insert(parameter, value);
                }
                val => return Ok(val),
            }
        }
    }

    fn read_message_with_notification_timeout(&mut self,
                                              timeout: Duration)
                                              -> std::io::Result<Option<BackendMessage>> {
        debug_assert!(!self.desynchronized);
        loop {
            match try_desync!(self, self.stream.read_message_timeout(timeout)) {
                Some(NoticeResponse { fields }) => {
                    if let Ok(err) = DbError::new_raw(fields) {
                        self.notice_handler.handle_notice(err);
                    }
                }
                Some(ParameterStatus { parameter, value }) => {
                    self.parameters.insert(parameter, value);
                }
                val => return Ok(val),
            }
        }
    }

    fn read_message(&mut self) -> std_io::Result<BackendMessage> {
        loop {
            match try!(self.read_message_with_notification()) {
                NotificationResponse { pid, channel, payload } => {
                    self.notifications.push_back(Notification {
                        pid: pid,
                        channel: channel,
                        payload: payload,
                    })
                }
                val => return Ok(val),
            }
        }
    }

    fn handle_auth(&mut self, user: UserInfo) -> result::Result<(), ConnectError> {
        match try!(self.read_message()) {
            AuthenticationOk => return Ok(()),
            AuthenticationCleartextPassword => {
                let pass = try!(user.password.ok_or(ConnectError::MissingPassword));
                try!(self.write_messages(&[PasswordMessage { password: &pass }]));
            }
            AuthenticationMD5Password { salt } => {
                let pass = try!(user.password.ok_or(ConnectError::MissingPassword));
                let mut hasher = Md5::new();
                let _ = hasher.input(pass.as_bytes());
                let _ = hasher.input(user.user.as_bytes());
                let output = hasher.result_str();
                hasher.reset();
                let _ = hasher.input(output.as_bytes());
                let _ = hasher.input(&salt);
                let output = format!("md5{}", hasher.result_str());
                try!(self.write_messages(&[PasswordMessage { password: &output }]));
            }
            AuthenticationKerberosV5 |
            AuthenticationSCMCredential |
            AuthenticationGSS |
            AuthenticationSSPI => return Err(ConnectError::UnsupportedAuthentication),
            ErrorResponse { fields } => return DbError::new_connect(fields),
            _ => return Err(ConnectError::IoError(bad_response())),
        }

        match try!(self.read_message()) {
            AuthenticationOk => Ok(()),
            ErrorResponse { fields } => return DbError::new_connect(fields),
            _ => return Err(ConnectError::IoError(bad_response())),
        }
    }

    fn set_notice_handler(&mut self, handler: Box<HandleNotice>) -> Box<HandleNotice> {
        mem::replace(&mut self.notice_handler, handler)
    }

    fn raw_prepare(&mut self, stmt_name: &str, query: &str) -> Result<(Vec<Type>, Vec<Column>)> {
        debug!("preparing query with name `{}`: {}", stmt_name, query);

        try!(self.write_messages(&[Parse {
                                       name: stmt_name,
                                       query: query,
                                       param_types: &[],
                                   },
                                   Describe {
                                       variant: b'S',
                                       name: stmt_name,
                                   },
                                   Sync]));

        match try!(self.read_message()) {
            ParseComplete => {}
            ErrorResponse { fields } => {
                try!(self.wait_for_ready());
                return DbError::new(fields);
            }
            _ => bad_response!(self),
        }

        let raw_param_types = match try!(self.read_message()) {
            ParameterDescription { types } => types,
            _ => bad_response!(self),
        };

        let raw_columns = match try!(self.read_message()) {
            RowDescription { descriptions } => descriptions,
            NoData => vec![],
            _ => bad_response!(self),
        };

        try!(self.wait_for_ready());

        let mut param_types = vec![];
        for oid in raw_param_types {
            param_types.push(try!(self.get_type(oid)));
        }

        let mut columns = vec![];
        for RowDescriptionEntry { name, type_oid, .. } in raw_columns {
            columns.push(Column::new(name, try!(self.get_type(type_oid))));
        }

        Ok((param_types, columns))
    }

    fn make_stmt_name(&mut self) -> String {
        let stmt_name = format!("s{}", self.next_stmt_id);
        self.next_stmt_id += 1;
        stmt_name
    }

    fn prepare<'a>(&mut self, query: &str, conn: &'a Connection) -> Result<Statement<'a>> {
        let stmt_name = self.make_stmt_name();
        let (param_types, columns) = try!(self.raw_prepare(&stmt_name, query));
        Ok(Statement::new(conn, stmt_name, param_types, columns, Cell::new(0), false))
    }

    fn prepare_cached<'a>(&mut self, query: &str, conn: &'a Connection) -> Result<Statement<'a>> {
        let stmt = self.cached_statements.get(query).cloned();

        let CachedStatement { name, param_types, columns } = match stmt {
            Some(stmt) => stmt,
            None => {
                let stmt_name = self.make_stmt_name();
                let (param_types, columns) = try!(self.raw_prepare(&stmt_name, query));
                let stmt = CachedStatement {
                    name: stmt_name,
                    param_types: param_types,
                    columns: columns,
                };
                self.cached_statements.insert(query.to_owned(), stmt.clone());
                stmt
            }
        };

        Ok(Statement::new(conn, name, param_types, columns, Cell::new(0), true))
    }

    fn close_statement(&mut self, name: &str, type_: u8) -> Result<()> {
        try!(self.write_messages(&[Close {
                                       variant: type_,
                                       name: name,
                                   },
                                   Sync]));
        let resp = match try!(self.read_message()) {
            CloseComplete => Ok(()),
            ErrorResponse { fields } => DbError::new(fields),
            _ => bad_response!(self),
        };
        try!(self.wait_for_ready());
        resp
    }

    fn get_type(&mut self, oid: Oid) -> Result<Type> {
        if let Some(ty) = Type::from_oid(oid) {
            return Ok(ty);
        }

        if let Some(ty) = self.unknown_types.get(&oid) {
            return Ok(ty.clone());
        }

        // Ew @ doing this manually :(
        let mut buf = vec![];
        let value = match try!(oid.to_sql_checked(&Type::Oid, &mut buf, &SessionInfo::new(self))) {
            IsNull::Yes => None,
            IsNull::No => Some(buf),
        };
        try!(self.write_messages(&[Bind {
                                       portal: "",
                                       statement: TYPEINFO_QUERY,
                                       formats: &[1],
                                       values: &[value],
                                       result_formats: &[1],
                                   },
                                   Execute {
                                       portal: "",
                                       max_rows: 0,
                                   },
                                   Sync]));
        match try!(self.read_message()) {
            BindComplete => {}
            ErrorResponse { fields } => {
                try!(self.wait_for_ready());
                return DbError::new(fields);
            }
            _ => bad_response!(self),
        }
        let (name, elem_oid, rngsubtype, schema): (String, Oid, Option<Oid>, String) =
            match try!(self.read_message()) {
                DataRow { row } => {
                    let ctx = SessionInfo::new(self);
                    (try!(FromSql::from_sql_nullable(&Type::Name,
                                                     row[0].as_ref().map(|r| &**r).as_mut(),
                                                     &ctx)),
                     try!(FromSql::from_sql_nullable(&Type::Oid,
                                                     row[1].as_ref().map(|r| &**r).as_mut(),
                                                     &ctx)),
                     try!(FromSql::from_sql_nullable(&Type::Oid,
                                                     row[2].as_ref().map(|r| &**r).as_mut(),
                                                     &ctx)),
                     try!(FromSql::from_sql_nullable(&Type::Name,
                                                     row[3].as_ref().map(|r| &**r).as_mut(),
                                                     &ctx)))
                }
                ErrorResponse { fields } => {
                    try!(self.wait_for_ready());
                    return DbError::new(fields);
                }
                _ => bad_response!(self),
            };
        match try!(self.read_message()) {
            CommandComplete { .. } => {}
            ErrorResponse { fields } => {
                try!(self.wait_for_ready());
                return DbError::new(fields);
            }
            _ => bad_response!(self),
        }
        try!(self.wait_for_ready());

        let kind = if elem_oid != 0 {
            Kind::Array(try!(self.get_type(elem_oid)))
        } else {
            match rngsubtype {
                Some(oid) => Kind::Range(try!(self.get_type(oid))),
                None => Kind::Simple,
            }
        };

        let type_ = Type::Other(Box::new(Other::new(name, oid, kind, schema)));
        self.unknown_types.insert(oid, type_.clone());
        Ok(type_)
    }

    fn is_desynchronized(&self) -> bool {
        self.desynchronized
    }

    fn wait_for_ready(&mut self) -> Result<()> {
        match try!(self.read_message()) {
            ReadyForQuery { .. } => Ok(()),
            _ => bad_response!(self),
        }
    }

    fn quick_query(&mut self, query: &str) -> Result<Vec<Vec<Option<String>>>> {
        check_desync!(self);
        debug!("executing query: {}", query);
        try!(self.write_messages(&[Query { query: query }]));

        let mut result = vec![];
        loop {
            match try!(self.read_message()) {
                ReadyForQuery { .. } => break,
                DataRow { row } => {
                    result.push(row.into_iter()
                                   .map(|opt| {
                                       opt.map(|b| String::from_utf8_lossy(&b).into_owned())
                                   })
                                   .collect());
                }
                CopyInResponse { .. } |
                CopyOutResponse { .. } => {
                    try!(self.write_messages(&[CopyFail {
                                                   message: "COPY queries cannot be directly \
                                                             executed",
                                               },
                                               Sync]));
                }
                ErrorResponse { fields } => {
                    try!(self.wait_for_ready());
                    return DbError::new(fields);
                }
                _ => {}
            }
        }
        Ok(result)
    }

    fn finish_inner(&mut self) -> Result<()> {
        check_desync!(self);
        try!(self.write_messages(&[Terminate]));
        Ok(())
    }
}

fn _ensure_send() {
    fn _is_send<T: Send>() {
    }
    _is_send::<Connection>();
}

/// A connection to a Postgres database.
pub struct Connection {
    conn: RefCell<InnerConnection>,
}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let conn = self.conn.borrow();
        fmt.debug_struct("Connection")
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
    /// Connection via Unix sockets is supported with the `unix_socket`
    /// feature. To connect to the server via Unix sockets, `host` should be
    /// set to the absolute path of the directory containing the socket file.
    /// Since `/` is a reserved character in URLs, the path should be URL
    /// encoded. If the path contains non-UTF 8 characters, a `ConnectParams`
    /// struct should be created manually and passed in. Note that Postgres
    /// does not support SSL over Unix sockets.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// use postgres::{Connection, SslMode};
    ///
    /// let url = "postgresql://postgres:hunter2@localhost:2994/foodb";
    /// let conn = Connection::connect(url, &SslMode::None).unwrap();
    /// ```
    ///
    /// ```rust,no_run
    /// use postgres::{Connection, SslMode};
    ///
    /// let url = "postgresql://postgres@%2Frun%2Fpostgres";
    /// let conn = Connection::connect(url, &SslMode::None).unwrap();
    /// ```
    ///
    /// ```rust,no_run
    /// use postgres::{Connection, UserInfo, ConnectParams, SslMode, ConnectTarget};
    ///
    /// # #[cfg(feature = "unix_socket")]
    /// # fn f() {
    /// # let some_crazy_path = Path::new("");
    /// let params = ConnectParams {
    ///     target: ConnectTarget::Unix(some_crazy_path),
    ///     port: None,
    ///     user: Some(UserInfo {
    ///         user: "postgres".to_owned(),
    ///         password: None
    ///     }),
    ///     database: None,
    ///     options: vec![],
    /// };
    /// let conn = Connection::connect(params, &SslMode::None).unwrap();
    /// # }
    /// ```
    pub fn connect<T>(params: T, ssl: &SslMode) -> result::Result<Connection, ConnectError>
        where T: IntoConnectParams
    {
        InnerConnection::connect(params, ssl).map(|conn| Connection { conn: RefCell::new(conn) })
    }

    /// Sets the notice handler for the connection, returning the old handler.
    pub fn set_notice_handler(&self, handler: Box<HandleNotice>) -> Box<HandleNotice> {
        self.conn.borrow_mut().set_notice_handler(handler)
    }

    /// Returns a structure providing access to asynchronous notifications.
    ///
    /// Use the `LISTEN` command to register this connection for notifications.
    pub fn notifications<'a>(&'a self) -> Notifications<'a> {
        Notifications::new(self)
    }

    /// Creates a new prepared statement.
    ///
    /// A statement may contain parameters, specified by `$n` where `n` is the
    /// index of the parameter in the list provided at execution time,
    /// 1-indexed.
    ///
    /// The statement is associated with the connection that created it and may
    /// not outlive that connection.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let x = 10i32;
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// let stmt = conn.prepare("SELECT foo FROM bar WHERE baz = $1").unwrap();
    /// for row in stmt.query(&[&x]).unwrap() {
    ///     let foo: String = row.get(0);
    ///     println!("foo: {}", foo);
    /// }
    /// ```
    pub fn prepare<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        self.conn.borrow_mut().prepare(query, self)
    }

    /// Creates a cached prepared statement.
    ///
    /// Like `prepare`, except that the statement is only prepared once over
    /// the lifetime of the connection and then cached. If the same statement
    /// is going to be used frequently, caching it can improve performance by
    /// reducing the number of round trips to the Postgres backend.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let x = 10i32;
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// let stmt = conn.prepare_cached("SELECT foo FROM bar WHERE baz = $1").unwrap();
    /// for row in stmt.query(&[&x]).unwrap() {
    ///     let foo: String = row.get(0);
    ///     println!("foo: {}", foo);
    /// }
    /// ```
    pub fn prepare_cached<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        self.conn.borrow_mut().prepare_cached(query, self)
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
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// let trans = conn.transaction().unwrap();
    /// trans.execute("UPDATE foo SET bar = 10", &[]).unwrap();
    /// // ...
    ///
    /// trans.commit().unwrap();
    /// ```
    pub fn transaction<'a>(&'a self) -> Result<Transaction<'a>> {
        let mut conn = self.conn.borrow_mut();
        check_desync!(conn);
        assert!(conn.trans_depth == 0,
                "`transaction` must be called on the active transaction");
        try!(conn.quick_query("BEGIN"));
        conn.trans_depth += 1;
        Ok(Transaction {
            conn: self,
            commit: Cell::new(false),
            depth: 1,
            finished: false,
        })
    }

    /// Sets the isolation level which will be used for future transactions.
    ///
    /// This is a simple wrapper around `SET TRANSACTION ISOLATION LEVEL ...`.
    ///
    /// # Note
    ///
    /// This will not change the behavior of an active transaction.
    pub fn set_transaction_isolation(&self, level: IsolationLevel) -> Result<()> {
        self.batch_execute(level.to_set_query())
    }

    /// Returns the isolation level which will be used for future transactions.
    ///
    /// This is a simple wrapper around `SHOW TRANSACTION ISOLATION LEVEL`.
    pub fn transaction_isolation(&self) -> Result<IsolationLevel> {
        let mut conn = self.conn.borrow_mut();
        check_desync!(conn);
        let result = try!(conn.quick_query("SHOW TRANSACTION ISOLATION LEVEL"));
        IsolationLevel::parse(result[0][0].as_ref().unwrap())
    }

    /// A convenience function for queries that are only run once.
    ///
    /// If an error is returned, it could have come from either the preparation
    /// or execution of the statement.
    ///
    /// On success, returns the number of rows modified or 0 if not applicable.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    pub fn execute(&self, query: &str, params: &[&ToSql]) -> Result<u64> {
        let (param_types, columns) = try!(self.conn.borrow_mut().raw_prepare("", query));
        let stmt = Statement::new(self,
                                  "".to_owned(),
                                  param_types,
                                  columns,
                                  Cell::new(0),
                                  true);
        stmt.execute(params)
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
    /// # use postgres::{Connection, SslMode, Result};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
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
        self.conn.borrow_mut().quick_query(query).map(|_| ())
    }

    /// Returns information used to cancel pending queries.
    ///
    /// Used with the `cancel_query` function. The object returned can be used
    /// to cancel any query executed by the connection it was created from.
    pub fn cancel_data(&self) -> CancelData {
        self.conn.borrow().cancel_data
    }

    /// Returns the value of the specified Postgres backend parameter, such as
    /// `timezone` or `server_version`.
    pub fn parameter(&self, param: &str) -> Option<String> {
        self.conn.borrow().parameters.get(param).cloned()
    }

    /// Returns whether or not the stream has been desynchronized due to an
    /// error in the communication channel with the server.
    ///
    /// If this has occurred, all further queries will immediately return an
    /// error.
    pub fn is_desynchronized(&self) -> bool {
        self.conn.borrow().is_desynchronized()
    }

    /// Determines if the `Connection` is currently "active", that is, if there
    /// are no active transactions.
    ///
    /// The `transaction` method can only be called on the active `Connection`
    /// or `Transaction`.
    pub fn is_active(&self) -> bool {
        self.conn.borrow().trans_depth == 0
    }

    /// Consumes the connection, closing it.
    ///
    /// Functionally equivalent to the `Drop` implementation for `Connection`
    /// except that it returns any error encountered to the caller.
    pub fn finish(self) -> Result<()> {
        let mut conn = self.conn.borrow_mut();
        conn.finished = true;
        conn.finish_inner()
    }
}

/// Represents a transaction on a database connection.
///
/// The transaction will roll back by default.
pub struct Transaction<'conn> {
    conn: &'conn Connection,
    depth: u32,
    commit: Cell<bool>,
    finished: bool,
}

impl<'a> fmt::Debug for Transaction<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Transaction")
           .field("commit", &self.commit.get())
           .field("depth", &self.depth)
           .finish()
    }
}

impl<'conn> Drop for Transaction<'conn> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish_inner();
        }
    }
}

impl<'conn> Transaction<'conn> {
    fn finish_inner(&mut self) -> Result<()> {
        let mut conn = self.conn.conn.borrow_mut();
        debug_assert!(self.depth == conn.trans_depth);
        let query = match (self.commit.get(), self.depth != 1) {
            (false, true) => "ROLLBACK TO sp",
            (false, false) => "ROLLBACK",
            (true, true) => "RELEASE sp",
            (true, false) => "COMMIT",
        };
        conn.trans_depth -= 1;
        conn.quick_query(query).map(|_| ())
    }

    /// Like `Connection::prepare`.
    pub fn prepare(&self, query: &str) -> Result<Statement<'conn>> {
        self.conn.prepare(query)
    }

    /// Like `Connection::prepare_cached`.
    ///
    /// Note that the statement will be cached for the duration of the
    /// connection, not just the duration of this transaction.
    pub fn prepare_cached(&self, query: &str) -> Result<Statement<'conn>> {
        self.conn.prepare_cached(query)
    }

    /// Like `Connection::execute`.
    pub fn execute(&self, query: &str, params: &[&ToSql]) -> Result<u64> {
        self.conn.execute(query, params)
    }

    /// Like `Connection::batch_execute`.
    pub fn batch_execute(&self, query: &str) -> Result<()> {
        self.conn.batch_execute(query)
    }

    /// Like `Connection::transaction`.
    ///
    /// # Panics
    ///
    /// Panics if there is an active nested transaction.
    pub fn transaction<'a>(&'a self) -> Result<Transaction<'a>> {
        let mut conn = self.conn.conn.borrow_mut();
        check_desync!(conn);
        assert!(conn.trans_depth == self.depth,
                "`transaction` may only be called on the active transaction");
        try!(conn.quick_query("SAVEPOINT sp"));
        conn.trans_depth += 1;
        Ok(Transaction {
            conn: self.conn,
            commit: Cell::new(false),
            depth: self.depth + 1,
            finished: false,
        })
    }

    /// Returns a reference to the `Transaction`'s `Connection`.
    pub fn connection(&self) -> &'conn Connection {
        self.conn
    }

    /// Like `Connection::is_active`.
    pub fn is_active(&self) -> bool {
        self.conn.conn.borrow().trans_depth == self.depth
    }

    /// Determines if the transaction is currently set to commit or roll back.
    pub fn will_commit(&self) -> bool {
        self.commit.get()
    }

    /// Sets the transaction to commit at its completion.
    pub fn set_commit(&self) {
        self.commit.set(true);
    }

    /// Sets the transaction to roll back at its completion.
    pub fn set_rollback(&self) {
        self.commit.set(false);
    }

    /// A convenience method which consumes and commits a transaction.
    pub fn commit(self) -> Result<()> {
        self.set_commit();
        self.finish()
    }

    /// Consumes the transaction, commiting or rolling it back as appropriate.
    ///
    /// Functionally equivalent to the `Drop` implementation of `Transaction`
    /// except that it returns any error to the caller.
    pub fn finish(mut self) -> Result<()> {
        self.finished = true;
        self.finish_inner()
    }
}

fn read_rows(conn: &mut InnerConnection, buf: &mut VecDeque<Vec<Option<Vec<u8>>>>) -> Result<bool> {
    let more_rows;
    loop {
        match try!(conn.read_message()) {
            EmptyQueryResponse | CommandComplete { .. } => {
                more_rows = false;
                break;
            }
            PortalSuspended => {
                more_rows = true;
                break;
            }
            DataRow { row } => buf.push_back(row),
            ErrorResponse { fields } => {
                try!(conn.wait_for_ready());
                return DbError::new(fields);
            }
            CopyInResponse { .. } => {
                try!(conn.write_messages(&[CopyFail {
                                               message: "COPY queries cannot be directly executed",
                                           },
                                           Sync]));
            }
            CopyOutResponse { .. } => {
                loop {
                    match try!(conn.read_message()) {
                        ReadyForQuery { .. } => break,
                        _ => {}
                    }
                }
                return Err(Error::IoError(std_io::Error::new(std_io::ErrorKind::InvalidInput,
                                                             "COPY queries cannot be directly \
                                                              executed")));
            }
            _ => {
                conn.desynchronized = true;
                return Err(Error::IoError(bad_response()));
            }
        }
    }
    try!(conn.wait_for_ready());
    Ok(more_rows)
}

/// A trait allowing abstraction over connections and transactions
pub trait GenericConnection {
    /// Like `Connection::prepare`.
    fn prepare<'a>(&'a self, query: &str) -> Result<Statement<'a>>;

    /// Like `Connection::prepare_cached`.
    fn prepare_cached<'a>(&'a self, query: &str) -> Result<Statement<'a>>;

    /// Like `Connection::execute`.
    fn execute(&self, query: &str, params: &[&ToSql]) -> Result<u64>;

    /// Like `Connection::transaction`.
    fn transaction<'a>(&'a self) -> Result<Transaction<'a>>;

    /// Like `Connection::batch_execute`.
    fn batch_execute(&self, query: &str) -> Result<()>;

    /// Like `Connection::is_active`.
    fn is_active(&self) -> bool;
}

impl GenericConnection for Connection {
    fn prepare<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        self.prepare(query)
    }

    fn prepare_cached<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        self.prepare_cached(query)
    }

    fn execute(&self, query: &str, params: &[&ToSql]) -> Result<u64> {
        self.execute(query, params)
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
    fn prepare<'b>(&'b self, query: &str) -> Result<Statement<'b>> {
        self.prepare(query)
    }

    fn prepare_cached<'b>(&'b self, query: &str) -> Result<Statement<'b>> {
        self.prepare_cached(query)
    }

    fn execute(&self, query: &str, params: &[&ToSql]) -> Result<u64> {
        self.execute(query, params)
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

trait OtherNew {
    fn new(name: String, oid: Oid, kind: Kind, schema: String) -> Other;
}

trait DbErrorNew {
    fn new_raw(fields: Vec<(u8, String)>) -> result::Result<DbError, ()>;
    fn new_connect<T>(fields: Vec<(u8, String)>) -> result::Result<T, ConnectError>;
    fn new<T>(fields: Vec<(u8, String)>) -> Result<T>;
}

trait RowsNew<'a> {
    fn new(stmt: &'a Statement<'a>, data: Vec<Vec<Option<Vec<u8>>>>) -> Rows<'a>;
}

trait LazyRowsNew<'trans, 'stmt> {
    fn new(stmt: &'stmt Statement<'stmt>,
           data: VecDeque<Vec<Option<Vec<u8>>>>,
           name: String,
           row_limit: i32,
           more_rows: bool,
           finished: bool,
           trans: &'trans Transaction<'trans>)
           -> LazyRows<'trans, 'stmt>;
}

trait SessionInfoNew<'a> {
    fn new(conn: &'a InnerConnection) -> SessionInfo<'a>;
}

trait StatementInternals<'conn> {
    fn new(conn: &'conn Connection,
           name: String,
           param_types: Vec<Type>,
           columns: Vec<Column>,
           next_portal_id: Cell<u32>,
           finished: bool)
           -> Statement<'conn>;

    fn conn(&self) -> &'conn Connection;
}

trait ColumnNew {
    fn new(name: String, type_: Type) -> Column;
}

trait NotificationsNew<'conn> {
    fn new(conn: &'conn Connection) -> Notifications<'conn>;
}

//! Rust-Postgres is a pure-Rust frontend for the popular PostgreSQL database. It
//! exposes a high level interface in the vein of JDBC or Go's `database/sql`
//! package.
//!
//! ```rust,no_run
//! # #![allow(unstable)]
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
//!         name: "Steven".to_string(),
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
#![doc(html_root_url="https://sfackler.github.io/rust-postgres/doc/v0.9.1")]
#![warn(missing_docs)]

extern crate bufstream;
extern crate byteorder;
extern crate crypto;
#[macro_use]
extern crate log;
extern crate phf;
extern crate rustc_serialize as serialize;
#[cfg(feature = "unix_socket")]
extern crate unix_socket;
extern crate debug_builders;

use bufstream::BufStream;
use crypto::digest::Digest;
use crypto::md5::Md5;
use debug_builders::DebugStruct;
use std::ascii::AsciiExt;
use std::borrow::ToOwned;
use std::cell::{Cell, RefCell};
use std::collections::{VecDeque, HashMap};
use std::fmt;
use std::iter::IntoIterator;
use std::io as std_io;
use std::io::prelude::*;
use std::mem;
use std::result;
#[cfg(feature = "unix_socket")]
use std::path::PathBuf;

use error::{Error, ConnectError, SqlState, DbError};
use types::{ToSql, FromSql};
use io::{StreamWrapper, NegotiateSsl};
use types::{IsNull, Kind, Type, SessionInfo, Oid, Other};
use message::BackendMessage::*;
use message::FrontendMessage::*;
use message::{FrontendMessage, BackendMessage, RowDescriptionEntry};
use message::{WriteMessage, ReadMessage};
use url::Url;
use rows::{Rows, LazyRows};

#[macro_use]
mod macros;

pub mod error;
pub mod io;
mod message;
mod priv_io;
mod url;
mod util;
pub mod types;
pub mod rows;

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
    Unix(PathBuf)
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
    fn into_connect_params(self) -> result::Result<ConnectParams, ConnectError>;
}

impl IntoConnectParams for ConnectParams {
    fn into_connect_params(self) -> result::Result<ConnectParams, ConnectError> {
        Ok(self)
    }
}

impl<'a> IntoConnectParams for &'a str {
    fn into_connect_params(self) -> result::Result<ConnectParams, ConnectError> {
        match Url::parse(self) {
            Ok(url) => url.into_connect_params(),
            Err(err) => return Err(ConnectError::InvalidUrl(err)),
        }
    }
}

impl IntoConnectParams for Url {
    fn into_connect_params(self) -> result::Result<ConnectParams, ConnectError> {
        let Url {
            host,
            port,
            user,
            path: url::Path { mut path, query: options, .. },
            ..
        } = self;

        #[cfg(feature = "unix_socket")]
        fn make_unix(maybe_path: String) -> result::Result<ConnectTarget, ConnectError> {
            Ok(ConnectTarget::Unix(PathBuf::from(&maybe_path)))
        }
        #[cfg(not(feature = "unix_socket"))]
        fn make_unix(_: String) -> result::Result<ConnectTarget, ConnectError> {
            Err(ConnectError::InvalidUrl("unix socket support requires the `unix_socket` feature"
                                         .to_string()))
        }

        let maybe_path = try!(url::decode_component(&host).map_err(ConnectError::InvalidUrl));
        let target = if maybe_path.starts_with("/") {
            try!(make_unix(maybe_path))
        } else {
            ConnectTarget::Tcp(host)
        };

        let user = user.map(|url::UserInfo { user, pass }| {
            UserInfo { user: user, password: pass }
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

/// An asynchronous notification.
#[derive(Clone, Debug)]
pub struct Notification {
    /// The process ID of the notifying backend process.
    pub pid: u32,
    /// The name of the channel that the notify has been raised on.
    pub channel: String,
    /// The "payload" string passed from the notifying process.
    pub payload: String,
}

/// An iterator over asynchronous notifications.
pub struct Notifications<'conn> {
    conn: &'conn Connection
}

impl<'a> fmt::Debug for Notifications<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        DebugStruct::new(fmt, "Notifications")
            .field("pending", &self.conn.conn.borrow().notifications.len())
            .finish()
    }
}

impl<'conn> Iterator for Notifications<'conn> {
    type Item = Notification;

    /// Returns the oldest pending notification or `None` if there are none.
    ///
    /// ## Note
    ///
    /// `next` may return `Some` notification after returning `None` if a new
    /// notification was received.
    fn next(&mut self) -> Option<Notification> {
        self.conn.conn.borrow_mut().notifications.pop_front()
    }
}

impl<'conn> Notifications<'conn> {
    /// Returns the oldest pending notification.
    ///
    /// If no notifications are pending, blocks until one arrives.
    pub fn next_block(&mut self) -> Result<Notification> {
        if let Some(notification) = self.next() {
            return Ok(notification);
        }

        let mut conn = self.conn.conn.borrow_mut();
        check_desync!(conn);
        match try!(conn.read_message_with_notification()) {
            NotificationResponse { pid, channel, payload } => {
                Ok(Notification {
                    pid: pid,
                    channel: channel,
                    payload: payload
                })
            }
            _ => unreachable!()
        }
    }

    /*
    /// Returns the oldest pending notification
    ///
    /// If no notifications are pending, blocks for up to `timeout` time, after
    /// which `None` is returned.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # #![allow(unstable)]
    /// use std::old_io::{IoError, IoErrorKind};
    /// use std::time::Duration;
    ///
    /// use postgres::Error;
    ///
    /// # let conn = postgres::Connection::connect("", &postgres::SslMode::None).unwrap();
    /// match conn.notifications().next_block_for(Duration::seconds(2)) {
    ///     Some(Ok(notification)) => println!("notification: {}", notification.payload),
    ///     Some(Err(e)) => println!("Error: {:?}", e),
    ///     None => println!("Wait for notification timed out"),
    /// }
    /// ```
    pub fn next_block_for(&mut self, timeout: Duration) -> Option<Result<Notification>> {
        if let Some(notification) = self.next() {
            return Some(Ok(notification));
        }

        let mut conn = self.conn.conn.borrow_mut();
        if conn.desynchronized {
            return Some(Err(Error::StreamDesynchronized));
        }

        let end = SteadyTime::now() + timeout;
        loop {
            let timeout = max(Duration::zero(), end - SteadyTime::now()).num_milliseconds() as u64;
            conn.stream.set_read_timeout(Some(timeout));
            match conn.read_one_message() {
                Ok(Some(NotificationResponse { pid, channel, payload })) => {
                    return Some(Ok(Notification {
                        pid: pid,
                        channel: channel,
                        payload: payload
                    }))
                }
                Ok(Some(_)) => unreachable!(),
                Ok(None) => {}
                Err(IoError { kind: IoErrorKind::TimedOut, .. }) => {
                    conn.desynchronized = false;
                    return None;
                }
                Err(e) => return Some(Err(Error::IoError(e))),
            }
        }
    }
    */
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
/// ## Example
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
/// # let _ =
/// postgres::cancel_query(url, &SslMode::None, cancel_data);
/// ```
pub fn cancel_query<T>(params: T, ssl: &SslMode, data: CancelData)
                          -> result::Result<(), ConnectError>
        where T: IntoConnectParams {
    let params = try!(params.into_connect_params());
    let mut socket = try!(priv_io::initialize_stream(&params, ssl));

    try!(socket.write_message(&CancelRequest {
        code: message::CANCEL_CODE,
        process_id: data.process_id,
        secret_key: data.secret_key
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
                       "communication with the server has desynchronized due to an earlier IO error")
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
    Prefer(Box<NegotiateSsl+std::marker::Sync+Send>),
    /// The connection must use SSL.
    Require(Box<NegotiateSsl+std::marker::Sync+Send>),
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

trait SessionInfoNew<'a> {
    fn new(conn: &'a InnerConnection) -> SessionInfo<'a>;
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
            where T: IntoConnectParams {
        let params = try!(params.into_connect_params());
        let stream = try!(priv_io::initialize_stream(&params, ssl));

        let ConnectParams { user, database, mut options, .. } = params;

        let user = try!(user.ok_or(ConnectError::MissingUser));

        let mut conn = InnerConnection {
            stream: BufStream::new(stream),
            next_stmt_id: 0,
            notice_handler: Box::new(LoggingNoticeHandler),
            notifications: VecDeque::new(),
            cancel_data: CancelData { process_id: 0, secret_key: 0 },
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
            parameters: &options
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

    fn setup_typeinfo_query(&mut self) -> result::Result<(), ConnectError> {
        match self.raw_prepare(TYPEINFO_QUERY,
                               "SELECT t.typname, t.typelem, r.rngsubtype \
                                FROM pg_catalog.pg_type t \
                                LEFT OUTER JOIN pg_catalog.pg_range r \
                                    ON r.rngtypid = t.oid \
                                WHERE t.oid = $1") {
            Ok(..) => return Ok(()),
            Err(Error::IoError(e)) => return Err(ConnectError::IoError(e)),
            // Range types weren't added until Postgres 9.2, so pg_range may not exist
            Err(Error::DbError(ref e)) if e.code() == &SqlState::UndefinedTable => {}
            Err(Error::DbError(e)) => return Err(ConnectError::DbError(e)),
            _ => unreachable!()
        }

        match self.raw_prepare(TYPEINFO_QUERY,
                               "SELECT typname, typelem, NULL::OID \
                                FROM pg_catalog.pg_type \
                                WHERE oid = $1") {
            Ok(..) => Ok(()),
            Err(Error::IoError(e)) => Err(ConnectError::IoError(e)),
            Err(Error::DbError(e)) => Err(ConnectError::DbError(e)),
            _ => unreachable!()
        }
    }

    fn write_messages(&mut self, messages: &[FrontendMessage]) -> std_io::Result<()> {
        debug_assert!(!self.desynchronized);
        for message in messages {
            try_desync!(self, self.stream.write_message(message));
        }
        Ok(try_desync!(self, self.stream.flush()))
    }

    fn read_one_message(&mut self) -> std_io::Result<Option<BackendMessage>> {
        debug_assert!(!self.desynchronized);
        match try_desync!(self, self.stream.read_message()) {
            NoticeResponse { fields } => {
                if let Ok(err) = DbError::new_raw(fields) {
                    self.notice_handler.handle_notice(err);
                }
                Ok(None)
            }
            ParameterStatus { parameter, value } => {
                self.parameters.insert(parameter, value);
                Ok(None)
            }
            val => Ok(Some(val))
        }
    }

    fn read_message_with_notification(&mut self) -> std_io::Result<BackendMessage> {
        loop {
            if let Some(msg) = try!(self.read_one_message()) {
                return Ok(msg);
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
                        payload: payload
                    })
                }
                val => return Ok(val)
            }
        }
    }

    fn handle_auth(&mut self, user: UserInfo) -> result::Result<(), ConnectError> {
        match try!(self.read_message()) {
            AuthenticationOk => return Ok(()),
            AuthenticationCleartextPassword => {
                let pass = try!(user.password.ok_or(ConnectError::MissingPassword));
                try!(self.write_messages(&[PasswordMessage {
                        password: &pass,
                    }]));
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
                try!(self.write_messages(&[PasswordMessage {
                        password: &output
                    }]));
            }
            AuthenticationKerberosV5
            | AuthenticationSCMCredential
            | AuthenticationGSS
            | AuthenticationSSPI => return Err(ConnectError::UnsupportedAuthentication),
            ErrorResponse { fields } => return DbError::new_connect(fields),
            _ => return Err(ConnectError::IoError(bad_response()))
        }

        match try!(self.read_message()) {
            AuthenticationOk => Ok(()),
            ErrorResponse { fields } => return DbError::new_connect(fields),
            _ => return Err(ConnectError::IoError(bad_response()))
        }
    }

    fn set_notice_handler(&mut self, handler: Box<HandleNotice>) -> Box<HandleNotice> {
        mem::replace(&mut self.notice_handler, handler)
    }

    fn raw_prepare(&mut self, stmt_name: &str, query: &str) -> Result<(Vec<Type>, Vec<Column>)> {
        debug!("preparing query with name `{}`: {}", stmt_name, query);

        try!(self.write_messages(&[
            Parse {
                name: stmt_name,
                query: query,
                param_types: &[]
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
            _ => bad_response!(self)
        };

        try!(self.wait_for_ready());

        let mut param_types = vec![];
        for oid in raw_param_types {
            param_types.push(try!(self.get_type(oid)));
        }

        let mut columns = vec![];
        for RowDescriptionEntry { name, type_oid, .. } in raw_columns {
            columns.push(Column {
                name: name,
                type_: try!(self.get_type(type_oid)),
            });
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
        Ok(Statement {
            conn: conn,
            name: stmt_name,
            param_types: param_types,
            columns: columns,
            next_portal_id: Cell::new(0),
            finished: false,
        })
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

        Ok(Statement {
            conn: conn,
            name: name,
            param_types: param_types,
            columns: columns,
            next_portal_id: Cell::new(0),
            finished: true, // << !
        })
    }

    fn close_statement(&mut self, name: &str, type_: u8) -> Result<()> {
        try!(self.write_messages(&[
            Close {
                variant: type_,
                name: name,
            },
            Sync]));
        let resp = match try!(self.read_message()) {
            CloseComplete => Ok(()),
            ErrorResponse { fields } => DbError::new(fields),
            _ => bad_response!(self)
        };
        try!(self.wait_for_ready());
        resp
    }

    fn get_type(&mut self, oid: Oid) -> Result<Type> {
        if let Some(ty) = Type::new(oid) {
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
        try!(self.write_messages(&[
            Bind {
                portal: "",
                statement: TYPEINFO_QUERY,
                formats: &[1],
                values: &[value],
                result_formats: &[1]
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
            _ => bad_response!(self)
        }
        let (name, elem_oid, rngsubtype): (String, Oid, Option<Oid>) =
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
                                                 &ctx)))
            }
            ErrorResponse { fields } => {
                try!(self.wait_for_ready());
                return DbError::new(fields);
            }
            _ => bad_response!(self)
        };
        match try!(self.read_message()) {
            CommandComplete { .. } => {}
            ErrorResponse { fields } => {
                try!(self.wait_for_ready());
                return DbError::new(fields);
            }
            _ => bad_response!(self)
        }
        try!(self.wait_for_ready());

        let kind = if elem_oid != 0 {
            Kind::Array(try!(self.get_type(elem_oid)))
        } else {
            match rngsubtype {
                Some(oid) => Kind::Range(try!(self.get_type(oid))),
                None => Kind::Simple
            }
        };

        let type_ = Type::Other(Box::new(Other::new(name, oid, kind)));
        self.unknown_types.insert(oid, type_.clone());
        Ok(type_)
    }

    fn is_desynchronized(&self) -> bool {
        self.desynchronized
    }

    fn wait_for_ready(&mut self) -> Result<()> {
        match try!(self.read_message()) {
            ReadyForQuery { .. } => Ok(()),
            _ => bad_response!(self)
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
                    result.push(row.into_iter().map(|opt| {
                        opt.map(|b| String::from_utf8_lossy(&b).into_owned())
                    }).collect());
                }
                CopyInResponse { .. } => {
                    try!(self.write_messages(&[
                        CopyFail {
                            message: "COPY queries cannot be directly executed",
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

/// A connection to a Postgres database.
pub struct Connection {
    conn: RefCell<InnerConnection>
}

impl fmt::Debug for Connection {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let conn = self.conn.borrow();
        DebugStruct::new(fmt, "Connection")
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
    /// ## Examples
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # fn f() -> Result<(), ::postgres::error::ConnectError> {
    /// let url = "postgresql://postgres:hunter2@localhost:2994/foodb";
    /// let conn = try!(Connection::connect(url, &SslMode::None));
    /// # Ok(()) };
    /// ```
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # fn f() -> Result<(), ::postgres::error::ConnectError> {
    /// let url = "postgresql://postgres@%2Frun%2Fpostgres";
    /// let conn = try!(Connection::connect(url, &SslMode::None));
    /// # Ok(()) };
    /// ```
    ///
    /// ```rust,no_run
    /// # #![allow(unstable)]
    /// # use postgres::{Connection, UserInfo, ConnectParams, SslMode, ConnectTarget};
    /// # #[cfg(feature = "unix_socket")]
    /// # fn f() -> Result<(), ::postgres::error::ConnectError> {
    /// # let some_crazy_path = Path::new("");
    /// let params = ConnectParams {
    ///     target: ConnectTarget::Unix(some_crazy_path),
    ///     port: None,
    ///     user: Some(UserInfo {
    ///         user: "postgres".to_string(),
    ///         password: None
    ///     }),
    ///     database: None,
    ///     options: vec![],
    /// };
    /// let conn = try!(Connection::connect(params, &SslMode::None));
    /// # Ok(()) };
    /// ```
    pub fn connect<T>(params: T, ssl: &SslMode) -> result::Result<Connection, ConnectError>
            where T: IntoConnectParams {
        InnerConnection::connect(params, ssl).map(|conn| {
            Connection { conn: RefCell::new(conn) }
        })
    }

    /// Sets the notice handler for the connection, returning the old handler.
    pub fn set_notice_handler(&self, handler: Box<HandleNotice>) -> Box<HandleNotice> {
        self.conn.borrow_mut().set_notice_handler(handler)
    }

    /// Returns an iterator over asynchronous notification messages.
    ///
    /// Use the `LISTEN` command to register this connection for notifications.
    pub fn notifications<'a>(&'a self) -> Notifications<'a> {
        Notifications { conn: self }
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
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// let maybe_stmt = conn.prepare("SELECT foo FROM bar WHERE baz = $1");
    /// let stmt = match maybe_stmt {
    ///     Ok(stmt) => stmt,
    ///     Err(err) => panic!("Error preparing statement: {:?}", err)
    /// };
    pub fn prepare<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        self.conn.borrow_mut().prepare(query, self)
    }

    /// Creates cached prepared statement.
    ///
    /// Like `prepare`, except that the statement is only prepared once over
    /// the lifetime of the connection and then cached. If the same statement
    /// is going to be used frequently, caching it can improve performance by
    /// reducing the number of round trips to the Postgres backend.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # fn f() -> postgres::Result<()> {
    /// # let x = 10i32;
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// let stmt = try!(conn.prepare_cached("SELECT foo FROM bar WHERE baz = $1"));
    /// for row in try!(stmt.query(&[&x])) {
    ///     println!("foo: {}", row.get::<_, String>(0));
    /// }
    /// # Ok(()) };
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
    /// ## Note
    /// A transaction will roll back by default. The `set_commit`,
    /// `set_rollback`, and `commit` methods alter this behavior.
    ///
    /// ## Panics
    ///
    /// Panics if a transaction is already active.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # fn foo() -> Result<(), postgres::error::Error> {
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// let trans = try!(conn.transaction());
    /// try!(trans.execute("UPDATE foo SET bar = 10", &[]));
    /// // ...
    ///
    /// try!(trans.commit());
    /// # Ok(())
    /// # }
    /// ```
    pub fn transaction<'a>(&'a self) -> Result<Transaction<'a>> {
        let mut conn = self.conn.borrow_mut();
        check_desync!(conn);
        assert!(conn.trans_depth == 0, "`transaction` must be called on the active transaction");
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
    /// ## Note
    ///
    /// This will not change the behavior of an active transaction.
    pub fn set_transaction_isolation(&self, level: IsolationLevel) -> Result<()> {
        self.batch_execute(level.to_set_query())
    }

    /// Returns the isolation level which will be used for future transactions.
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
    /// ## Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    pub fn execute(&self, query: &str, params: &[&ToSql]) -> Result<u64> {
        let (param_types, columns) = try!(self.conn.borrow_mut().raw_prepare("", query));
        let stmt = Statement {
            conn: self,
            name: "".to_owned(),
            param_types: param_types,
            columns: columns,
            next_portal_id: Cell::new(0),
            finished: true, // << !!
        };
        stmt.execute(params)
    }

    /// Execute a sequence of SQL statements.
    ///
    /// Statements should be separated by `;` characters. If an error occurs,
    /// execution of the sequence will stop at that point. This is intended for
    /// execution of batches of non-dynamic statements - for example, creation
    /// of a schema for a fresh database.
    ///
    /// ## Warning
    ///
    /// Prepared statements should be used for any SQL statement which contains
    /// user-specified data, as it provides functionality to safely embed that
    /// data in the statement. Do not form statements via string concatenation
    /// and feed them into this method.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, Result};
    /// fn init_db(conn: &Connection) -> Result<()> {
    ///     conn.batch_execute("
    ///         CREATE TABLE person (
    ///             id SERIAL PRIMARY KEY,
    ///             name NOT NULL
    ///         );
    ///
    ///         CREATE TABLE purchase (
    ///             id SERIAL PRIMARY KEY,
    ///             person INT NOT NULL REFERENCES person (id),
    ///             time TIMESTAMPTZ NOT NULL,
    ///         );
    ///
    ///         CREATE INDEX ON purchase (time);
    ///         ")
    /// }
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
        DebugStruct::new(fmt, "Transaction")
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
    /// ## Panics
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

/// A prepared statement.
pub struct Statement<'conn> {
    conn: &'conn Connection,
    name: String,
    param_types: Vec<Type>,
    columns: Vec<Column>,
    next_portal_id: Cell<u32>,
    finished: bool,
}

impl<'a> fmt::Debug for Statement<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        DebugStruct::new(fmt, "Statement")
            .field("name", &self.name)
            .field("parameter_types", &self.param_types)
            .field("columns", &self.columns)
            .finish()
    }
}

impl<'conn> Drop for Statement<'conn> {
    fn drop(&mut self) {
        let _ = self.finish_inner();
    }
}

impl<'conn> Statement<'conn> {
    fn finish_inner(&mut self) -> Result<()> {
        if !self.finished {
            self.finished = true;
            let mut conn = self.conn.conn.borrow_mut();
            check_desync!(conn);
            conn.close_statement(&self.name, b'S')
        } else {
            Ok(())
        }
    }

    fn inner_execute(&self, portal_name: &str, row_limit: i32, params: &[&ToSql]) -> Result<()> {
        let mut conn = self.conn.conn.borrow_mut();
        assert!(self.param_types().len() == params.len(),
                "expected {} parameters but got {}",
                self.param_types.len(),
                params.len());
        debug!("executing statement {} with parameters: {:?}", self.name, params);
        let mut values = vec![];
        for (param, ty) in params.iter().zip(self.param_types.iter()) {
            let mut buf = vec![];
            match try!(param.to_sql_checked(ty, &mut buf, &SessionInfo::new(&*conn))) {
                IsNull::Yes => values.push(None),
                IsNull::No => values.push(Some(buf)),
            }
        };

        try!(conn.write_messages(&[
            Bind {
                portal: portal_name,
                statement: &self.name,
                formats: &[1],
                values: &values,
                result_formats: &[1]
            },
            Execute {
                portal: portal_name,
                max_rows: row_limit
            },
            Sync]));

        match try!(conn.read_message()) {
            BindComplete => Ok(()),
            ErrorResponse { fields } => {
                try!(conn.wait_for_ready());
                DbError::new(fields)
            }
            _ => {
                conn.desynchronized = true;
                Err(Error::IoError(bad_response()))
            }
        }
    }

    fn inner_query<'a>(&'a self, portal_name: &str, row_limit: i32, params: &[&ToSql])
                       -> Result<(VecDeque<Vec<Option<Vec<u8>>>>, bool)> {
        try!(self.inner_execute(portal_name, row_limit, params));

        let mut buf = VecDeque::new();
        let more_rows = try!(read_rows(&mut self.conn.conn.borrow_mut(), &mut buf));
        Ok((buf, more_rows))
    }

    /// Returns a slice containing the expected parameter types.
    pub fn param_types(&self) -> &[Type] {
        &self.param_types
    }

    /// Returns a slice describing the columns of the result of the query.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Executes the prepared statement, returning the number of rows modified.
    ///
    /// If the statement does not modify any rows (e.g. SELECT), 0 is returned.
    ///
    /// ## Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// # let bar = 1i32;
    /// # let baz = true;
    /// let stmt = conn.prepare("UPDATE foo SET bar = $1 WHERE baz = $2").unwrap();
    /// match stmt.execute(&[&bar, &baz]) {
    ///     Ok(count) => println!("{} row(s) updated", count),
    ///     Err(err) => println!("Error executing query: {:?}", err)
    /// }
    /// ```
    pub fn execute(&self, params: &[&ToSql]) -> Result<u64> {
        check_desync!(self.conn);
        try!(self.inner_execute("", 0, params));

        let mut conn = self.conn.conn.borrow_mut();
        let num;
        loop {
            match try!(conn.read_message()) {
                DataRow { .. } => {}
                ErrorResponse { fields } => {
                    try!(conn.wait_for_ready());
                    return DbError::new(fields);
                }
                CommandComplete { tag } => {
                    num = util::parse_update_count(tag);
                    break;
                }
                EmptyQueryResponse => {
                    num = 0;
                    break;
                }
                CopyInResponse { .. } => {
                    try!(conn.write_messages(&[
                        CopyFail {
                            message: "COPY queries cannot be directly executed",
                        },
                        Sync]));
                }
                _ => {
                    conn.desynchronized = true;
                    return Err(Error::IoError(bad_response()));
                }
            }
        }
        try!(conn.wait_for_ready());

        Ok(num)
    }

    /// Executes the prepared statement, returning the resulting rows.
    ///
    /// ## Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// let stmt = conn.prepare("SELECT foo FROM bar WHERE baz = $1").unwrap();
    /// # let baz = true;
    /// let rows = match stmt.query(&[&baz]) {
    ///     Ok(rows) => rows,
    ///     Err(err) => panic!("Error running query: {:?}", err)
    /// };
    /// for row in &rows {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// ```
    pub fn query<'a>(&'a self, params: &[&ToSql]) -> Result<Rows<'a>> {
        check_desync!(self.conn);
        self.inner_query("", 0, params).map(|(buf, _)| {
            Rows::new(self, buf.into_iter().collect())
        })
    }

    /// Executes the prepared statement, returning a lazily loaded iterator
    /// over the resulting rows.
    ///
    /// No more than `row_limit` rows will be stored in memory at a time. Rows
    /// will be pulled from the database in batches of `row_limit` as needed.
    /// If `row_limit` is less than or equal to 0, `lazy_query` is equivalent
    /// to `query`.
    ///
    /// This can only be called inside of a transaction, and the `Transaction`
    /// object representing the active transaction must be passed to
    /// `lazy_query`.
    ///
    /// ## Panics
    ///
    /// Panics if the provided `Transaction` is not associated with the same
    /// `Connection` as this `Statement`, if the `Transaction` is not
    /// active, or if the number of parameters provided does not match the
    /// number of parameters expected.
    pub fn lazy_query<'trans, 'stmt>(&'stmt self,
                                     trans: &'trans Transaction,
                                     params: &[&ToSql],
                                     row_limit: i32)
                                     -> Result<LazyRows<'trans, 'stmt>> {
        assert!(self.conn as *const _ == trans.conn as *const _,
                "the `Transaction` passed to `lazy_query` must be associated with the same \
                 `Connection` as the `Statement`");
        let conn = self.conn.conn.borrow();
        check_desync!(conn);
        assert!(conn.trans_depth == trans.depth,
                "`lazy_query` must be passed the active transaction");
        drop(conn);

        let id = self.next_portal_id.get();
        self.next_portal_id.set(id + 1);
        let portal_name = format!("{}p{}", self.name, id);

        self.inner_query(&portal_name, row_limit, params).map(move |(data, more_rows)| {
            LazyRows::new(self, data, portal_name, row_limit, more_rows, false, trans)
        })
    }

    /// Executes a `COPY FROM STDIN` statement, returning the number of rows
    /// added.
    ///
    /// The contents of the provided `Read`er are passed to the Postgres server
    /// verbatim; it is the caller's responsibility to ensure the data is in
    /// the proper format. See the [Postgres documentation](http://www.postgresql.org/docs/9.4/static/sql-copy.html)
    /// for details.
    ///
    /// If the statement is not a `COPY FROM STDIN` statement, it will still be
    /// executed and this method will return an error.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// conn.batch_execute("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR)").unwrap();
    /// let stmt = conn.prepare("COPY people FROM STDIN").unwrap();
    /// stmt.copy_in(&[], &mut "1\tjohn\n2\tjane\n".as_bytes()).unwrap();
    /// ```
    pub fn copy_in<R: Read>(&self, params: &[&ToSql], r: &mut R) -> Result<u64> {
        try!(self.inner_execute("", 0, params));
        let mut conn = self.conn.conn.borrow_mut();

        match try!(conn.read_message()) {
            CopyInResponse { .. } => {}
            _ => {
                loop {
                    match try!(conn.read_message()) {
                        ReadyForQuery { .. } => {
                            return Err(Error::IoError(std_io::Error::new(
                                        std_io::ErrorKind::InvalidInput,
                                        "called `copy_in` on a non-`COPY FROM STDIN` statement")));
                        }
                        _ => {}
                    }
                }
            }
        }

        let mut buf = vec![];
        loop {
            match std::io::copy(&mut r.take(16 * 1024), &mut buf) {
                Ok(0) => break,
                Ok(len) => {
                    try_desync!(conn, conn.stream.write_message(
                            &CopyData {
                                data: &buf[..len as usize],
                            }));
                    buf.clear();
                }
                Err(err) => {
                    // FIXME better to return the error directly
                    try_desync!(conn, conn.stream.write_message(
                            &CopyFail {
                                message: &err.to_string(),
                            }));
                    break;
                }
            }
        }

        try!(conn.write_messages(&[CopyDone, Sync]));

        let num = match try!(conn.read_message()) {
            CommandComplete { tag } => util::parse_update_count(tag),
            ErrorResponse { fields } => {
                try!(conn.wait_for_ready());
                return DbError::new(fields);
            }
            _ => {
                conn.desynchronized = true;
                return Err(Error::IoError(bad_response()));
            }
        };

        try!(conn.wait_for_ready());
        Ok(num)
    }

    /// Consumes the statement, clearing it from the Postgres session.
    ///
    /// If this statement was created via the `prepare_cached` method, `finish`
    /// does nothing.
    ///
    /// Functionally identical to the `Drop` implementation of the
    /// `Statement` except that it returns any error to the caller.
    pub fn finish(mut self) -> Result<()> {
        self.finish_inner()
    }
}

/// Information about a column of the result of a query.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Column {
    name: String,
    type_: Type
}

impl Column {
    /// The name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The type of the data in the column.
    pub fn type_(&self) -> &Type {
        &self.type_
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
                try!(conn.write_messages(&[
                    CopyFail {
                        message: "COPY queries cannot be directly executed",
                    },
                    Sync]));
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
    fn new(name: String, oid: Oid, kind: Kind) -> Other;
}

trait DbErrorNew {
    fn new_raw(fields: Vec<(u8, String)>) -> result::Result<DbError, ()>;
    fn new_connect<T>(fields: Vec<(u8, String)>) -> result::Result<T, ConnectError>;
    fn new<T>(fields: Vec<(u8, String)>) -> Result<T>;
}

trait TypeNew {
    fn new(oid: Oid) -> Option<Type>;
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
           trans: &'trans Transaction<'trans>) -> LazyRows<'trans, 'stmt>;
}

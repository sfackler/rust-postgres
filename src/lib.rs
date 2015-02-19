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
//!     for row in &stmt.query(&[]).unwrap() {
//!         let person = Person {
//!             id: row.get(0),
//!             name: row.get(1),
//!             data: row.get(2)
//!         };
//!         println!("Found person {}", person.name);
//!     }
//! }
//! ```
#![doc(html_root_url="https://sfackler.github.io/rust-postgres/doc")]
#![feature(unsafe_destructor, collections, old_io, io, core, old_path, std_misc)]
#![warn(missing_docs)]

extern crate byteorder;
#[macro_use]
extern crate log;
extern crate openssl;
extern crate phf;
extern crate "rustc-serialize" as serialize;
extern crate time;

use openssl::crypto::hash::{self, Hasher};
use openssl::ssl::{SslContext, MaybeSslStream};
use serialize::hex::ToHex;
use std::borrow::{ToOwned, Cow};
use std::cell::{Cell, RefCell};
use std::cmp::max;
use std::collections::{VecDeque, HashMap};
use std::fmt;
use std::iter::IntoIterator;
use std::old_io::{BufferedStream, IoResult, IoError, IoErrorKind};
use std::old_io::net::ip::Port;
use std::mem;
use std::slice;
use std::result;
use std::time::Duration;
use time::SteadyTime;

use url::Url;
pub use error::{Error, ConnectError, SqlState, DbError, ErrorPosition};
#[doc(inline)]
pub use types::{Oid, Type, Kind, ToSql, FromSql};
#[doc(inline)]
pub use types::Slice;
use io::{InternalStream, Timeout};
use message::BackendMessage::*;
use message::FrontendMessage::*;
use message::{FrontendMessage, BackendMessage, RowDescriptionEntry};
use message::{WriteMessage, ReadMessage};

#[macro_use]
mod macros;

mod error;
mod io;
mod message;
mod ugh_privacy;
mod url;
mod util;
pub mod types;

const TYPEINFO_QUERY: &'static str = "t";

/// A type alias of the result returned by many methods.
pub type Result<T> = result::Result<T, Error>;

/// Specifies the target server to connect to.
#[derive(Clone, Debug)]
pub enum ConnectTarget {
    /// Connect via TCP to the specified host.
    Tcp(String),
    /// Connect via a Unix domain socket in the specified directory.
    Unix(Path)
}

/// Authentication information
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
    pub port: Option<Port>,
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
            path: url::Path { path, query: options, .. },
            ..
        } = self;

        let maybe_path = try!(url::decode_component(&host).map_err(ConnectError::InvalidUrl));
        let target = if maybe_path.starts_with("/") {
            ConnectTarget::Unix(Path::new(maybe_path))
        } else {
            ConnectTarget::Tcp(host)
        };

        let user = user.map(|url::UserInfo { user, pass }| {
            UserInfo { user: user, password: pass }
        });

        // path contains the leading /
        let database = path.slice_shift_char().map(|(_, path)| path.to_owned());

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
pub trait NoticeHandler: Send {
    /// Handle a Postgres notice message
    fn handle(&mut self, notice: DbError);
}

/// A notice handler which logs at the `info` level.
///
/// This is the default handler used by a `Connection`.
#[derive(Copy, Debug)]
pub struct DefaultNoticeHandler;

impl NoticeHandler for DefaultNoticeHandler {
    fn handle(&mut self, notice: DbError) {
        info!("{}: {}", notice.severity(), notice.message());
    }
}

/// An asynchronous notification
#[derive(Clone, Debug)]
pub struct Notification {
    /// The process ID of the notifying backend process
    pub pid: u32,
    /// The name of the channel that the notify has been raised on
    pub channel: String,
    /// The "payload" string passed from the notifying process
    pub payload: String,
}

/// An iterator over asynchronous notifications
pub struct Notifications<'conn> {
    conn: &'conn Connection
}

impl<'a> fmt::Debug for Notifications<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Notifications {{ pending: {} }}", self.conn.conn.borrow().notifications.len())
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
}

/// Contains information necessary to cancel queries for a session
#[derive(Copy, Clone, Debug)]
pub struct CancelData {
    /// The process ID of the session
    pub process_id: u32,
    /// The secret key for the session
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
/// # #![allow(unstable)]
/// # use postgres::{Connection, SslMode};
/// # use std::thread::Thread;
/// # let url = "";
/// let conn = Connection::connect(url, &SslMode::None).unwrap();
/// let cancel_data = conn.cancel_data();
/// Thread::spawn(move || {
///     conn.execute("SOME EXPENSIVE QUERY", &[]).unwrap();
/// });
/// # let _ =
/// postgres::cancel_query(url, &SslMode::None, cancel_data);
/// ```
pub fn cancel_query<T>(params: T, ssl: &SslMode, data: CancelData)
                       -> result::Result<(), ConnectError> where T: IntoConnectParams {
    let params = try!(params.into_connect_params());
    let mut socket = try!(io::initialize_stream(&params, ssl));

    try!(socket.write_message(&CancelRequest {
        code: message::CANCEL_CODE,
        process_id: data.process_id,
        secret_key: data.secret_key
    }));
    try!(socket.flush());

    Ok(())
}

#[derive(Clone)]
struct CachedStatement {
    name: String,
    param_types: Vec<Type>,
    columns: Vec<Column>,
}

struct InnerConnection {
    stream: BufferedStream<MaybeSslStream<InternalStream>>,
    notice_handler: Box<NoticeHandler>,
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
        let stream = try!(io::initialize_stream(&params, ssl));

        let ConnectParams { user, database, mut options, .. } = params;

        let user = try!(user.ok_or(ConnectError::MissingUser));

        let mut conn = InnerConnection {
            stream: BufferedStream::new(stream),
            next_stmt_id: 0,
            notice_handler: Box::new(DefaultNoticeHandler),
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
        options.push(("TimeZone".to_owned(), "GMT".to_owned()));
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
                ErrorResponse { fields } => return ugh_privacy::dberror_new_connect(fields),
                _ => return Err(ConnectError::BadResponse),
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
                               "SELECT typname, typelem, NULL::OID FROM pg_catalog.pg_type \
                                WHERE oid = $1") {
            Ok(..) => Ok(()),
            Err(Error::IoError(e)) => Err(ConnectError::IoError(e)),
            Err(Error::DbError(e)) => Err(ConnectError::DbError(e)),
            _ => unreachable!()
        }
    }

    fn write_messages(&mut self, messages: &[FrontendMessage]) -> IoResult<()> {
        debug_assert!(!self.desynchronized);
        for message in messages {
            try_desync!(self, self.stream.write_message(message));
        }
        Ok(try_desync!(self, self.stream.flush()))
    }

    fn read_one_message(&mut self) -> IoResult<Option<BackendMessage>> {
        debug_assert!(!self.desynchronized);
        match try_desync!(self, self.stream.read_message()) {
            NoticeResponse { fields } => {
                if let Ok(err) = ugh_privacy::dberror_new_raw(fields) {
                    self.notice_handler.handle(err);
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

    fn read_message_with_notification(&mut self) -> IoResult<BackendMessage> {
        loop {
            if let Some(msg) = try!(self.read_one_message()) {
                return Ok(msg);
            }
        }
    }

    fn read_message(&mut self) -> IoResult<BackendMessage> {
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
                let mut hasher = Hasher::new(hash::Type::MD5);
                let _ = hasher.write_all(pass.as_bytes());
                let _ = hasher.write_all(user.user.as_bytes());
                let output = hasher.finish().to_hex();
                let _ = hasher.write_all(output.as_bytes());
                let _ = hasher.write_all(&salt);
                let output = format!("md5{}", hasher.finish().to_hex());
                try!(self.write_messages(&[PasswordMessage {
                        password: &output
                    }]));
            }
            AuthenticationKerberosV5
            | AuthenticationSCMCredential
            | AuthenticationGSS
            | AuthenticationSSPI => return Err(ConnectError::UnsupportedAuthentication),
            ErrorResponse { fields } => return ugh_privacy::dberror_new_connect(fields),
            _ => return Err(ConnectError::BadResponse)
        }

        match try!(self.read_message()) {
            AuthenticationOk => Ok(()),
            ErrorResponse { fields } => return ugh_privacy::dberror_new_connect(fields),
            _ => return Err(ConnectError::BadResponse)
        }
    }

    fn set_notice_handler(&mut self, handler: Box<NoticeHandler>) -> Box<NoticeHandler> {
        mem::replace(&mut self.notice_handler, handler)
    }

    fn raw_prepare(&mut self, stmt_name: &str, query: &str)
                   -> Result<(Vec<Type>, Vec<Column>)> {
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
                return ugh_privacy::dberror_new(fields);
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

    fn prepare_copy_in<'a>(&mut self, table: &str, rows: &[&str], conn: &'a Connection)
                           -> Result<CopyInStatement<'a>> {
        let mut query = vec![];
        let _ = write!(&mut query, "SELECT ");
        let _ = util::comma_join(&mut query, rows.iter().cloned());
        let _ = write!(&mut query, " FROM {}", table);
        let query = String::from_utf8(query).unwrap();
        let (_, columns) = try!(self.raw_prepare("", &query));
        let column_types = columns.into_iter().map(|desc| desc.type_).collect();

        let mut query = vec![];
        let _ = write!(&mut query, "COPY {} (", table);
        let _ = util::comma_join(&mut query, rows.iter().cloned());
        let _ = write!(&mut query, ") FROM STDIN WITH (FORMAT binary)");
        let query = String::from_utf8(query).unwrap();
        let stmt_name = self.make_stmt_name();
        try!(self.raw_prepare(&stmt_name, &query));

        Ok(CopyInStatement {
            conn: conn,
            name: stmt_name,
            column_types: column_types,
            finished: false,
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
            ErrorResponse { fields } => ugh_privacy::dberror_new(fields),
            _ => bad_response!(self)
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
        try!(self.write_messages(&[
            Bind {
                portal: "",
                statement: TYPEINFO_QUERY,
                formats: &[1],
                values: &[try!(oid.to_sql(&Type::Oid))],
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
                return ugh_privacy::dberror_new(fields);
            }
            _ => bad_response!(self)
        }
        let (name, elem_oid, rngsubtype): (String, Oid, Option<Oid>) =
                match try!(self.read_message()) {
            DataRow { row } => {
                (try!(FromSql::from_sql_nullable(&Type::Name,
                                                 row[0].as_ref().map(|r| &**r).as_mut())),
                 try!(FromSql::from_sql_nullable(&Type::Oid,
                                                 row[1].as_ref().map(|r| &**r).as_mut())),
                 try!(FromSql::from_sql_nullable(&Type::Oid,
                                                 row[2].as_ref().map(|r| &**r).as_mut())))
            }
            ErrorResponse { fields } => {
                try!(self.wait_for_ready());
                return ugh_privacy::dberror_new(fields);
            }
            _ => bad_response!(self)
        };
        match try!(self.read_message()) {
            CommandComplete { .. } => {}
            ErrorResponse { fields } => {
                try!(self.wait_for_ready());
                return ugh_privacy::dberror_new(fields);
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

        let type_ = Type::Other(Box::new(ugh_privacy::new_other(name, oid, kind)));
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
                    return ugh_privacy::dberror_new(fields);
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
        write!(fmt,
               "Connection {{ cancel_data: {:?}, notifications: {:?}, transaction_depth: {:?}, \
                desynchronized: {:?}, cached_statements: {:?} }}",
               conn.cancel_data,
               conn.notifications.len(),
               conn.trans_depth,
               conn.desynchronized,
               conn.cached_statements.len())
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
    /// absolute path of the directory containing the socket file. Since `/` is
    /// a reserved character in URLs, the path should be URL encoded.  If the
    /// path contains non-UTF 8 characters, a `ConnectParams` struct
    /// should be created manually and passed in. Note that Postgres does not
    /// support SSL over Unix sockets.
    ///
    /// ## Examples
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode, ConnectError};
    /// # fn f() -> Result<(), ConnectError> {
    /// let url = "postgresql://postgres:hunter2@localhost:2994/foodb";
    /// let conn = try!(Connection::connect(url, &SslMode::None));
    /// # Ok(()) };
    /// ```
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode, ConnectError};
    /// # fn f() -> Result<(), ConnectError> {
    /// let url = "postgresql://postgres@%2Frun%2Fpostgres";
    /// let conn = try!(Connection::connect(url, &SslMode::None));
    /// # Ok(()) };
    /// ```
    ///
    /// ```rust,no_run
    /// # #![allow(unstable)]
    /// # use postgres::{Connection, UserInfo, ConnectParams, SslMode, ConnectTarget, ConnectError};
    /// # fn f() -> Result<(), ConnectError> {
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
    pub fn set_notice_handler(&self, handler: Box<NoticeHandler>) -> Box<NoticeHandler> {
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
    /// for row in &try!(stmt.query(&[&x])) {
    ///     println!("foo: {}", row.get::<_, String>(0));
    /// }
    /// # Ok(()) };
    /// ```
    pub fn prepare_cached<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        self.conn.borrow_mut().prepare_cached(query, self)
    }

    /// Creates a new COPY FROM STDIN prepared statement.
    ///
    /// These statements provide a method to efficiently bulk-upload data to
    /// the database.
    pub fn prepare_copy_in<'a>(&'a self, table: &str, rows: &[&str])
                               -> Result<CopyInStatement<'a>> {
        self.conn.borrow_mut().prepare_copy_in(table, rows, self)
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
    /// # fn foo() -> Result<(), postgres::Error> {
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

    /// Returns the value of the specified parameter.
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

/// Specifies the SSL support requested for a new connection
#[derive(Debug)]
pub enum SslMode {
    /// The connection will not use SSL
    None,
    /// The connection will use SSL if the backend supports it
    Prefer(SslContext),
    /// The connection must use SSL
    Require(SslContext)
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
        write!(fmt, "Transaction {{ commit: {:?}, depth: {:?} }}", self.commit.get(), self.depth)
    }
}

#[unsafe_destructor]
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

    /// Like `Connection::prepare_copy_in`.
    pub fn prepare_copy_in(&self, table: &str, cols: &[&str]) -> Result<CopyInStatement<'conn>> {
        self.conn.prepare_copy_in(table, cols)
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
        assert!(conn.trans_depth == self.depth, "`transaction` may only be called on the active \
                                                 transaction");
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

/// A prepared statement
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
        write!(fmt,
               "Statement {{ name: {:?}, parameter_types: {:?}, columns: {:?} }}",
               self.name,
               self.param_types,
               self.columns)
    }
}

#[unsafe_destructor]
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
        let mut values = vec![];
        for (param, ty) in params.iter().zip(self.param_types.iter()) {
            values.push(try!(param.to_sql(ty)));
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
                ugh_privacy::dberror_new(fields)
            }
            _ => {
                conn.desynchronized = true;
                Err(Error::BadResponse)
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
                    return ugh_privacy::dberror_new(fields);
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
                    return Err(Error::BadResponse);
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
            Rows {
                stmt: self,
                data: buf.into_iter().collect()
            }
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
            LazyRows {
                _trans: trans,
                stmt: self,
                data: data,
                name: portal_name,
                row_limit: row_limit,
                more_rows: more_rows,
                finished: false,
            }
        })
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
                return ugh_privacy::dberror_new(fields);
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
                return Err(Error::BadResponse);
            }
        }
    }
    try!(conn.wait_for_ready());
    Ok(more_rows)
}

/// The resulting rows of a query.
pub struct Rows<'stmt> {
    stmt: &'stmt Statement<'stmt>,
    data: Vec<Vec<Option<Vec<u8>>>>,
}

impl<'a> fmt::Debug for Rows<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt,
               "Rows {{ columns: {:?}, rows: {:?} }}",
               self.columns(),
               self.data.len())
    }
}

impl<'stmt> Rows<'stmt> {
    /// Returns a slice describing the columns of the `Rows`.
    pub fn columns(&self) -> &'stmt [Column] {
        self.stmt.columns()
    }

    /// Returns an iterator over the `Row`s.
    pub fn iter<'a>(&'a self) -> RowsIter<'a> {
        RowsIter {
            stmt: self.stmt,
            iter: self.data.iter()
        }
    }
}

impl<'a> IntoIterator for &'a Rows<'a> {
    type Item = Row<'a>;
    type IntoIter = RowsIter<'a>;

    fn into_iter(self) -> RowsIter<'a> {
        self.iter()
    }
}

/// An iterator over `Row`s.
pub struct RowsIter<'a> {
    stmt: &'a Statement<'a>,
    iter: slice::Iter<'a, Vec<Option<Vec<u8>>>>,
}

impl<'a> Iterator for RowsIter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Row<'a>> {
        self.iter.next().map(|row| {
            Row {
                stmt: self.stmt,
                data: Cow::Borrowed(row),
            }
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a> DoubleEndedIterator for RowsIter<'a> {
    fn next_back(&mut self) -> Option<Row<'a>> {
        self.iter.next_back().map(|row| {
            Row {
                stmt: self.stmt,
                data: Cow::Borrowed(row),
            }
        })
    }
}

impl<'a> ExactSizeIterator for RowsIter<'a> {}

/// A single result row of a query.
pub struct Row<'a> {
    stmt: &'a Statement<'a>,
    data: Cow<'a, [Option<Vec<u8>>]>
}

impl<'a> fmt::Debug for Row<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "Row {{ statement: {:?} }}", self.stmt)
    }
}

impl<'a> Row<'a> {
    /// Returns the number of values in the row
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns a slice describing the columns of the `Row`.
    pub fn columns(&self) -> &[Column] {
        self.stmt.columns()
    }

    /// Retrieves the contents of a field of the row.
    ///
    /// A field can be accessed by the name or index of its column, though
    /// access by index is more efficient. Rows are 0-indexed.
    ///
    /// Returns an `Error` value if the index does not reference a column or
    /// the return type is not compatible with the Postgres type.
    pub fn get_opt<I, T>(&self, idx: I) -> Result<T> where I: RowIndex, T: FromSql {
        let idx = try!(idx.idx(self.stmt).ok_or(Error::InvalidColumn));
        let ty = &self.stmt.columns[idx].type_;
        if !<T as FromSql>::accepts(ty) {
            return Err(Error::WrongType(ty.clone()));
        }
        FromSql::from_sql_nullable(ty, self.data[idx].as_ref().map(|e| &**e).as_mut())
    }

    /// Retrieves the contents of a field of the row.
    ///
    /// A field can be accessed by the name or index of its column, though
    /// access by index is more efficient. Rows are 0-indexed.
    ///
    /// ## Panics
    ///
    /// Panics if the index does not reference a column or the return type is
    /// not compatible with the Postgres type.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// # let stmt = conn.prepare("").unwrap();
    /// # let mut result = stmt.query(&[]).unwrap();
    /// # let row = result.iter().next().unwrap();
    /// let foo: i32 = row.get(0u);
    /// let bar: String = row.get("bar");
    /// ```
    pub fn get<I, T>(&self, idx: I) -> T where I: RowIndex + fmt::Debug + Clone, T: FromSql {
        match self.get_opt(idx.clone()) {
            Ok(ok) => ok,
            Err(err) => panic!("error retrieving column {:?}: {:?}", idx, err)
        }
    }

    /// Retrieves the specified field as a raw buffer of Postgres data.
    ///
    /// ## Panics
    ///
    /// Panics if the index does not references a column.
    pub fn get_bytes<I>(&self, idx: I) -> Option<&[u8]> where I: RowIndex + fmt::Debug {
        match idx.idx(self.stmt) {
            Some(idx) => self.data[idx].as_ref().map(|e| &**e),
            None => panic!("invalid index {:?}", idx),
        }
    }
}

/// A trait implemented by types that can index into columns of a row.
pub trait RowIndex {
    /// Returns the index of the appropriate column, or `None` if no such
    /// column exists.
    fn idx(&self, stmt: &Statement) -> Option<usize>;
}

impl RowIndex for usize {
    #[inline]
    fn idx(&self, stmt: &Statement) -> Option<usize> {
        if *self >= stmt.columns.len() {
            None
        } else {
            Some(*self)
        }
    }
}

impl<'a> RowIndex for &'a str {
    #[inline]
    fn idx(&self, stmt: &Statement) -> Option<usize> {
        stmt.columns().iter().position(|d| d.name == *self)
    }
}

/// A lazily-loaded iterator over the resulting rows of a query
pub struct LazyRows<'trans, 'stmt> {
    stmt: &'stmt Statement<'stmt>,
    data: VecDeque<Vec<Option<Vec<u8>>>>,
    name: String,
    row_limit: i32,
    more_rows: bool,
    finished: bool,
    _trans: &'trans Transaction<'trans>,
}

#[unsafe_destructor]
impl<'a, 'b> Drop for LazyRows<'a, 'b> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish_inner();
        }
    }
}

impl<'a, 'b> fmt::Debug for LazyRows<'a, 'b> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt,
               "LazyRows {{ name: {:?}, row_limit: {:?}, remaining_rows: {:?}, \
                more_rows: {:?} }}",
               self.name,
               self.row_limit,
               self.data.len(),
               self.more_rows)
    }
}

impl<'trans, 'stmt> LazyRows<'trans, 'stmt> {
    fn finish_inner(&mut self) -> Result<()> {
        let mut conn = self.stmt.conn.conn.borrow_mut();
        check_desync!(conn);
        conn.close_statement(&self.name, b'P')
    }

    fn execute(&mut self) -> Result<()> {
        let mut conn = self.stmt.conn.conn.borrow_mut();

        try!(conn.write_messages(&[
            Execute {
                portal: &self.name,
                max_rows: self.row_limit
            },
            Sync]));
        read_rows(&mut conn, &mut self.data).map(|more_rows| {
            self.more_rows = more_rows;
            ()
        })
    }

    /// Returns a slice describing the columns of the `LazyRows`.
    pub fn columns(&self) -> &[Column] {
        self.stmt.columns()
    }

    /// Consumes the `LazyRows`, cleaning up associated state.
    ///
    /// Functionally identical to the `Drop` implementation on `LazyRows`
    /// except that it returns any error to the caller.
    pub fn finish(mut self) -> Result<()> {
        self.finish_inner()
    }
}

impl<'trans, 'stmt> Iterator for LazyRows<'trans, 'stmt> {
    type Item = Result<Row<'stmt>>;

    fn next(&mut self) -> Option<Result<Row<'stmt>>> {
        if self.data.is_empty() && self.more_rows {
            if let Err(err) = self.execute() {
                return Some(Err(err));
            }
        }

        self.data.pop_front().map(|r| {
            Ok(Row {
                stmt: self.stmt,
                data: Cow::Owned(r),
            })
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let lower = self.data.len();
        let upper = if self.more_rows {
            None
        } else {
            Some(lower)
        };
        (lower, upper)
    }
}

/// A prepared COPY FROM STDIN statement
pub struct CopyInStatement<'a> {
    conn: &'a Connection,
    name: String,
    column_types: Vec<Type>,
    finished: bool,
}

impl<'a> fmt::Debug for CopyInStatement<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "CopyInStatement {{ name: {:?}, column_types: {:?} }}",
               self.name, self.column_types)
    }
}

#[unsafe_destructor]
impl<'a> Drop for CopyInStatement<'a> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish_inner();
        }
    }
}

/// An `Iterator` variant which returns borrowed values.
pub trait StreamIterator {
    /// Returns the next value, or `None` if there is none.
    fn next<'a>(&'a mut self) -> Option<&'a (ToSql + 'a)>;
}

/// An adapter type implementing `StreamIterator` for a `Vec<Box<ToSql>>`.
pub struct VecStreamIterator<'a> {
    v: Vec<Box<ToSql + 'a>>,
    idx: usize,
}

impl<'a> VecStreamIterator<'a> {
    /// Creates a new `VecStreamIterator`.
    pub fn new(v: Vec<Box<ToSql + 'a>>) -> VecStreamIterator<'a> {
        VecStreamIterator {
            v: v,
            idx: 0,
        }
    }

    /// Returns the underlying `Vec`.
    pub fn into_inner(self) -> Vec<Box<ToSql + 'a>> {
        self.v
    }
}

impl<'a> StreamIterator for VecStreamIterator<'a> {
    fn next<'b>(&'b mut self) -> Option<&'b (ToSql + 'b)> {
        match self.v.get_mut(self.idx) {
            Some(mut e) => {
                self.idx += 1;
                Some(&mut **e)
            },
            None => None,
        }
    }
}

impl<'a> CopyInStatement<'a> {
    fn finish_inner(&mut self) -> Result<()> {
        let mut conn = self.conn.conn.borrow_mut();
        check_desync!(conn);
        conn.close_statement(&self.name, b'S')
    }

    /// Returns a slice containing the expected column types.
    pub fn column_types(&self) -> &[Type] {
        &self.column_types
    }

    /// Executes the prepared statement.
    ///
    /// The `rows` argument is an `Iterator` returning `StreamIterator` values,
    /// each one of which provides values for a row of input. This setup is
    /// designed to allow callers to avoid having to maintain the entire row
    /// set in memory.
    ///
    /// Returns the number of rows copied.
    pub fn execute<I, J>(&self, rows: I) -> Result<u64>
            where I: Iterator<Item=J>, J: StreamIterator {
        let mut conn = self.conn.conn.borrow_mut();

        try!(conn.write_messages(&[
            Bind {
                portal: "",
                statement: &self.name,
                formats: &[],
                values: &[],
                result_formats: &[]
            },
            Execute {
                portal: "",
                max_rows: 0,
            },
            Sync]));

        match try!(conn.read_message()) {
            BindComplete => {},
            ErrorResponse { fields } => {
                try!(conn.wait_for_ready());
                return ugh_privacy::dberror_new(fields);
            }
            _ => {
                conn.desynchronized = true;
                return Err(Error::BadResponse);
            }
        }

        match try!(conn.read_message()) {
            CopyInResponse { .. } => {}
            _ => {
                conn.desynchronized = true;
                return Err(Error::BadResponse);
            }
        }

        let mut buf = vec![];
        let _ = buf.write_all(b"PGCOPY\n\xff\r\n\x00");
        let _ = buf.write_be_i32(0);
        let _ = buf.write_be_i32(0);

        'l: for mut row in rows {
            let _ = buf.write_be_i16(self.column_types.len() as i16);

            let mut types = self.column_types.iter();
            loop {
                match (row.next(), types.next()) {
                    (Some(val), Some(ty)) => {
                        match val.to_sql(ty) {
                            Ok(None) => {
                                let _ = buf.write_be_i32(-1);
                            }
                            Ok(Some(val)) => {
                                let _ = buf.write_be_i32(val.len() as i32);
                                let _ = buf.write_all(&val);
                            }
                            Err(err) => {
                                // FIXME this is not the right way to handle this
                                try_desync!(conn, conn.stream.write_message(
                                    &CopyFail {
                                        message: &err.to_string(),
                                    }));
                                break 'l;
                            }
                        }
                    }
                    (Some(_), None) | (None, Some(_)) => {
                        try_desync!(conn, conn.stream.write_message(
                            &CopyFail {
                                message: "Invalid column count",
                            }));
                        break 'l;
                    }
                    (None, None) => break
                }
            }

            try_desync!(conn, conn.stream.write_message(
                &CopyData {
                    data: &buf
                }));
            buf.clear();
        }

        let _ = buf.write_be_i16(-1);
        try!(conn.write_messages(&[
            CopyData {
                data: &buf,
            },
            CopyDone,
            Sync]));

        let num = match try!(conn.read_message()) {
            CommandComplete { tag } => util::parse_update_count(tag),
            ErrorResponse { fields } => {
                try!(conn.wait_for_ready());
                return ugh_privacy::dberror_new(fields);
            }
            _ => {
                conn.desynchronized = true;
                return Err(Error::BadResponse);
            }
        };

        try!(conn.wait_for_ready());
        Ok(num)
    }

    /// Consumes the statement, clearing it from the Postgres session.
    ///
    /// Functionally identical to the `Drop` implementation of the
    /// `CopyInStatement` except that it returns any error to the
    /// caller.
    pub fn finish(mut self) -> Result<()> {
        self.finished = true;
        self.finish_inner()
    }
}

/// A trait allowing abstraction over connections and transactions
pub trait GenericConnection {
    /// Like `Connection::prepare`.
    fn prepare<'a>(&'a self, query: &str) -> Result<Statement<'a>>;

    /// Like `Connection::prepare_cached`.
    fn prepare_cached<'a>(&'a self, query: &str) -> Result<Statement<'a>>;

    /// Like `Connection::execute`.
    fn execute(&self, query: &str, params: &[&ToSql]) -> Result<u64>;

    /// Like `Connection::prepare_copy_in`.
    fn prepare_copy_in<'a>(&'a self, table: &str, columns: &[&str])
                           -> Result<CopyInStatement<'a>>;

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

    fn prepare_copy_in<'a>(&'a self, table: &str, columns: &[&str])
                           -> Result<CopyInStatement<'a>> {
        self.prepare_copy_in(table, columns)
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

    fn prepare_copy_in<'b>(&'b self, table: &str, columns: &[&str])
                           -> Result<CopyInStatement<'b>> {
        self.prepare_copy_in(table, columns)
    }

    fn batch_execute(&self, query: &str) -> Result<()> {
        self.batch_execute(query)
    }

    fn is_active(&self) -> bool {
        self.is_active()
    }
}

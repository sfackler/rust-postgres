//! Rust-Postgres is a pure-Rust frontend for the popular PostgreSQL database. It
//! exposes a high level interface in the vein of JDBC or Go's `database/sql`
//! package.
//!
//! ```rust,no_run
//! extern crate postgres;
//! extern crate time;
//!
//! use time::Timespec;
//!
//! use postgres::{Connection, NoSsl};
//!
//! struct Person {
//!     id: i32,
//!     name: String,
//!     time_created: Timespec,
//!     data: Option<Vec<u8>>
//! }
//!
//! fn main() {
//!     let conn = Connection::connect("postgresql://postgres@localhost", &NoSsl)
//!             .unwrap();
//!
//!     conn.execute("CREATE TABLE person (
//!                     id              SERIAL PRIMARY KEY,
//!                     name            VARCHAR NOT NULL,
//!                     time_created    TIMESTAMP NOT NULL,
//!                     data            BYTEA
//!                   )", []).unwrap();
//!     let me = Person {
//!         id: 0,
//!         name: "Steven".into_string(),
//!         time_created: time::get_time(),
//!         data: None
//!     };
//!     conn.execute("INSERT INTO person (name, time_created, data)
//!                     VALUES ($1, $2, $3)",
//!                  &[&me.name, &me.time_created, &me.data]).unwrap();
//!
//!     let stmt = conn.prepare("SELECT id, name, time_created, data FROM person")
//!             .unwrap();
//!     for row in stmt.query([]).unwrap() {
//!         let person = Person {
//!             id: row.get(0),
//!             name: row.get(1),
//!             time_created: row.get(2),
//!             data: row.get(3)
//!         };
//!         println!("Found person {}", person.name);
//!     }
//! }
//! ```
#![doc(html_root_url="https://sfackler.github.io/doc")]
#![feature(globs, macro_rules, struct_variant, phase, unsafe_destructor, slicing_syntax, default_type_params, if_let)]
#![warn(missing_docs)]

extern crate openssl;
extern crate serialize;
extern crate time;
extern crate phf;
#[phase(plugin)]
extern crate phf_mac;
#[phase(plugin, link)]
extern crate log;

use url::Url;
use openssl::crypto::hash::{MD5, Hasher};
use openssl::ssl::SslContext;
use serialize::hex::ToHex;
use std::cell::{Cell, RefCell};
use std::collections::{RingBuf, HashMap};
use std::io::{BufferedStream, IoResult, MemWriter};
use std::io::net::ip::Port;
use std::mem;
use std::fmt;
use std::result;

use error::{InvalidUrl,
            MissingPassword,
            MissingUser,
            PgConnectStreamError,
            PgConnectBadResponse,
            PgInvalidColumn,
            PgStreamDesynchronized,
            PgStreamError,
            PgWrongParamCount,
            UnsupportedAuthentication,
            PgWrongConnection,
            PgWrongTransaction,
            PgBadResponse};
use io::{MaybeSslStream, InternalStream};
use message::{FrontendMessage, BackendMessage, RowDescriptionEntry};
use message::FrontendMessage::*;
use message::BackendMessage::*;
use message::{WriteMessage, ReadMessage};
#[doc(inline)]
pub use types::{Oid, Type, ToSql, FromSql};
pub use error::{Error, ConnectError, SqlState, DbError, ErrorPosition};

#[macro_escape]
mod macros;

mod io;
mod message;
mod url;
mod util;
mod error;
pub mod types;

const CANARY: u32 = 0xdeadbeef;

/// A typedef of the result returned by many methods.
pub type Result<T> = result::Result<T, Error>;

/// Specifies the target server to connect to.
#[deriving(Clone)]
pub enum ConnectTarget {
    /// Connect via TCP to the specified host.
    Tcp(String),
    /// Connect via a Unix domain socket in the specified directory.
    Unix(Path)
}

/// Authentication information
#[deriving(Clone)]
pub struct UserInfo {
    /// The username
    pub user: String,
    /// An optional password
    pub password: Option<String>,
}

/// Information necessary to open a new connection to a Postgres server.
#[deriving(Clone)]
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
            Err(err) => return Err(InvalidUrl(err)),
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

        let maybe_path = try!(url::decode_component(host[]).map_err(InvalidUrl));
        let target = if maybe_path[].starts_with("/") {
            ConnectTarget::Unix(Path::new(maybe_path))
        } else {
            ConnectTarget::Tcp(host)
        };

        let user = user.map(|url::UserInfo { user, pass }| {
            UserInfo { user: user, password: pass }
        });

        let database = if !path.is_empty() {
            // path contains the leading /
            let (_, path) = path[].slice_shift_char();
            Some(path.into_string())
        } else {
            None
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
pub trait NoticeHandler {
    /// Handle a Postgres notice message
    fn handle(&mut self, notice: DbError);
}

/// A notice handler which logs at the `info` level.
///
/// This is the default handler used by a `Connection`.
pub struct DefaultNoticeHandler;

impl NoticeHandler for DefaultNoticeHandler {
    fn handle(&mut self, notice: DbError) {
        info!("{}: {}", notice.severity, notice.message);
    }
}

/// An asynchronous notification
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

impl<'conn> Iterator<Notification> for Notifications<'conn> {
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

/// Contains information necessary to cancel queries for a session
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
/// # use postgres::{Connection, NoSsl};
/// # let url = "";
/// let conn = Connection::connect(url, &NoSsl).unwrap();
/// let cancel_data = conn.cancel_data();
/// spawn(proc() {
///     conn.execute("SOME EXPENSIVE QUERY", []).unwrap();
/// });
/// # let _ =
/// postgres::cancel_query(url, &NoSsl, cancel_data);
/// ```
pub fn cancel_query<T>(params: T, ssl: &SslMode, data: CancelData)
                       -> result::Result<(), ConnectError> where T: IntoConnectParams {
    let params = try!(params.into_connect_params());
    let mut socket = try!(io::initialize_stream(&params, ssl));

    try_pg_conn!(socket.write_message(&CancelRequest {
        code: message::CANCEL_CODE,
        process_id: data.process_id,
        secret_key: data.secret_key
    }));
    try_pg_conn!(socket.flush());

    Ok(())
}

struct InnerConnection {
    stream: BufferedStream<MaybeSslStream<InternalStream>>,
    next_stmt_id: uint,
    notice_handler: Box<NoticeHandler+Send>,
    notifications: RingBuf<Notification>,
    cancel_data: CancelData,
    unknown_types: HashMap<Oid, String>,
    desynchronized: bool,
    finished: bool,
    trans_depth: u32,
    canary: u32,
}

impl Drop for InnerConnection {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish_inner();
        }
    }
}

impl InnerConnection {
    fn connect<T>(params: T, ssl: &SslMode)
                  -> result::Result<InnerConnection, ConnectError>
            where T: IntoConnectParams {
        let params = try!(params.into_connect_params());
        let stream = try!(io::initialize_stream(&params, ssl));

        let ConnectParams {
            user,
            database,
            mut options,
            ..
        } = params;

        let user = try!(user.ok_or(MissingUser));

        let mut conn = InnerConnection {
            stream: BufferedStream::new(stream),
            next_stmt_id: 0,
            notice_handler: box DefaultNoticeHandler,
            notifications: RingBuf::new(),
            cancel_data: CancelData { process_id: 0, secret_key: 0 },
            unknown_types: HashMap::new(),
            desynchronized: false,
            finished: false,
            trans_depth: 0,
            canary: CANARY,
        };

        options.push(("client_encoding".into_string(), "UTF8".into_string()));
        // Postgres uses the value of TimeZone as the time zone for TIMESTAMP
        // WITH TIME ZONE values. Timespec converts to GMT internally.
        options.push(("TimeZone".into_string(), "GMT".into_string()));
        // We have to clone here since we need the user again for auth
        options.push(("user".into_string(), user.user.clone()));
        match database {
            Some(database) => options.push(("database".into_string(), database)),
            None => {}
        }

        try_pg_conn!(conn.write_messages([StartupMessage {
            version: message::PROTOCOL_VERSION,
            parameters: options[]
        }]));

        try!(conn.handle_auth(user));

        loop {
            match try_pg_conn!(conn.read_message()) {
                BackendKeyData { process_id, secret_key } => {
                    conn.cancel_data.process_id = process_id;
                    conn.cancel_data.secret_key = secret_key;
                }
                ReadyForQuery { .. } => break,
                ErrorResponse { fields } => return DbError::new_connect(fields),
                _ => return Err(PgConnectBadResponse),
            }
        }

        Ok(conn)
    }

    fn write_messages(&mut self, messages: &[FrontendMessage]) -> IoResult<()> {
        debug_assert!(!self.desynchronized);
        for message in messages.iter() {
            try_desync!(self, self.stream.write_message(message));
        }
        Ok(try_desync!(self, self.stream.flush()))
    }

    fn read_message(&mut self) -> IoResult<BackendMessage> {
        debug_assert!(!self.desynchronized);
        loop {
            match try_desync!(self, self.stream.read_message()) {
                NoticeResponse { fields } => {
                    if let Ok(err) = DbError::new_raw(fields) {
                        self.notice_handler.handle(err);
                    }
                }
                NotificationResponse { pid, channel, payload } => {
                    self.notifications.push(Notification {
                        pid: pid,
                        channel: channel,
                        payload: payload
                    })
                }
                ParameterStatus { parameter, value } => {
                    debug!("Parameter {} = {}", parameter, value)
                }
                val => return Ok(val)
            }
        }
    }

    fn handle_auth(&mut self, user: UserInfo) -> result::Result<(), ConnectError> {
        match try_pg_conn!(self.read_message()) {
            AuthenticationOk => return Ok(()),
            AuthenticationCleartextPassword => {
                let pass = try!(user.password.ok_or(MissingPassword));
                try_pg_conn!(self.write_messages([PasswordMessage {
                        password: pass[],
                    }]));
            }
            AuthenticationMD5Password { salt } => {
                let pass = try!(user.password.ok_or(MissingPassword));
                let hasher = Hasher::new(MD5);
                hasher.update(pass.as_bytes());
                hasher.update(user.user.as_bytes());
                let output = hasher.finalize()[].to_hex();
                let hasher = Hasher::new(MD5);
                hasher.update(output.as_bytes());
                hasher.update(salt);
                let output = format!("md5{}",
                                     hasher.finalize()[].to_hex());
                try_pg_conn!(self.write_messages([PasswordMessage {
                        password: output[]
                    }]));
            }
            AuthenticationKerberosV5
            | AuthenticationSCMCredential
            | AuthenticationGSS
            | AuthenticationSSPI => return Err(UnsupportedAuthentication),
            ErrorResponse { fields } => return DbError::new_connect(fields),
            _ => {
                self.desynchronized = true;
                return Err(PgConnectBadResponse);
            }
        }

        match try_pg_conn!(self.read_message()) {
            AuthenticationOk => Ok(()),
            ErrorResponse { fields } => return DbError::new_connect(fields),
            _ => {
                self.desynchronized = true;
                return Err(PgConnectBadResponse);
            }
        }
    }

    fn set_notice_handler(&mut self, handler: Box<NoticeHandler+Send>) -> Box<NoticeHandler+Send> {
        mem::replace(&mut self.notice_handler, handler)
    }

    fn raw_prepare(&mut self, query: &str)
                   -> Result<(String, Vec<Type>, Vec<ResultDescription>)> {
        let stmt_name = format!("s{}", self.next_stmt_id);
        self.next_stmt_id += 1;

        try_pg!(self.write_messages([
            Parse {
                name: stmt_name[],
                query: query,
                param_types: []
            },
            Describe {
                variant: b'S',
                name: stmt_name[],
            },
            Sync]));

        match try_pg!(self.read_message()) {
            ParseComplete => {}
            ErrorResponse { fields } => {
                try!(self.wait_for_ready());
                return DbError::new(fields);
            }
            _ => bad_response!(self),
        }

        let mut param_types: Vec<_> = match try_pg!(self.read_message()) {
            ParameterDescription { types } => {
                types.iter().map(|ty| Type::from_oid(*ty)).collect()
            }
            _ => bad_response!(self),
        };

        let mut result_desc: Vec<_> = match try_pg!(self.read_message()) {
            RowDescription { descriptions } => {
                descriptions.into_iter().map(|RowDescriptionEntry { name, type_oid, .. }| {
                    ResultDescription {
                        name: name,
                        ty: Type::from_oid(type_oid)
                    }
                }).collect()
            }
            NoData => vec![],
            _ => bad_response!(self)
        };

        try!(self.wait_for_ready());

        // now that the connection is ready again, get unknown type names
        try!(self.set_type_names(param_types.iter_mut()));
        try!(self.set_type_names(result_desc.iter_mut().map(|d| &mut d.ty)));

        Ok((stmt_name, param_types, result_desc))
    }

    fn prepare<'a>(&mut self, query: &str, conn: &'a Connection) -> Result<Statement<'a>> {
        let (stmt_name, param_types, result_desc) = try!(self.raw_prepare(query));
        Ok(Statement {
            conn: conn,
            name: stmt_name,
            param_types: param_types,
            result_desc: result_desc,
            next_portal_id: Cell::new(0),
            finished: false,
        })
    }

    fn prepare_copy_in<'a>(&mut self, table: &str, rows: &[&str], conn: &'a Connection)
                           -> Result<CopyInStatement<'a>> {
        let mut query = MemWriter::new();
        let _ = write!(query, "SELECT ");
        let _ = util::comma_join(&mut query, rows.iter().map(|&e| e));
        let _ = write!(query, " FROM {}", table);
        let query = String::from_utf8(query.unwrap()).unwrap();
        let (stmt_name, _, result_desc) = try!(self.raw_prepare(query[]));

        let column_types = result_desc.iter().map(|desc| desc.ty.clone()).collect();
        try!(self.close_statement(stmt_name[]));

        let mut query = MemWriter::new();
        let _ = write!(query, "COPY {} (", table);
        let _ = util::comma_join(&mut query, rows.iter().map(|&e| e));
        let _ = write!(query, ") FROM STDIN WITH (FORMAT binary)");
        let query = String::from_utf8(query.unwrap()).unwrap();
        let (stmt_name, _, _) = try!(self.raw_prepare(query[]));

        Ok(CopyInStatement {
            conn: conn,
            name: stmt_name,
            column_types: column_types,
            finished: false,
        })
    }

    fn close_statement(&mut self, stmt_name: &str) -> Result<()> {
        try_pg!(self.write_messages([
            Close {
                variant: b'S',
                name: stmt_name,
            },
            Sync]));
        loop {
            match try_pg!(self.read_message()) {
                ReadyForQuery { .. } => break,
                ErrorResponse { fields } => {
                    try!(self.wait_for_ready());
                    return DbError::new(fields);
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn set_type_names<'a, I>(&mut self, mut it: I) -> Result<()>
            where I: Iterator<&'a mut Type> {
        for ty in it {
            if let &Type::Unknown { oid, ref mut name } = ty {
                *name = try!(self.get_type_name(oid));
            }
        }

        Ok(())
    }

    fn get_type_name(&mut self, oid: Oid) -> Result<String> {
        if let Some(name) = self.unknown_types.find(&oid) {
            return Ok(name.clone());
        }
        let name = try!(self.quick_query(format!("SELECT typname FROM pg_type \
                                                  WHERE oid={}", oid)[]))
            .into_iter().next().unwrap().into_iter().next().unwrap().unwrap();
        self.unknown_types.insert(oid, name.clone());
        Ok(name)
    }

    fn is_desynchronized(&self) -> bool {
        self.desynchronized
    }

    fn canary(&self) -> u32 {
        self.canary
    }

    fn wait_for_ready(&mut self) -> Result<()> {
        match try_pg!(self.read_message()) {
            ReadyForQuery { .. } => Ok(()),
            _ => bad_response!(self)
        }
    }

    fn quick_query(&mut self, query: &str) -> Result<Vec<Vec<Option<String>>>> {
        check_desync!(self);
        try_pg!(self.write_messages([Query { query: query }]));

        let mut result = vec![];
        loop {
            match try_pg!(self.read_message()) {
                ReadyForQuery { .. } => break,
                DataRow { row } => {
                    result.push(row.into_iter().map(|opt| {
                        opt.map(|b| String::from_utf8_lossy(b[]).into_string())
                    }).collect());
                }
                CopyInResponse { .. } => {
                    try_pg!(self.write_messages([
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
        self.canary = 0;
        try_pg!(self.write_messages([Terminate]));
        Ok(())
    }
}

/// A connection to a Postgres database.
pub struct Connection {
    conn: RefCell<InnerConnection>
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
    /// # use postgres::{Connection, NoSsl};
    /// # let _ = || {
    /// let url = "postgresql://postgres:hunter2@localhost:2994/foodb";
    /// let conn = try!(Connection::connect(url, &NoSsl));
    /// # Ok(()) };
    /// ```
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, NoSsl};
    /// # let _ = || {
    /// let url = "postgresql://postgres@%2Frun%2Fpostgres";
    /// let conn = try!(Connection::connect(url, &NoSsl));
    /// # Ok(()) };
    /// ```
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, UserInfo, ConnectParams, NoSsl, ConnectTarget};
    /// # let _ = || {
    /// # let some_crazy_path = Path::new("");
    /// let params = ConnectParams {
    ///     target: ConnectTarget::Unix(some_crazy_path),
    ///     port: None,
    ///     user: Some(UserInfo {
    ///         user: "postgres".into_string(),
    ///         password: None
    ///     }),
    ///     database: None,
    ///     options: vec![],
    /// };
    /// let conn = try!(Connection::connect(params, &NoSsl));
    /// # Ok(()) };
    /// ```
    pub fn connect<T>(params: T, ssl: &SslMode) -> result::Result<Connection, ConnectError>
            where T: IntoConnectParams {
        InnerConnection::connect(params, ssl).map(|conn| {
            Connection { conn: RefCell::new(conn) }
        })
    }

    /// Sets the notice handler for the connection, returning the old handler.
    pub fn set_notice_handler(&self, handler: Box<NoticeHandler+Send>) -> Box<NoticeHandler+Send> {
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
    /// # use postgres::{Connection, NoSsl};
    /// # let conn = Connection::connect("", &NoSsl).unwrap();
    /// let maybe_stmt = conn.prepare("SELECT foo FROM bar WHERE baz = $1");
    /// let stmt = match maybe_stmt {
    ///     Ok(stmt) => stmt,
    ///     Err(err) => panic!("Error preparing statement: {}", err)
    /// };
    pub fn prepare<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        let mut conn = self.conn.borrow_mut();
        if conn.trans_depth != 0 {
            return Err(PgWrongTransaction);
        }
        conn.prepare(query, self)
    }

    /// Creates a new COPY FROM STDIN prepared statement.
    ///
    /// These statements provide a method to efficiently bulk-upload data to
    /// the database.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, NoSsl};
    /// # use postgres::types::ToSql;
    /// # let _ = || {
    /// # let conn = Connection::connect("", &NoSsl).unwrap();
    /// try!(conn.execute("CREATE TABLE foo (
    ///                     bar INT PRIMARY KEY,
    ///                     baz VARCHAR
    ///                    )", []));
    ///
    /// let stmt = try!(conn.prepare_copy_in("foo", ["bar", "baz"]));
    /// let data: &[&[&ToSql]] = &[&[&0i32, &"blah".into_string()],
    ///                            &[&1i32, &None::<String>]];
    /// try!(stmt.execute(data.iter().map(|r| r.iter().map(|&e| e))));
    /// # Ok(()) };
    /// ```
    pub fn prepare_copy_in<'a>(&'a self, table: &str, rows: &[&str])
                               -> Result<CopyInStatement<'a>> {
        let mut conn = self.conn.borrow_mut();
        if conn.trans_depth != 0 {
            return Err(PgWrongTransaction);
        }
        conn.prepare_copy_in(table, rows, self)
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
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, NoSsl};
    /// # fn foo() -> Result<(), postgres::Error> {
    /// # let conn = Connection::connect("", &NoSsl).unwrap();
    /// let trans = try!(conn.transaction());
    /// try!(trans.execute("UPDATE foo SET bar = 10", []));
    /// // ...
    ///
    /// try!(trans.commit());
    /// # Ok(())
    /// # }
    /// ```
    pub fn transaction<'a>(&'a self) -> Result<Transaction<'a>> {
        let mut conn = self.conn.borrow_mut();
        check_desync!(conn);
        if conn.trans_depth != 0 {
            return Err(PgWrongTransaction);
        }
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
    pub fn execute(&self, query: &str, params: &[&ToSql]) -> Result<uint> {
        self.prepare(query).and_then(|stmt| stmt.execute(params))
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
    /// data in the statment. Do not form statements via string concatenation
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
        let mut conn = self.conn.borrow_mut();
        if conn.trans_depth != 0 {
            return Err(PgWrongTransaction);
        }
        conn.quick_query(query).map(|_| ())
    }

    /// Returns information used to cancel pending queries.
    ///
    /// Used with the `cancel_query` function. The object returned can be used
    /// to cancel any query executed by the connection it was created from.
    pub fn cancel_data(&self) -> CancelData {
        self.conn.borrow().cancel_data
    }

    /// Returns whether or not the stream has been desynchronized due to an
    /// error in the communication channel with the server.
    ///
    /// If this has occurred, all further queries will immediately return an
    /// error.
    pub fn is_desynchronized(&self) -> bool {
        self.conn.borrow().is_desynchronized()
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

    fn canary(&self) -> u32 {
        self.conn.borrow().canary()
    }

    fn write_messages(&self, messages: &[FrontendMessage]) -> IoResult<()> {
        self.conn.borrow_mut().write_messages(messages)
    }
}

/// Specifies the SSL support requested for a new connection
pub enum SslMode {
    /// The connection will not use SSL
    NoSsl,
    /// The connection will use SSL if the backend supports it
    PreferSsl(SslContext),
    /// The connection must use SSL
    RequireSsl(SslContext)
}

/// Represents a transaction on a database connection.
///
/// The transaction will roll back by default.
pub struct Transaction<'conn> {
    conn: &'conn Connection,
    commit: Cell<bool>,
    depth: u32,
    finished: bool,
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
    pub fn prepare<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        let mut conn = self.conn.conn.borrow_mut();
        if conn.trans_depth != self.depth {
            return Err(PgWrongTransaction);
        }
        conn.prepare(query, self.conn)
    }

    /// Like `Connection::prepare_copy_in`.
    pub fn prepare_copy_in<'a>(&'a self, table: &str, cols: &[&str])
                               -> Result<CopyInStatement<'a>> {
        let mut conn = self.conn.conn.borrow_mut();
        if conn.trans_depth != self.depth {
            return Err(PgWrongTransaction);
        }
        conn.prepare_copy_in(table, cols, self.conn)
    }

    /// Like `Connection::execute`.
    pub fn execute(&self, query: &str, params: &[&ToSql]) -> Result<uint> {
        self.prepare(query).and_then(|s| s.execute(params))
    }

    /// Like `Connection::batch_execute`.
    pub fn batch_execute(&self, query: &str) -> Result<()> {
        let mut conn = self.conn.conn.borrow_mut();
        if conn.trans_depth != self.depth {
            return Err(PgWrongTransaction);
        }
        conn.quick_query(query).map(|_| ())
    }

    /// Like `Connection::transaction`.
    pub fn transaction<'a>(&'a self) -> Result<Transaction<'a>> {
        let mut conn = self.conn.conn.borrow_mut();
        check_desync!(conn);
        if conn.trans_depth != self.depth {
            return Err(PgWrongTransaction);
        }
        try!(conn.quick_query("SAVEPOINT sp"));
        conn.trans_depth += 1;
        Ok(Transaction {
            conn: self.conn,
            commit: Cell::new(false),
            depth: self.depth + 1,
            finished: false,
        })
    }

    /// Executes a prepared statement, returning a lazily loaded iterator over
    /// the resulting rows.
    ///
    /// No more than `row_limit` rows will be stored in memory at a time. Rows
    /// will be pulled from the database in batches of `row_limit` as needed.
    /// If `row_limit` is less than or equal to 0, `lazy_query` is equivalent
    /// to `query`.
    pub fn lazy_query<'trans, 'stmt>(&'trans self,
                                     stmt: &'stmt Statement,
                                     params: &[&ToSql],
                                     row_limit: i32)
                                     -> Result<LazyRows<'trans, 'stmt>> {
        if self.conn as *const _ != stmt.conn as *const _ {
            return Err(PgWrongConnection);
        }
        check_desync!(self.conn);
        stmt.lazy_query(row_limit, params).map(|result| {
            LazyRows {
                _trans: self,
                result: result
            }
        })
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
    /// Functionally equivalent to the `Drop` implementation of
    /// `Transaction` except that it returns any error to the caller.
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
    result_desc: Vec<ResultDescription>,
    next_portal_id: Cell<uint>,
    finished: bool,
}

#[unsafe_destructor]
impl<'conn> Drop for Statement<'conn> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish_inner();
        }
    }
}

impl<'conn> Statement<'conn> {
    fn finish_inner(&mut self) -> Result<()> {
        let mut conn = self.conn.conn.borrow_mut();
        check_desync!(conn);
        conn.close_statement(self.name[])
    }

    fn inner_execute(&self, portal_name: &str, row_limit: i32, params: &[&ToSql])
                     -> Result<()> {
        let mut conn = self.conn.conn.borrow_mut();
        if self.param_types.len() != params.len() {
            return Err(PgWrongParamCount {
                expected: self.param_types.len(),
                actual: params.len(),
            });
        }
        let mut values = vec![];
        for (param, ty) in params.iter().zip(self.param_types.iter()) {
            values.push(try!(param.to_sql(ty)));
        };

        try_pg!(conn.write_messages([
            Bind {
                portal: portal_name,
                statement: self.name[],
                formats: [1],
                values: values[],
                result_formats: [1]
            },
            Execute {
                portal: portal_name,
                max_rows: row_limit
            },
            Sync]));

        match try_pg!(conn.read_message()) {
            BindComplete => Ok(()),
            ErrorResponse { fields } => {
                try!(conn.wait_for_ready());
                DbError::new(fields)
            }
            _ => {
                conn.desynchronized = true;
                Err(PgBadResponse)
            }
        }
    }

    fn lazy_query<'a>(&'a self, row_limit: i32, params: &[&ToSql])
                      -> Result<Rows<'a>> {
        let id = self.next_portal_id.get();
        self.next_portal_id.set(id + 1);
        let portal_name = format!("{}p{}", self.name, id);

        try!(self.inner_execute(portal_name[], row_limit, params));

        let mut result = Rows {
            stmt: self,
            name: portal_name,
            data: RingBuf::new(),
            row_limit: row_limit,
            more_rows: true,
            finished: false,
        };
        try!(result.read_rows())

        Ok(result)
    }

    /// Returns a slice containing the expected parameter types.
    pub fn param_types(&self) -> &[Type] {
        self.param_types[]
    }

    /// Returns a slice describing the columns of the result of the query.
    pub fn result_descriptions(&self) -> &[ResultDescription] {
        self.result_desc[]
    }

    /// Executes the prepared statement, returning the number of rows modified.
    ///
    /// If the statement does not modify any rows (e.g. SELECT), 0 is returned.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, NoSsl};
    /// # let conn = Connection::connect("", &NoSsl).unwrap();
    /// # let bar = 1i32;
    /// # let baz = true;
    /// let stmt = conn.prepare("UPDATE foo SET bar = $1 WHERE baz = $2").unwrap();
    /// match stmt.execute(&[&bar, &baz]) {
    ///     Ok(count) => println!("{} row(s) updated", count),
    ///     Err(err) => println!("Error executing query: {}", err)
    /// }
    pub fn execute(&self, params: &[&ToSql]) -> Result<uint> {
        check_desync!(self.conn);
        try!(self.inner_execute("", 0, params));

        let mut conn = self.conn.conn.borrow_mut();
        let num;
        loop {
            match try_pg!(conn.read_message()) {
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
                    try_pg!(conn.write_messages([
                        CopyFail {
                            message: "COPY queries cannot be directly executed",
                        },
                        Sync]));
                }
                _ => {
                    conn.desynchronized = true;
                    return Err(PgBadResponse);
                }
            }
        }
        try!(conn.wait_for_ready());

        Ok(num)
    }

    /// Executes the prepared statement, returning an iterator over the
    /// resulting rows.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, NoSsl};
    /// # let conn = Connection::connect("", &NoSsl).unwrap();
    /// let stmt = conn.prepare("SELECT foo FROM bar WHERE baz = $1").unwrap();
    /// # let baz = true;
    /// let mut rows = match stmt.query(&[&baz]) {
    ///     Ok(rows) => rows,
    ///     Err(err) => panic!("Error running query: {}", err)
    /// };
    /// for row in rows {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// ```
    pub fn query<'a>(&'a self, params: &[&ToSql]) -> Result<Rows<'a>> {
        check_desync!(self.conn);
        self.lazy_query(0, params)
    }

    /// Consumes the statement, clearing it from the Postgres session.
    ///
    /// Functionally identical to the `Drop` implementation of the
    /// `Statement` except that it returns any error to the caller.
    pub fn finish(mut self) -> Result<()> {
        self.finished = true;
        self.finish_inner()
    }
}

/// Information about a column of the result of a query.
#[deriving(PartialEq, Eq)]
pub struct ResultDescription {
    /// The name of the column
    pub name: String,
    /// The type of the data in the column
    pub ty: Type
}

/// An iterator over the resulting rows of a query.
pub struct Rows<'stmt> {
    stmt: &'stmt Statement<'stmt>,
    name: String,
    data: RingBuf<Vec<Option<Vec<u8>>>>,
    row_limit: i32,
    more_rows: bool,
    finished: bool,
}

#[unsafe_destructor]
impl<'stmt> Drop for Rows<'stmt> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish_inner();
        }
    }
}

impl<'stmt> Rows<'stmt> {
    fn finish_inner(&mut self) -> Result<()> {
        let mut conn = self.stmt.conn.conn.borrow_mut();
        check_desync!(conn);
        try_pg!(conn.write_messages([
            Close {
                variant: b'P',
                name: self.name[]
            },
            Sync]));

        loop {
            match try_pg!(conn.read_message()) {
                ReadyForQuery { .. } => break,
                ErrorResponse { fields } => {
                    try!(conn.wait_for_ready());
                    return DbError::new(fields);
                }
                _ => {}
            }
        }

        Ok(())
    }

    fn read_rows(&mut self) -> Result<()> {
        let mut conn = self.stmt.conn.conn.borrow_mut();
        loop {
            match try_pg!(conn.read_message()) {
                EmptyQueryResponse | CommandComplete { .. } => {
                    self.more_rows = false;
                    break;
                },
                PortalSuspended => {
                    self.more_rows = true;
                    break;
                },
                DataRow { row } => self.data.push(row),
                ErrorResponse { fields } => {
                    try!(conn.wait_for_ready());
                    return DbError::new(fields);
                }
                CopyInResponse { .. } => {
                    try_pg!(conn.write_messages([
                        CopyFail {
                            message: "COPY queries cannot be directly executed",
                        },
                        Sync]));
                }
                _ => {
                    conn.desynchronized = true;
                    return Err(PgBadResponse);
                }
            }
        }
        conn.wait_for_ready()
    }

    fn execute(&mut self) -> Result<()> {
        try_pg!(self.stmt.conn.write_messages([
            Execute {
                portal: self.name[],
                max_rows: self.row_limit
            },
            Sync]));
        self.read_rows()
    }

    /// Consumes the `Rows`, cleaning up associated state.
    ///
    /// Functionally identical to the `Drop` implementation on `Rows`
    /// except that it returns any error to the caller.
    pub fn finish(mut self) -> Result<()> {
        self.finished = true;
        self.finish_inner()
    }

    fn try_next(&mut self) -> Option<Result<Row<'stmt>>> {
        if self.data.is_empty() && self.more_rows {
            if let Err(err) = self.execute() {
                return Some(Err(err));
            }
        }

        self.data.pop_front().map(|row| Ok(Row { stmt: self.stmt, data: row }))
    }
}

impl<'stmt> Iterator<Row<'stmt>> for Rows<'stmt> {
    #[inline]
    fn next(&mut self) -> Option<Row<'stmt>> {
        // we'll never hit the network on a non-lazy result
        self.try_next().map(|r| r.unwrap())
    }

    #[inline]
    fn size_hint(&self) -> (uint, Option<uint>) {
        let lower = self.data.len();
        let upper = if self.more_rows {
            None
         } else {
            Some(lower)
         };
         (lower, upper)
    }
}

/// A single result row of a query.
pub struct Row<'stmt> {
    stmt: &'stmt Statement<'stmt>,
    data: Vec<Option<Vec<u8>>>
}

impl<'stmt> Row<'stmt> {
    /// Returns the number of values in the row
    #[inline]
    pub fn len(&self) -> uint {
        self.data.len()
    }

    /// Retrieves the contents of a field of the row.
    ///
    /// A field can be accessed by the name or index of its column, though
    /// access by index is more efficient. Rows are 0-indexed.
    ///
    /// Returns an `Error` value if the index does not reference a column or
    /// the return type is not compatible with the Postgres type.
    pub fn get_opt<I, T>(&self, idx: I) -> Result<T> where I: RowIndex, T: FromSql {
        let idx = try!(idx.idx(self.stmt).ok_or(PgInvalidColumn));
        FromSql::from_sql(&self.stmt.result_desc[idx].ty, &self.data[idx])
    }

    /// Retrieves the contents of a field of the row.
    ///
    /// A field can be accessed by the name or index of its column, though
    /// access by index is more efficient. Rows are 0-indexed.
    ///
    /// ## Failure
    ///
    /// Fails if the index does not reference a column or the return type is
    /// not compatible with the Postgres type.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, NoSsl};
    /// # let conn = Connection::connect("", &NoSsl).unwrap();
    /// # let stmt = conn.prepare("").unwrap();
    /// # let mut result = stmt.query([]).unwrap();
    /// # let row = result.next().unwrap();
    /// let foo: i32 = row.get(0u);
    /// let bar: String = row.get("bar");
    /// ```
    pub fn get<I, T>(&self, idx: I) -> T where I: RowIndex + fmt::Show + Clone, T: FromSql {
        match self.get_opt(idx.clone()) {
            Ok(ok) => ok,
            Err(err) => panic!("error retrieving column {}: {}", idx, err)
        }
    }
}

/// A trait implemented by types that can index into columns of a row.
pub trait RowIndex {
    /// Returns the index of the appropriate column, or `None` if no such
    /// column exists.
    fn idx(&self, stmt: &Statement) -> Option<uint>;
}

impl RowIndex for uint {
    #[inline]
    fn idx(&self, stmt: &Statement) -> Option<uint> {
        if *self > stmt.result_desc.len() {
            None
        } else {
            Some(*self)
        }
    }
}

impl<'a> RowIndex for &'a str {
    #[inline]
    fn idx(&self, stmt: &Statement) -> Option<uint> {
        stmt.result_descriptions().iter().position(|d| d.name[] == *self)
    }
}

/// A lazily-loaded iterator over the resulting rows of a query
pub struct LazyRows<'trans, 'stmt> {
    result: Rows<'stmt>,
    _trans: &'trans Transaction<'trans>,
}

impl<'trans, 'stmt> LazyRows<'trans, 'stmt> {
    /// Like `Rows::finish`.
    #[inline]
    pub fn finish(self) -> Result<()> {
        self.result.finish()
    }
}

impl<'trans, 'stmt> Iterator<Result<Row<'stmt>>> for LazyRows<'trans, 'stmt> {
    #[inline]
    fn next(&mut self) -> Option<Result<Row<'stmt>>> {
        self.result.try_next()
    }

    #[inline]
    fn size_hint(&self) -> (uint, Option<uint>) {
        self.result.size_hint()
    }
}

/// A prepared COPY FROM STDIN statement
pub struct CopyInStatement<'a> {
    conn: &'a Connection,
    name: String,
    column_types: Vec<Type>,
    finished: bool,
}

#[unsafe_destructor]
impl<'a> Drop for CopyInStatement<'a> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish_inner();
        }
    }
}

impl<'a> CopyInStatement<'a> {
    fn finish_inner(&mut self) -> Result<()> {
        let mut conn = self.conn.conn.borrow_mut();
        check_desync!(conn);
        conn.close_statement(self.name[])
    }

    /// Returns a slice containing the expected column types.
    pub fn column_types(&self) -> &[Type] {
        self.column_types[]
    }

    /// Executes the prepared statement.
    ///
    /// Each iterator returned by the `rows` iterator will be interpreted as
    /// providing a single result row.
    ///
    /// Returns the number of rows copied.
    pub fn execute<'b, I, J>(&self, mut rows: I) -> Result<uint>
            where I: Iterator<J>, J: Iterator<&'b ToSql + 'b> {
        let mut conn = self.conn.conn.borrow_mut();

        try_pg!(conn.write_messages([
            Bind {
                portal: "",
                statement: self.name[],
                formats: [],
                values: [],
                result_formats: []
            },
            Execute {
                portal: "",
                max_rows: 0,
            },
            Sync]));

        match try_pg!(conn.read_message()) {
            BindComplete => {},
            ErrorResponse { fields } => {
                try!(conn.wait_for_ready());
                return DbError::new(fields);
            }
            _ => {
                conn.desynchronized = true;
                return Err(PgBadResponse);
            }
        }

        match try_pg!(conn.read_message()) {
            CopyInResponse { .. } => {}
            _ => {
                conn.desynchronized = true;
                return Err(PgBadResponse);
            }
        }

        let mut buf = MemWriter::new();
        let _ = buf.write(b"PGCOPY\n\xff\r\n\x00");
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
                                let _ = buf.write(val[]);
                            }
                            Err(err) => {
                                // FIXME this is not the right way to handle this
                                try_pg_desync!(conn, conn.stream.write_message(
                                    &CopyFail {
                                        message: err.to_string()[],
                                    }));
                                break 'l;
                            }
                        }
                    }
                    (Some(_), None) | (None, Some(_)) => {
                        try_pg_desync!(conn, conn.stream.write_message(
                            &CopyFail {
                                message: "Invalid column count",
                            }));
                        break 'l;
                    }
                    (None, None) => break
                }
            }

            let mut data = buf.unwrap();
            try_pg_desync!(conn, conn.stream.write_message(
                &CopyData {
                    data: data[]
                }));
            data.clear();
            buf = MemWriter::from_vec(data);
        }

        let _ = buf.write_be_i16(-1);
        try_pg!(conn.write_messages([
            CopyData {
                data: buf.unwrap()[],
            },
            CopyDone,
            Sync]));

        let num = match try_pg!(conn.read_message()) {
            CommandComplete { tag } => util::parse_update_count(tag),
            ErrorResponse { fields } => {
                try!(conn.wait_for_ready());
                return DbError::new(fields);
            }
            _ => {
                conn.desynchronized = true;
                return Err(PgBadResponse);
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

    /// Like `Connection::execute`.
    fn execute(&self, query: &str, params: &[&ToSql]) -> Result<uint> {
        self.prepare(query).and_then(|s| s.execute(params))
    }

    /// Like `Connection::prepare_copy_in`.
    fn prepare_copy_in<'a>(&'a self, table: &str, columns: &[&str])
                           -> Result<CopyInStatement<'a>>;

    /// Like `Connection::transaction`.
    fn transaction<'a>(&'a self) -> Result<Transaction<'a>>;

    /// Like `Connection::batch_execute`.
    fn batch_execute(&self, query: &str) -> Result<()>;
}

impl GenericConnection for Connection {
    fn prepare<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        self.prepare(query)
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
}

impl<'a> GenericConnection for Transaction<'a> {
    fn prepare<'a>(&'a self, query: &str) -> Result<Statement<'a>> {
        self.prepare(query)
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
}

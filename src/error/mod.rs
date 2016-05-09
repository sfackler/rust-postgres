//! Error types.

use std::error;
use std::convert::From;
use std::fmt;
use std::io;
use std::result;
use std::collections::HashMap;

pub use self::sqlstate::SqlState;
use {Result, DbErrorNew};

mod sqlstate;

/// A Postgres error or notice.
#[derive(Clone, PartialEq, Eq)]
pub struct DbError {
    /// The field contents are ERROR, FATAL, or PANIC (in an error message),
    /// or WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message), or a
    /// localized translation of one of these.
    pub severity: String,

    /// The SQLSTATE code for the error.
    pub code: SqlState,

    /// The primary human-readable error message. This should be accurate but
    /// terse (typically one line).
    pub message: String,

    /// An optional secondary error message carrying more detail about the
    /// problem. Might run to multiple lines.
    pub detail: Option<String>,

    /// An optional suggestion what to do about the problem. This is intended
    /// to differ from Detail in that it offers advice (potentially
    /// inappropriate) rather than hard facts. Might run to multiple lines.
    pub hint: Option<String>,

    /// An optional error cursor position into either the original query string
    /// or an internally generated query.
    pub position: Option<ErrorPosition>,

    /// An indication of the context in which the error occurred. Presently
    /// this includes a call stack traceback of active procedural language
    /// functions and internally-generated queries. The trace is one entry per
    /// line, most recent first.
    pub where_: Option<String>,

    /// If the error was associated with a specific database object, the name
    /// of the schema containing that object, if any. (PostgreSQL 9.3+)
    pub schema: Option<String>,

    /// If the error was associated with a specific table, the name of the
    /// table. (Refer to the schema name field for the name of the table's
    /// schema.) (PostgreSQL 9.3+)
    pub table: Option<String>,

    /// If the error was associated with a specific table column, the name of
    /// the column. (Refer to the schema and table name fields to identify the
    /// table.) (PostgreSQL 9.3+)
    pub column: Option<String>,

    /// If the error was associated with a specific data type, the name of the
    /// data type. (Refer to the schema name field for the name of the data
    /// type's schema.) (PostgreSQL 9.3+)
    pub datatype: Option<String>,

    /// If the error was associated with a specific constraint, the name of the
    /// constraint. Refer to fields listed above for the associated table or
    /// domain. (For this purpose, indexes are treated as constraints, even if
    /// they weren't created with constraint syntax.) (PostgreSQL 9.3+)
    pub constraint: Option<String>,

    /// The file name of the source-code location where the error was reported.
    pub file: Option<String>,

    /// The line number of the source-code location where the error was
    /// reported.
    pub line: Option<u32>,

    /// The name of the source-code routine reporting the error.
    pub routine: Option<String>,

    _p: (),
}

impl DbErrorNew for DbError {
    fn new_raw(fields: Vec<(u8, String)>) -> result::Result<DbError, ()> {
        let mut map: HashMap<_, _> = fields.into_iter().collect();
        Ok(DbError {
            severity: try!(map.remove(&b'S').ok_or(())),
            code: SqlState::from_code(try!(map.remove(&b'C').ok_or(()))),
            message: try!(map.remove(&b'M').ok_or(())),
            detail: map.remove(&b'D'),
            hint: map.remove(&b'H'),
            position: match map.remove(&b'P') {
                Some(pos) => Some(ErrorPosition::Normal(try!(pos.parse().map_err(|_| ())))),
                None => {
                    match map.remove(&b'p') {
                        Some(pos) => {
                            Some(ErrorPosition::Internal {
                                position: try!(pos.parse().map_err(|_| ())),
                                query: try!(map.remove(&b'q').ok_or(())),
                            })
                        }
                        None => None,
                    }
                }
            },
            where_: map.remove(&b'W'),
            schema: map.remove(&b's'),
            table: map.remove(&b't'),
            column: map.remove(&b'c'),
            datatype: map.remove(&b'd'),
            constraint: map.remove(&b'n'),
            file: map.remove(&b'F'),
            line: map.remove(&b'L').and_then(|l| l.parse().ok()),
            routine: map.remove(&b'R'),
            _p: (),
        })
    }

    fn new_connect<T>(fields: Vec<(u8, String)>) -> result::Result<T, ConnectError> {
        match DbError::new_raw(fields) {
            Ok(err) => Err(ConnectError::Db(Box::new(err))),
            Err(()) => Err(ConnectError::Io(::bad_response())),
        }
    }

    fn new<T>(fields: Vec<(u8, String)>) -> Result<T> {
        match DbError::new_raw(fields) {
            Ok(err) => Err(Error::Db(Box::new(err))),
            Err(()) => Err(Error::Io(::bad_response())),
        }
    }
}

// manual impl to leave out _p
impl fmt::Debug for DbError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("DbError")
           .field("severity", &self.severity)
           .field("code", &self.code)
           .field("message", &self.message)
           .field("detail", &self.detail)
           .field("hint", &self.hint)
           .field("position", &self.position)
           .field("where_", &self.where_)
           .field("schema", &self.schema)
           .field("table", &self.table)
           .field("column", &self.column)
           .field("datatype", &self.datatype)
           .field("constraint", &self.constraint)
           .field("file", &self.file)
           .field("line", &self.line)
           .field("routine", &self.routine)
           .finish()
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}: {}", self.severity, self.message)
    }
}

impl error::Error for DbError {
    fn description(&self) -> &str {
        &self.message
    }
}

/// Reasons a new Postgres connection could fail.
#[derive(Debug)]
pub enum ConnectError {
    /// An error relating to connection parameters.
    ConnectParams(Box<error::Error + Sync + Send>),
    /// An error from the Postgres server itself.
    Db(Box<DbError>),
    /// An error initializing the SSL session.
    Ssl(Box<error::Error + Sync + Send>),
    /// An error communicating with the server.
    Io(io::Error),
}

impl fmt::Display for ConnectError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        try!(fmt.write_str(error::Error::description(self)));
        match *self {
            ConnectError::ConnectParams(ref msg) => write!(fmt, ": {}", msg),
            ConnectError::Db(ref err) => write!(fmt, ": {}", err),
            ConnectError::Ssl(ref err) => write!(fmt, ": {}", err),
            ConnectError::Io(ref err) => write!(fmt, ": {}", err),
        }
    }
}

impl error::Error for ConnectError {
    fn description(&self) -> &str {
        match *self {
            ConnectError::ConnectParams(_) => "Invalid connection parameters",
            ConnectError::Db(_) => "Error reported by Postgres",
            ConnectError::Ssl(_) => "Error initiating SSL session",
            ConnectError::Io(_) => "Error communicating with the server",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            ConnectError::ConnectParams(ref err) | ConnectError::Ssl(ref err) => Some(&**err),
            ConnectError::Db(ref err) => Some(&**err),
            ConnectError::Io(ref err) => Some(err),
        }
    }
}

impl From<io::Error> for ConnectError {
    fn from(err: io::Error) -> ConnectError {
        ConnectError::Io(err)
    }
}

impl From<DbError> for ConnectError {
    fn from(err: DbError) -> ConnectError {
        ConnectError::Db(Box::new(err))
    }
}

/// Represents the position of an error in a query.
#[derive(Clone, PartialEq, Eq, Debug)]
pub enum ErrorPosition {
    /// A position in the original query.
    Normal(u32),
    /// A position in an internally generated query.
    Internal {
        /// The byte position.
        position: u32,
        /// A query generated by the Postgres server.
        query: String,
    },
}

/// An error encountered when communicating with the Postgres server.
#[derive(Debug)]
pub enum Error {
    /// An error reported by the Postgres server.
    Db(Box<DbError>),
    /// An error communicating with the Postgres server.
    Io(io::Error),
    /// An error converting between Postgres and Rust types.
    Conversion(Box<error::Error + Sync + Send>),
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        try!(fmt.write_str(error::Error::description(self)));
        match *self {
            Error::Db(ref err) => write!(fmt, ": {}", err),
            Error::Io(ref err) => write!(fmt, ": {}", err),
            Error::Conversion(ref err) => write!(fmt, ": {}", err),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Db(_) => "Error reported by Postgres",
            Error::Io(_) => "Error communicating with the server",
            Error::Conversion(_) => "Error converting between Postgres and Rust types",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Db(ref err) => Some(&**err),
            Error::Io(ref err) => Some(err),
            Error::Conversion(ref err) => Some(&**err),
        }
    }
}

impl From<DbError> for Error {
    fn from(err: DbError) -> Error {
        Error::Db(Box::new(err))
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> io::Error {
        io::Error::new(io::ErrorKind::Other, err)
    }
}

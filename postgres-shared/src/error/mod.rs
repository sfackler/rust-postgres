//! Errors.

use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::ErrorFields;
use std::error;
use std::convert::From;
use std::fmt;
use std::io;

pub use self::sqlstate::*;

mod sqlstate;

/// The severity of a Postgres error or notice.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Severity {
    /// PANIC
    Panic,
    /// FATAL
    Fatal,
    /// ERROR
    Error,
    /// WARNING
    Warning,
    /// NOTICE
    Notice,
    /// DEBUG
    Debug,
    /// INFO
    Info,
    /// LOG
    Log,
}

impl fmt::Display for Severity {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        let s = match *self {
            Severity::Panic => "PANIC",
            Severity::Fatal => "FATAL",
            Severity::Error => "ERROR",
            Severity::Warning => "WARNING",
            Severity::Notice => "NOTICE",
            Severity::Debug => "DEBUG",
            Severity::Info => "INFO",
            Severity::Log => "LOG",
        };
        fmt.write_str(s)
    }
}

impl Severity {
    fn from_str(s: &str) -> Option<Severity> {
        match s {
            "PANIC" => Some(Severity::Panic),
            "FATAL" => Some(Severity::Fatal),
            "ERROR" => Some(Severity::Error),
            "WARNING" => Some(Severity::Warning),
            "NOTICE" => Some(Severity::Notice),
            "DEBUG" => Some(Severity::Debug),
            "INFO" => Some(Severity::Info),
            "LOG" => Some(Severity::Log),
            _ => None,
        }
    }
}

/// A Postgres error or notice.
#[derive(Clone, PartialEq, Eq)]
pub struct DbError {
    /// The field contents are ERROR, FATAL, or PANIC (in an error message),
    /// or WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message), or a
    /// localized translation of one of these.
    pub severity: String,

    /// A parsed, nonlocalized version of `severity`. (PostgreSQL 9.6+)
    pub parsed_severity: Option<Severity>,

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

impl DbError {
    #[doc(hidden)]
    pub fn new(fields: &mut ErrorFields) -> io::Result<DbError> {
        let mut severity = None;
        let mut parsed_severity = None;
        let mut code = None;
        let mut message = None;
        let mut detail = None;
        let mut hint = None;
        let mut normal_position = None;
        let mut internal_position = None;
        let mut internal_query = None;
        let mut where_ = None;
        let mut schema = None;
        let mut table = None;
        let mut column = None;
        let mut datatype = None;
        let mut constraint = None;
        let mut file = None;
        let mut line = None;
        let mut routine = None;

        while let Some(field) = fields.next()? {
            match field.type_() {
                b'S' => severity = Some(field.value().to_owned()),
                b'C' => code = Some(SqlState::from_code(field.value())),
                b'M' => message = Some(field.value().to_owned()),
                b'D' => detail = Some(field.value().to_owned()),
                b'H' => hint = Some(field.value().to_owned()),
                b'P' => {
                    normal_position = Some(field.value().parse::<u32>().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "`P` field did not contain an integer",
                        )
                    })?);
                }
                b'p' => {
                    internal_position = Some(field.value().parse::<u32>().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "`p` field did not contain an integer",
                        )
                    })?);
                }
                b'q' => internal_query = Some(field.value().to_owned()),
                b'W' => where_ = Some(field.value().to_owned()),
                b's' => schema = Some(field.value().to_owned()),
                b't' => table = Some(field.value().to_owned()),
                b'c' => column = Some(field.value().to_owned()),
                b'd' => datatype = Some(field.value().to_owned()),
                b'n' => constraint = Some(field.value().to_owned()),
                b'F' => file = Some(field.value().to_owned()),
                b'L' => {
                    line = Some(field.value().parse::<u32>().map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "`L` field did not contain an integer",
                        )
                    })?);
                }
                b'R' => routine = Some(field.value().to_owned()),
                b'V' => {
                    parsed_severity = Some(Severity::from_str(field.value()).ok_or_else(|| {
                        io::Error::new(
                            io::ErrorKind::InvalidInput,
                            "`V` field contained an invalid value",
                        )
                    })?);
                }
                _ => {}
            }
        }

        Ok(DbError {
            severity: severity.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "`S` field missing")
            })?,
            parsed_severity: parsed_severity,
            code: code.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "`C` field missing")
            })?,
            message: message.ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "`M` field missing")
            })?,
            detail: detail,
            hint: hint,
            position: match normal_position {
                Some(position) => Some(ErrorPosition::Normal(position)),
                None => {
                    match internal_position {
                        Some(position) => {
                            Some(ErrorPosition::Internal {
                                position: position,
                                query: internal_query.ok_or_else(|| {
                                    io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        "`q` field missing but `p` field present",
                                    )
                                })?,
                            })
                        }
                        None => None,
                    }
                }
            },
            where_: where_,
            schema: schema,
            table: table,
            column: column,
            datatype: datatype,
            constraint: constraint,
            file: file,
            line: line,
            routine: routine,
            _p: (),
        })
    }
}

// manual impl to leave out _p
impl fmt::Debug for DbError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("DbError")
            .field("severity", &self.severity)
            .field("parsed_severity", &self.parsed_severity)
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

#[doc(hidden)]
pub fn connect(e: Box<error::Error + Sync + Send>) -> Error {
    Error(Box::new(ErrorKind::ConnectParams(e)))
}

#[doc(hidden)]
pub fn tls(e: Box<error::Error + Sync + Send>) -> Error {
    Error(Box::new(ErrorKind::Tls(e)))
}

#[doc(hidden)]
pub fn db(e: DbError) -> Error {
    Error(Box::new(ErrorKind::Db(e)))
}

#[doc(hidden)]
pub fn io(e: io::Error) -> Error {
    Error(Box::new(ErrorKind::Io(e)))
}

#[doc(hidden)]
pub fn conversion(e: Box<error::Error + Sync + Send>) -> Error {
    Error(Box::new(ErrorKind::Conversion(e)))
}

#[derive(Debug)]
enum ErrorKind {
    ConnectParams(Box<error::Error + Sync + Send>),
    Tls(Box<error::Error + Sync + Send>),
    Db(DbError),
    Io(io::Error),
    Conversion(Box<error::Error + Sync + Send>),
}

/// An error communicating with the Postgres server.
#[derive(Debug)]
pub struct Error(Box<ErrorKind>);

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(error::Error::description(self))?;
        match *self.0 {
            ErrorKind::ConnectParams(ref err) => write!(fmt, ": {}", err),
            ErrorKind::Tls(ref err) => write!(fmt, ": {}", err),
            ErrorKind::Db(ref err) => write!(fmt, ": {}", err),
            ErrorKind::Io(ref err) => write!(fmt, ": {}", err),
            ErrorKind::Conversion(ref err) => write!(fmt, ": {}", err),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self.0 {
            ErrorKind::ConnectParams(_) => "invalid connection parameters",
            ErrorKind::Tls(_) => "TLS handshake error",
            ErrorKind::Db(_) => "database error",
            ErrorKind::Io(_) => "IO error",
            ErrorKind::Conversion(_) => "type conversion error",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self.0 {
            ErrorKind::ConnectParams(ref err) => Some(&**err),
            ErrorKind::Tls(ref err) => Some(&**err),
            ErrorKind::Db(ref err) => Some(err),
            ErrorKind::Io(ref err) => Some(err),
            ErrorKind::Conversion(ref err) => Some(&**err),
        }
    }
}

impl Error {
    /// Returns the SQLSTATE error code associated with this error if it is a DB
    /// error.
    pub fn code(&self) -> Option<&SqlState> {
        self.as_db().map(|e| &e.code)
    }

    /// Returns the inner error if this is a connection parameter error.
    pub fn as_connection(&self) -> Option<&(error::Error + 'static + Sync + Send)> {
        match *self.0 {
            ErrorKind::ConnectParams(ref err) => Some(&**err),
            _ => None,
        }
    }

    /// Returns the `DbError` associated with this error if it is a DB error.
    pub fn as_db(&self) -> Option<&DbError> {
        match *self.0 {
            ErrorKind::Db(ref err) => Some(err),
            _ => None
        }
    }

    /// Returns the inner error if this is a conversion error.
    pub fn as_conversion(&self) -> Option<&(error::Error + 'static + Sync + Send)> {
        match *self.0 {
            ErrorKind::Conversion(ref err) => Some(&**err),
            _ => None,
        }
    }

    /// Returns the inner `io::Error` associated with this error if it is an IO
    /// error.
    pub fn as_io(&self) -> Option<&io::Error> {
        match *self.0 {
            ErrorKind::Io(ref err) => Some(err),
            _ => None,
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error(Box::new(ErrorKind::Io(err)))
    }
}

impl From<Error> for io::Error {
    fn from(err: Error) -> io::Error {
        match *err.0 {
            ErrorKind::Io(e) => e,
            _ => io::Error::new(io::ErrorKind::Other, err),
        }
    }
}

//! Errors.

use postgres_protocol::message::backend::ErrorResponseBody;
use std::error::{self, Error as _Error};
use std::fmt;
use std::io;

pub use postgres_protocol::error::{DbError, SqlState};

#[derive(Debug, PartialEq)]
enum Kind {
    Io,
    UnexpectedMessage,
    Tls,
    ToSql(usize),
    FromSql(usize),
    Column(String),
    Closed,
    Db,
    Parse,
    Encode,
    Authentication,
    ConfigParse,
    Config,
    RowCount,
    #[cfg(feature = "runtime")]
    Connect,
}

struct ErrorInner {
    kind: Kind,
    cause: Option<Box<dyn error::Error + Sync + Send>>,
}

/// An error communicating with the Postgres server.
pub struct Error(Box<ErrorInner>);

impl fmt::Debug for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Error")
            .field("kind", &self.0.kind)
            .field("cause", &self.0.cause)
            .finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        match &self.0.kind {
            Kind::Io => fmt.write_str("error communicating with the server")?,
            Kind::UnexpectedMessage => fmt.write_str("unexpected message from server")?,
            Kind::Tls => fmt.write_str("error performing TLS handshake")?,
            Kind::ToSql(idx) => write!(fmt, "error serializing parameter {}", idx)?,
            Kind::FromSql(idx) => write!(fmt, "error deserializing column {}", idx)?,
            Kind::Column(column) => write!(fmt, "invalid column `{}`", column)?,
            Kind::Closed => fmt.write_str("connection closed")?,
            Kind::Db => fmt.write_str("db error")?,
            Kind::Parse => fmt.write_str("error parsing response from server")?,
            Kind::Encode => fmt.write_str("error encoding message to server")?,
            Kind::Authentication => fmt.write_str("authentication error")?,
            Kind::ConfigParse => fmt.write_str("invalid connection string")?,
            Kind::Config => fmt.write_str("invalid configuration")?,
            Kind::RowCount => fmt.write_str("query returned an unexpected number of rows")?,
            #[cfg(feature = "runtime")]
            Kind::Connect => fmt.write_str("error connecting to server")?,
        };
        if let Some(ref cause) = self.0.cause {
            write!(fmt, ": {}", cause)?;
        }
        Ok(())
    }
}

impl error::Error for Error {
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        self.0.cause.as_ref().map(|e| &**e as _)
    }
}

impl Error {
    /// Consumes the error, returning its cause.
    pub fn into_source(self) -> Option<Box<dyn error::Error + Sync + Send>> {
        self.0.cause
    }

    /// Returns the SQLSTATE error code associated with the error.
    ///
    /// This is a convenience method that downcasts the cause to a `DbError`
    /// and returns its code.
    pub fn code(&self) -> Option<&SqlState> {
        self.source()
            .and_then(|e| e.downcast_ref::<DbError>())
            .map(DbError::code)
    }

    fn new(kind: Kind, cause: Option<Box<dyn error::Error + Sync + Send>>) -> Error {
        Error(Box::new(ErrorInner { kind, cause }))
    }

    pub(crate) fn closed() -> Error {
        Error::new(Kind::Closed, None)
    }

    pub(crate) fn unexpected_message() -> Error {
        Error::new(Kind::UnexpectedMessage, None)
    }

    #[allow(clippy::needless_pass_by_value)]
    pub(crate) fn db(error: ErrorResponseBody) -> Error {
        match DbError::parse(&mut error.fields()) {
            Ok(e) => Error::new(Kind::Db, Some(Box::new(e))),
            Err(e) => Error::new(Kind::Parse, Some(Box::new(e))),
        }
    }

    pub(crate) fn parse(e: io::Error) -> Error {
        Error::new(Kind::Parse, Some(Box::new(e)))
    }

    pub(crate) fn encode(e: io::Error) -> Error {
        Error::new(Kind::Encode, Some(Box::new(e)))
    }

    #[allow(clippy::wrong_self_convention)]
    pub(crate) fn to_sql(e: Box<dyn error::Error + Sync + Send>, idx: usize) -> Error {
        Error::new(Kind::ToSql(idx), Some(e))
    }

    pub(crate) fn from_sql(e: Box<dyn error::Error + Sync + Send>, idx: usize) -> Error {
        Error::new(Kind::FromSql(idx), Some(e))
    }

    pub(crate) fn column(column: String) -> Error {
        Error::new(Kind::Column(column), None)
    }

    pub(crate) fn tls(e: Box<dyn error::Error + Sync + Send>) -> Error {
        Error::new(Kind::Tls, Some(e))
    }

    pub(crate) fn io(e: io::Error) -> Error {
        Error::new(Kind::Io, Some(Box::new(e)))
    }

    pub(crate) fn authentication(e: Box<dyn error::Error + Sync + Send>) -> Error {
        Error::new(Kind::Authentication, Some(e))
    }

    pub(crate) fn config_parse(e: Box<dyn error::Error + Sync + Send>) -> Error {
        Error::new(Kind::ConfigParse, Some(e))
    }

    pub(crate) fn config(e: Box<dyn error::Error + Sync + Send>) -> Error {
        Error::new(Kind::Config, Some(e))
    }

    pub(crate) fn row_count() -> Error {
        Error::new(Kind::RowCount, None)
    }

    #[cfg(feature = "runtime")]
    pub(crate) fn connect(e: io::Error) -> Error {
        Error::new(Kind::Connect, Some(Box::new(e)))
    }
}

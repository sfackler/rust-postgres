//! Error types.
use std::fmt;
use std::io;
use std::error;

#[doc(inline)]
// FIXME
pub use postgres_shared::error::*;

#[derive(Debug)]
pub(crate) enum ErrorKind {
    ConnectParams(Box<error::Error + Sync + Send>),
    Tls(Box<error::Error + Sync + Send>),
    Db(DbError),
    Io(io::Error),
    Conversion(Box<error::Error + Sync + Send>),
}

/// An error communicating with the Postgres server.
#[derive(Debug)]
pub struct Error(pub(crate) Box<ErrorKind>);

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

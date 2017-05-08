//! Error types.
use std::fmt;
use std::io;
use std::error;

#[doc(inline)]
pub use postgres_shared::error::{DbError, ConnectError, ErrorPosition, Severity, SqlState};

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
        fmt.write_str(error::Error::description(self))?;
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

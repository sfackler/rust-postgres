//! Error types.

use std::error;
use std::io;
use std::fmt;

use Connection;

#[doc(inline)]
pub use postgres_shared::error::{DbError, ConnectError, ErrorPosition, Severity, SqlState};

/// A runtime error.
#[derive(Debug)]
pub enum Error<C = Connection> {
    /// An error communicating with the database.
    ///
    /// IO errors are fatal - the connection is not returned.
    Io(io::Error),
    /// An error reported by the database.
    Db(Box<DbError>, C),
    /// An error converting between Rust and Postgres types.
    Conversion(Box<error::Error + Sync + Send>, C),
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(error::Error::description(self))?;
        match *self {
            Error::Db(ref err, _) => write!(fmt, ": {}", err),
            Error::Io(ref err) => write!(fmt, ": {}", err),
            Error::Conversion(ref err, _) => write!(fmt, ": {}", err),
        }
    }
}

impl error::Error for Error {
    fn description(&self) -> &str {
        match *self {
            Error::Db(_, _) => "Error reported by Postgres",
            Error::Io(_) => "Error communicating with the server",
            Error::Conversion(_, _) => "Error converting between Postgres and Rust types",
        }
    }

    fn cause(&self) -> Option<&error::Error> {
        match *self {
            Error::Db(ref err, _) => Some(&**err),
            Error::Io(ref err) => Some(err),
            Error::Conversion(ref err, _) => Some(&**err),
        }
    }
}

impl From<io::Error> for Error {
    fn from(err: io::Error) -> Error {
        Error::Io(err)
    }
}

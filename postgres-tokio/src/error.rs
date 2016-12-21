use std::error;
use std::io;
use std::fmt;

use Connection;

#[doc(inline)]
pub use postgres_shared::error::*;

#[derive(Debug)]
pub enum Error {
    Io(io::Error),
    Db(Box<DbError>, Connection),
    Conversion(Box<error::Error + Sync + Send>, Connection),
}

impl fmt::Display for Error {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        try!(fmt.write_str(error::Error::description(self)));
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

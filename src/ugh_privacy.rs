use std::collections::HashMap;
use std::error;
use std::fmt;
use std::result;

use Result;
use types::{Oid, Kind};
use error::{SqlState, ErrorPosition, ConnectError, Error};

/// Information about an unknown type.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Other {
    name: String,
    oid: Oid,
    kind: Kind,
}

pub fn new_other(name: String, oid: Oid, kind: Kind) -> Other {
    Other {
        name: name,
        oid: oid,
        kind: kind,
    }
}

impl Other {
    /// The name of the type.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The OID of this type.
    pub fn oid(&self) -> Oid {
        self.oid
    }

    /// The kind of this type
    pub fn kind(&self) -> &Kind {
        &self.kind
    }
}

/// A Postgres error or notice.
#[derive(Clone, PartialEq, Eq, Debug)]
pub struct DbError {
    severity: String,
    code: SqlState,
    message: String,
    detail: Option<String>,
    hint: Option<String>,
    position: Option<ErrorPosition>,
    where_: Option<String>,
    schema: Option<String>,
    table: Option<String>,
    column: Option<String>,
    datatype: Option<String>,
    constraint: Option<String>,
    file: String,
    line: u32,
    routine: String
}

pub fn dberror_new_raw(fields: Vec<(u8, String)>) -> result::Result<DbError, ()> {
    let mut map: HashMap<_, _> = fields.into_iter().collect();
    Ok(DbError {
        severity: try!(map.remove(&b'S').ok_or(())),
        code: SqlState::from_code(try!(map.remove(&b'C').ok_or(()))),
        message: try!(map.remove(&b'M').ok_or(())),
        detail: map.remove(&b'D'),
        hint: map.remove(&b'H'),
        position: match map.remove(&b'P') {
            Some(pos) => Some(ErrorPosition::Normal(try!(pos.parse().map_err(|_| ())))),
            None => match map.remove(&b'p') {
                Some(pos) => Some(ErrorPosition::Internal {
                    position: try!(pos.parse().map_err(|_| ())),
                    query: try!(map.remove(&b'q').ok_or(()))
                }),
                None => None
            }
        },
        where_: map.remove(&b'W'),
        schema: map.remove(&b's'),
        table: map.remove(&b't'),
        column: map.remove(&b'c'),
        datatype: map.remove(&b'd'),
        constraint: map.remove(&b'n'),
        file: try!(map.remove(&b'F').ok_or(())),
        line: try!(map.remove(&b'L').and_then(|l| l.parse().ok()).ok_or(())),
        routine: try!(map.remove(&b'R').ok_or(())),
    })
}

pub fn dberror_new_connect<T>(fields: Vec<(u8, String)>) -> result::Result<T, ConnectError> {
    match dberror_new_raw(fields) {
        Ok(err) => Err(ConnectError::DbError(err)),
        Err(()) => Err(ConnectError::BadResponse),
    }
}

pub fn dberror_new<T>(fields: Vec<(u8, String)>) -> Result<T> {
    match dberror_new_raw(fields) {
        Ok(err) => Err(Error::DbError(err)),
        Err(()) => Err(Error::BadResponse),
    }
}

impl DbError {
    /// The field contents are ERROR, FATAL, or PANIC (in an error message),
    /// or WARNING, NOTICE, DEBUG, INFO, or LOG (in a notice message), or a
    /// localized translation of one of these.
    pub fn severity(&self) -> &str {
        &self.severity
    }

    /// The SQLSTATE code for the error.
    pub fn code(&self) -> &SqlState {
        &self.code
    }

    /// The primary human-readable error message. This should be accurate but
    /// terse (typically one line).
    pub fn message(&self) -> &str {
        &self.message
    }

    /// An optional secondary error message carrying more detail about the
    /// problem. Might run to multiple lines.
    pub fn detail(&self) -> Option<&str> {
        self.detail.as_ref().map(|s| &**s)
    }

    /// An optional suggestion what to do about the problem. This is intended
    /// to differ from Detail in that it offers advice (potentially
    /// inappropriate) rather than hard facts. Might run to multiple lines.
    pub fn hint(&self) -> Option<&str> {
        self.hint.as_ref().map(|s| &**s)
    }

    /// An optional error cursor position into either the original query string
    /// or an internally generated query.
    pub fn position(&self) -> Option<&ErrorPosition> {
        self.position.as_ref()
    }

    /// An indication of the context in which the error occurred. Presently
    /// this includes a call stack traceback of active procedural language
    /// functions and internally-generated queries. The trace is one entry per
    /// line, most recent first.
    pub fn where_(&self) -> Option<&str> {
        self.where_.as_ref().map(|s| &**s)
    }

    /// If the error was associated with a specific database object, the name
    /// of the schema containing that object, if any. (PostgreSQL 9.3+)
    pub fn schema(&self) -> Option<&str> {
        self.schema.as_ref().map(|s| &**s)
    }

    /// If the error was associated with a specific table, the name of the
    /// table. (Refer to the schema name field for the name of the table's
    /// schema.) (PostgreSQL 9.3+)
    pub fn table(&self) -> Option<&str> {
        self.table.as_ref().map(|s| &**s)
    }

    /// If the error was associated with a specific table column, the name of
    /// the column. (Refer to the schema and table name fields to identify the
    /// table.) (PostgreSQL 9.3+)
    pub fn column(&self) -> Option<&str> {
        self.column.as_ref().map(|s| &**s)
    }

    /// If the error was associated with a specific data type, the name of the
    /// data type. (Refer to the schema name field for the name of the data
    /// type's schema.) (PostgreSQL 9.3+)
    pub fn datatype(&self) -> Option<&str> {
        self.datatype.as_ref().map(|s| &**s)
    }

    /// If the error was associated with a specific constraint, the name of the
    /// constraint. Refer to fields listed above for the associated table or
    /// domain. (For this purpose, indexes are treated as constraints, even if
    /// they weren't created with constraint syntax.) (PostgreSQL 9.3+)
    pub fn constraint(&self) -> Option<&str> {
        self.constraint.as_ref().map(|s| &**s)
    }

    /// The file name of the source-code location where the error was reported.
    pub fn file(&self) -> &str {
        &self.file
    }

    /// The line number of the source-code location where the error was
    /// reported.
    pub fn line(&self) -> u32 {
        self.line
    }

    /// The name of the source-code routine reporting the error.
    pub fn routine(&self) -> &str {
        &self.routine
    }
}

impl fmt::Display for DbError {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt, "{}: {}", self.severity, self.message)
    }
}

impl error::Error for DbError {
    fn description(&self) -> &str {
        &*self.message
    }
}

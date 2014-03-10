use collections::{Deque, RingBuf};
use std::cell::Cell;
use std::from_str::FromStr;
use std::task;

use PostgresConnection;
use error::{PgDbError,
            PgStreamDesynchronized,
            PgStreamError,
            PostgresDbError,
            PostgresError};
use message::{Bind,
              Close,
              CommandComplete,
              DataRow,
              ErrorResponse,
              Execute,
              ReadyForQuery,
              RowDescriptionEntry,
              Sync};
use types::{FromSql, ToSql, PostgresType};

/// A trait containing methods that can be called on a prepared statement.
pub trait PostgresStatement {
    /// Returns a slice containing the expected parameter types.
    fn param_types<'a>(&'a self) -> &'a [PostgresType];

    /// Returns a slice describing the columns of the result of the query.
    fn result_descriptions<'a>(&'a self) -> &'a [ResultDescription];

    /// Attempts to execute the prepared statement, returning the number of
    /// rows modified.
    ///
    /// If the statement does not modify any rows (e.g. SELECT), 0 is returned.
    ///
    /// # Failure
    ///
    /// Fails if the number or types of the provided parameters do not match
    /// the parameters of the statement.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{PostgresConnection, NoSsl, PostgresStatement};
    /// # use postgres::types::ToSql;
    /// # let conn = PostgresConnection::connect("", &NoSsl);
    /// # let bar = 1i32;
    /// # let baz = true;
    /// let stmt = conn.prepare("UPDATE foo SET bar = $1 WHERE baz = $2");
    /// match stmt.try_execute([&bar as &ToSql, &baz as &ToSql]) {
    ///     Ok(count) => println!("{} row(s) updated", count),
    ///     Err(err) => println!("Error executing query: {}", err)
    /// }
    fn try_execute(&self, params: &[&ToSql]) -> Result<uint, PostgresError>;

    /// A convenience function wrapping `try_execute`.
    ///
    /// # Failure
    ///
    /// Fails if there was an error executing the statement.
    fn execute(&self, params: &[&ToSql]) -> uint {
        match self.try_execute(params) {
            Ok(count) => count,
            Err(err) => fail!("Error running query\n{}", err)
        }
    }

    /// Attempts to execute the prepared statement, returning an iterator over
    /// the resulting rows.
    ///
    /// # Failure
    ///
    /// Fails if the number or types of the provided parameters do not match
    /// the parameters of the statement.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{PostgresConnection, NoSsl, PostgresStatement};
    /// # use postgres::types::ToSql;
    /// # let conn = PostgresConnection::connect("", &NoSsl);
    /// let stmt = conn.prepare("SELECT foo FROM bar WHERE baz = $1");
    /// # let baz = true;
    /// let mut rows = match stmt.try_query([&baz as &ToSql]) {
    ///     Ok(rows) => rows,
    ///     Err(err) => fail!("Error running query: {}", err)
    /// };
    /// for row in rows {
    ///     let foo: i32 = row["foo"];
    ///     println!("foo: {}", foo);
    /// }
    /// ```
    fn try_query<'a>(&'a self, params: &[&ToSql])
            -> Result<PostgresResult<'a>, PostgresError>;

    /// A convenience function wrapping `try_query`.
    ///
    /// # Failure
    ///
    /// Fails if there was an error executing the statement.
    fn query<'a>(&'a self, params: &[&ToSql]) -> PostgresResult<'a> {
        match self.try_query(params) {
            Ok(result) => result,
            Err(err) => fail!("Error executing query:\n{}", err)
        }
    }

    /// Consumes the statement, clearing it from the Postgres session.
    ///
    /// Functionally identical to the `Drop` implementation of the
    /// `PostgresStatement` except that it returns any error to the caller.
    fn finish(self) -> Result<(), PostgresError>;
}

/// A statement prepared outside of a transaction.
pub struct NormalPostgresStatement<'conn> {
    priv conn: &'conn PostgresConnection,
    priv name: ~str,
    priv param_types: ~[PostgresType],
    priv result_desc: ~[ResultDescription],
    priv next_portal_id: Cell<uint>,
    priv finished: Cell<bool>,
}

pub fn make_NormalPostgresStatement<'a>(conn: &'a PostgresConnection,
                                        name: ~str,
                                        param_types: ~[PostgresType],
                                        result_desc: ~[ResultDescription])
                                        -> NormalPostgresStatement<'a> {
    NormalPostgresStatement {
        conn: conn,
        name: name,
        param_types: param_types,
        result_desc: result_desc,
        next_portal_id: Cell::new(0),
        finished: Cell::new(false),
    }
}

#[unsafe_destructor]
impl<'conn> Drop for NormalPostgresStatement<'conn> {
    fn drop(&mut self) {
        if !self.finished.get() {
            match self.finish_inner() {
                Ok(()) | Err(PgStreamDesynchronized) => {}
                Err(err) =>
                    fail_unless_failing!("Error dropping statement: {}", err)
            }
        }
    }
}

impl<'conn> NormalPostgresStatement<'conn> {
    fn finish_inner(&mut self) -> Result<(), PostgresError> {
        check_desync!(self.conn);
        if_ok_pg!(self.conn.write_messages([
            Close {
                variant: 'S' as u8,
                name: self.name.as_slice()
            },
            Sync]));
        loop {
            match if_ok_pg!(self.conn.read_message()) {
                ReadyForQuery { .. } => break,
                ErrorResponse { fields } => {
                    try!(self.conn.wait_for_ready());
                    return Err(PgDbError(PostgresDbError::new(fields)));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn execute(&self, portal_name: &str, row_limit: uint, params: &[&ToSql])
            -> Result<(), PostgresError> {
        let mut formats = ~[];
        let mut values = ~[];
        assert!(self.param_types.len() == params.len(),
                "Expected {} parameters but found {}",
                self.param_types.len(), params.len());
        for (&param, ty) in params.iter().zip(self.param_types.iter()) {
            let (format, value) = param.to_sql(ty);
            formats.push(format as i16);
            values.push(value);
        };

        let result_formats: ~[i16] = self.result_desc.iter().map(|desc| {
            desc.ty.result_format() as i16
        }).collect();

        if_ok_pg!(self.conn.write_messages([
            Bind {
                portal: portal_name,
                statement: self.name.as_slice(),
                formats: formats,
                values: values,
                result_formats: result_formats
            },
            Execute {
                portal: portal_name,
                max_rows: row_limit as i32
            },
            Sync]));

        match if_ok_pg!(self.conn.read_message()) {
            BindComplete => Ok(()),
            ErrorResponse { fields } => {
                try!(self.conn.wait_for_ready());
                Err(PgDbError(PostgresDbError::new(fields)))
            }
            _ => unreachable!()
        }
    }

    fn try_lazy_query<'a>(&'a self, row_limit: uint, params: &[&ToSql])
            -> Result<PostgresResult<'a>, PostgresError> {
        let id = self.next_portal_id.get();
        self.next_portal_id.set(id + 1);
        let portal_name = format!("{}_portal_{}", self.name, id);

        try!(self.execute(portal_name, row_limit, params));

        let mut result = PostgresResult {
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
}

impl<'conn> PostgresStatement for NormalPostgresStatement<'conn> {
    fn param_types<'a>(&'a self) -> &'a [PostgresType] {
        self.param_types.as_slice()
    }

    fn result_descriptions<'a>(&'a self) -> &'a [ResultDescription] {
        self.result_desc.as_slice()
    }

    fn try_execute(&self, params: &[&ToSql])
                      -> Result<uint, PostgresError> {
        check_desync!(self.conn);
        try!(self.execute("", 0, params));

        let num;
        loop {
            match if_ok_pg!(self.conn.read_message()) {
                DataRow { .. } => {}
                ErrorResponse { fields } => {
                    try!(self.conn.wait_for_ready());
                    return Err(PgDbError(PostgresDbError::new(fields)));
                }
                CommandComplete { tag } => {
                    let s = tag.split(' ').last().unwrap();
                    num = match FromStr::from_str(s) {
                        None => 0,
                        Some(n) => n
                    };
                    break;
                }
                EmptyQueryResponse => {
                    num = 0;
                    break;
                }
                _ => unreachable!()
            }
        }
        try!(self.conn.wait_for_ready());

        Ok(num)
    }

    fn try_query<'a>(&'a self, params: &[&ToSql])
            -> Result<PostgresResult<'a>, PostgresError> {
        check_desync!(self.conn);
        self.try_lazy_query(0, params)
    }

    fn finish(mut self) -> Result<(), PostgresError> {
        self.finished.set(true);
        self.finish_inner()
    }
}

/// Information about a column of the result of a query.
#[deriving(Eq)]
pub struct ResultDescription {
    /// The name of the column
    name: ~str,
    /// The type of the data in the column
    ty: PostgresType
}

pub fn make_ResultDescription(row: RowDescriptionEntry) -> ResultDescription {
    let RowDescriptionEntry { name, type_oid, .. } = row;

    ResultDescription {
        name: name,
        ty: PostgresType::from_oid(type_oid)
    }
}

/// A statement prepared inside of a transaction.
///
/// Provides additional functionality over a `NormalPostgresStatement`.
pub struct TransactionalPostgresStatement<'conn> {
    priv stmt: NormalPostgresStatement<'conn>
}

pub fn make_TransactionalPostgresStatement<'a>(stmt: NormalPostgresStatement<'a>)
                                               -> TransactionalPostgresStatement<'a> {
    TransactionalPostgresStatement {
        stmt: stmt,
    }
}

impl<'conn> PostgresStatement for TransactionalPostgresStatement<'conn> {
    fn param_types<'a>(&'a self) -> &'a [PostgresType] {
        self.stmt.param_types()
    }

    fn result_descriptions<'a>(&'a self) -> &'a [ResultDescription] {
        self.stmt.result_descriptions()
    }

    fn try_execute(&self, params: &[&ToSql]) -> Result<uint, PostgresError> {
        self.stmt.try_execute(params)
    }

    fn try_query<'a>(&'a self, params: &[&ToSql])
            -> Result<PostgresResult<'a>, PostgresError> {
        self.stmt.try_query(params)
    }

    fn finish(self) -> Result<(), PostgresError> {
        self.stmt.finish()
    }
}

impl<'conn> TransactionalPostgresStatement<'conn> {
    /// Attempts to execute the prepared statement, returning a lazily loaded
    /// iterator over the resulting rows.
    ///
    /// No more than `row_limit` rows will be stored in memory at a time. Rows
    /// will be pulled from the database in batches of `row_limit` as needed.
    /// If `row_limit` is 0, `try_lazy_query` is equivalent to `try_query`.
    ///
    /// # Failure
    ///
    /// Fails if the number or types of the provided parameters do not match
    /// the parameters of the statement.
    pub fn try_lazy_query<'a>(&'a self, row_limit: uint, params: &[&ToSql])
            -> Result<PostgresResult<'a>, PostgresError> {
        check_desync!(self.stmt.conn);
        self.stmt.try_lazy_query(row_limit, params)
    }

    /// A convenience wrapper around `try_lazy_query`.
    ///
    /// # Failure
    ///
    /// Fails if there was an error executing the statement.
    pub fn lazy_query<'a>(&'a self, row_limit: uint, params: &[&ToSql])
            -> PostgresResult<'a> {
        match self.try_lazy_query(row_limit, params) {
            Ok(result) => result,
            Err(err) => fail!("Error executing query:\n{}", err)
        }
    }
}

/// An iterator over the resulting rows of a query.
pub struct PostgresResult<'stmt> {
    priv stmt: &'stmt NormalPostgresStatement<'stmt>,
    priv name: ~str,
    priv data: RingBuf<~[Option<~[u8]>]>,
    priv row_limit: uint,
    priv more_rows: bool,
    priv finished: bool,
}

#[unsafe_destructor]
impl<'stmt> Drop for PostgresResult<'stmt> {
    fn drop(&mut self) {
        if !self.finished {
            match self.finish_inner() {
                Ok(()) | Err(PgStreamDesynchronized) => {}
                Err(err) =>
                    fail_unless_failing!("Error dropping result: {}", err)
            }
        }
    }
}

impl<'stmt> PostgresResult<'stmt> {
    fn finish_inner(&mut self) -> Result<(), PostgresError> {
        check_desync!(self.stmt.conn);
        if_ok_pg!(self.stmt.conn.write_messages([
            Close {
                variant: 'P' as u8,
                name: self.name.as_slice()
            },
            Sync]));
        loop {
            match if_ok_pg!(self.stmt.conn.read_message()) {
                ReadyForQuery { .. } => break,
                ErrorResponse { fields } => {
                    try!(self.stmt.conn.wait_for_ready());
                    return Err(PgDbError(PostgresDbError::new(fields)));
                }
                _ => {}
            }
        }
        Ok(())
    }

    fn read_rows(&mut self) -> Result<(), PostgresError> {
        loop {
            match if_ok_pg!(self.stmt.conn.read_message()) {
                EmptyQueryResponse |
                CommandComplete { .. } => {
                    self.more_rows = false;
                    break;
                },
                PortalSuspended => {
                    self.more_rows = true;
                    break;
                },
                DataRow { row } => self.data.push_back(row),
                _ => unreachable!()
            }
        }
        self.stmt.conn.wait_for_ready()
    }

    fn execute(&mut self) -> Result<(), PostgresError> {
        if_ok_pg!(self.stmt.conn.write_messages([
            Execute {
                portal: self.name,
                max_rows: self.row_limit as i32
            },
            Sync]));
        self.read_rows()
    }
}

impl<'stmt> PostgresResult<'stmt> {
    /// Consumes the `PostgresResult`, cleaning up associated state.
    ///
    /// Functionally identical to the `Drop` implementation on
    /// `PostgresResult` except that it returns any error to the caller.
    pub fn finish(mut self) -> Result<(), PostgresError> {
        self.finished = true;
        self.finish_inner()
    }

    /// Like `PostgresResult::next` except that it returns any errors to the
    /// caller instead of failing.
    pub fn try_next(&mut self) -> Result<Option<PostgresRow<'stmt>>,
                                         PostgresError> {
        if self.data.is_empty() && self.more_rows {
            try!(self.execute());
        }

        let row = self.data.pop_front().map(|row| {
            PostgresRow {
                stmt: self.stmt,
                data: row
            }
        });
        Ok(row)
    }
}

impl<'stmt> Iterator<PostgresRow<'stmt>> for PostgresResult<'stmt> {
    fn next(&mut self) -> Option<PostgresRow<'stmt>> {
        match self.try_next() {
            Ok(ok) => ok,
            Err(err) => fail!("Error fetching rows: {}", err)
        }
    }

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
///
/// A value can be accessed by the name or index of its column, though access
/// by index is more efficient. Rows are 1-indexed.
///
/// ```rust,no_run
/// # use postgres::{PostgresConnection, PostgresStatement, NoSsl};
/// # let conn = PostgresConnection::connect("", &NoSsl);
/// # let stmt = conn.prepare("");
/// # let mut result = stmt.query([]);
/// # let row = result.next().unwrap();
/// let foo: i32 = row[1];
/// let bar: ~str = row["bar"];
/// ```
pub struct PostgresRow<'stmt> {
    priv stmt: &'stmt NormalPostgresStatement<'stmt>,
    priv data: ~[Option<~[u8]>]
}

impl<'stmt> Container for PostgresRow<'stmt> {
    #[inline]
    fn len(&self) -> uint {
        self.data.len()
    }
}

impl<'stmt, I: RowIndex, T: FromSql> Index<I, T> for PostgresRow<'stmt> {
    fn index(&self, idx: &I) -> T {
        let idx = idx.idx(self.stmt);
        FromSql::from_sql(&self.stmt.result_desc[idx].ty, &self.data[idx])
    }
}

/// A trait implemented by types that can index into columns of a row.
pub trait RowIndex {
    /// Returns the index of the appropriate column.
    ///
    /// # Failure
    ///
    /// Fails if there is no corresponding column.
    fn idx(&self, stmt: &NormalPostgresStatement) -> uint;
}

impl RowIndex for uint {
    #[inline]
    fn idx(&self, _stmt: &NormalPostgresStatement) -> uint {
        assert!(*self != 0, "out of bounds row access");
        *self - 1
    }
}

// This is a convenience as the 1 in get[1] resolves to int :(
impl RowIndex for int {
    #[inline]
    fn idx(&self, _stmt: &NormalPostgresStatement) -> uint {
        assert!(*self >= 1, "out of bounds row access");
        (*self - 1) as uint
    }
}

impl<'a> RowIndex for &'a str {
    fn idx(&self, stmt: &NormalPostgresStatement) -> uint {
        for (i, desc) in stmt.result_descriptions().iter().enumerate() {
            if desc.name.as_slice() == *self {
                return i;
            }
        }
        fail!("there is no column with name {}", *self);
    }
}


use debug_builders::DebugStruct;
use std::cell::Cell;
use std::collections::VecDeque;
use std::fmt;
use std::io;

use error::{Error, DbError};
use types::{ReadWithInfo, SessionInfo, Type, ToSql, IsNull};
use message::FrontendMessage::*;
use message::BackendMessage::*;
use message::WriteMessage;
use util;
use rows::{Rows, LazyRows};
use {read_rows, bad_response, Connection, Transaction, StatementInternals, Result, RowsNew};
use {SessionInfoNew, LazyRowsNew, DbErrorNew, ColumnNew};

/// A prepared statement.
pub struct Statement<'conn> {
    conn: &'conn Connection,
    name: String,
    param_types: Vec<Type>,
    columns: Vec<Column>,
    next_portal_id: Cell<u32>,
    finished: bool,
}

impl<'a> fmt::Debug for Statement<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        DebugStruct::new(fmt, "Statement")
            .field("name", &self.name)
            .field("parameter_types", &self.param_types)
            .field("columns", &self.columns)
            .finish()
    }
}

impl<'conn> Drop for Statement<'conn> {
    fn drop(&mut self) {
        let _ = self.finish_inner();
    }
}

impl<'conn> StatementInternals<'conn> for Statement<'conn> {
    fn new(conn: &'conn Connection,
           name: String,
           param_types: Vec<Type>,
           columns: Vec<Column>,
           next_portal_id: Cell<u32>,
           finished: bool) -> Statement<'conn> {
        Statement {
            conn: conn,
            name: name,
            param_types: param_types,
            columns: columns,
            next_portal_id: next_portal_id,
            finished: finished,
        }
    }

    fn conn(&self) -> &'conn Connection {
        self.conn
    }
}

impl<'conn> Statement<'conn> {
    fn finish_inner(&mut self) -> Result<()> {
        if !self.finished {
            self.finished = true;
            let mut conn = self.conn.conn.borrow_mut();
            check_desync!(conn);
            conn.close_statement(&self.name, b'S')
        } else {
            Ok(())
        }
    }

    fn inner_execute(&self, portal_name: &str, row_limit: i32, params: &[&ToSql]) -> Result<()> {
        let mut conn = self.conn.conn.borrow_mut();
        assert!(self.param_types().len() == params.len(),
                "expected {} parameters but got {}",
                self.param_types.len(),
                params.len());
        debug!("executing statement {} with parameters: {:?}", self.name, params);
        let mut values = vec![];
        for (param, ty) in params.iter().zip(self.param_types.iter()) {
            let mut buf = vec![];
            match try!(param.to_sql_checked(ty, &mut buf, &SessionInfo::new(&*conn))) {
                IsNull::Yes => values.push(None),
                IsNull::No => values.push(Some(buf)),
            }
        };

        try!(conn.write_messages(&[
            Bind {
                portal: portal_name,
                statement: &self.name,
                formats: &[1],
                values: &values,
                result_formats: &[1]
            },
            Execute {
                portal: portal_name,
                max_rows: row_limit
            },
            Sync]));

        match try!(conn.read_message()) {
            BindComplete => Ok(()),
            ErrorResponse { fields } => {
                try!(conn.wait_for_ready());
                DbError::new(fields)
            }
            _ => {
                conn.desynchronized = true;
                Err(Error::IoError(bad_response()))
            }
        }
    }

    fn inner_query<'a>(&'a self, portal_name: &str, row_limit: i32, params: &[&ToSql])
                       -> Result<(VecDeque<Vec<Option<Vec<u8>>>>, bool)> {
        try!(self.inner_execute(portal_name, row_limit, params));

        let mut buf = VecDeque::new();
        let more_rows = try!(read_rows(&mut self.conn.conn.borrow_mut(), &mut buf));
        Ok((buf, more_rows))
    }

    /// Returns a slice containing the expected parameter types.
    pub fn param_types(&self) -> &[Type] {
        &self.param_types
    }

    /// Returns a slice describing the columns of the result of the query.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Executes the prepared statement, returning the number of rows modified.
    ///
    /// If the statement does not modify any rows (e.g. SELECT), 0 is returned.
    ///
    /// ## Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// # let bar = 1i32;
    /// # let baz = true;
    /// let stmt = conn.prepare("UPDATE foo SET bar = $1 WHERE baz = $2").unwrap();
    /// match stmt.execute(&[&bar, &baz]) {
    ///     Ok(count) => println!("{} row(s) updated", count),
    ///     Err(err) => println!("Error executing query: {:?}", err)
    /// }
    /// ```
    pub fn execute(&self, params: &[&ToSql]) -> Result<u64> {
        check_desync!(self.conn);
        try!(self.inner_execute("", 0, params));

        let mut conn = self.conn.conn.borrow_mut();
        let num;
        loop {
            match try!(conn.read_message()) {
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
                    try!(conn.write_messages(&[
                        CopyFail {
                            message: "COPY queries cannot be directly executed",
                        },
                        Sync]));
                }
                CopyOutResponse { .. } => {
                    loop {
                        match try!(conn.read_message()) {
                            BCopyDone => break,
                            ErrorResponse { fields } => {
                                try!(conn.wait_for_ready());
                                return DbError::new(fields);
                            }
                            _ => {}
                        }
                    }
                    num = 0;
                    break;
                }
                _ => {
                    conn.desynchronized = true;
                    return Err(Error::IoError(bad_response()));
                }
            }
        }
        try!(conn.wait_for_ready());

        Ok(num)
    }

    /// Executes the prepared statement, returning the resulting rows.
    ///
    /// ## Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    ///
    /// ## Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// let stmt = conn.prepare("SELECT foo FROM bar WHERE baz = $1").unwrap();
    /// # let baz = true;
    /// let rows = match stmt.query(&[&baz]) {
    ///     Ok(rows) => rows,
    ///     Err(err) => panic!("Error running query: {:?}", err)
    /// };
    /// for row in &rows {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// ```
    pub fn query<'a>(&'a self, params: &[&ToSql]) -> Result<Rows<'a>> {
        check_desync!(self.conn);
        self.inner_query("", 0, params).map(|(buf, _)| {
            Rows::new(self, buf.into_iter().collect())
        })
    }

    /// Executes the prepared statement, returning a lazily loaded iterator
    /// over the resulting rows.
    ///
    /// No more than `row_limit` rows will be stored in memory at a time. Rows
    /// will be pulled from the database in batches of `row_limit` as needed.
    /// If `row_limit` is less than or equal to 0, `lazy_query` is equivalent
    /// to `query`.
    ///
    /// This can only be called inside of a transaction, and the `Transaction`
    /// object representing the active transaction must be passed to
    /// `lazy_query`.
    ///
    /// ## Panics
    ///
    /// Panics if the provided `Transaction` is not associated with the same
    /// `Connection` as this `Statement`, if the `Transaction` is not
    /// active, or if the number of parameters provided does not match the
    /// number of parameters expected.
    pub fn lazy_query<'trans, 'stmt>(&'stmt self,
                                     trans: &'trans Transaction,
                                     params: &[&ToSql],
                                     row_limit: i32)
                                     -> Result<LazyRows<'trans, 'stmt>> {
        assert!(self.conn as *const _ == trans.conn as *const _,
                "the `Transaction` passed to `lazy_query` must be associated with the same \
                 `Connection` as the `Statement`");
        let conn = self.conn.conn.borrow();
        check_desync!(conn);
        assert!(conn.trans_depth == trans.depth,
                "`lazy_query` must be passed the active transaction");
        drop(conn);

        let id = self.next_portal_id.get();
        self.next_portal_id.set(id + 1);
        let portal_name = format!("{}p{}", self.name, id);

        self.inner_query(&portal_name, row_limit, params).map(move |(data, more_rows)| {
            LazyRows::new(self, data, portal_name, row_limit, more_rows, false, trans)
        })
    }

    /// Executes a `COPY FROM STDIN` statement, returning the number of rows
    /// added.
    ///
    /// The contents of the provided reader are passed to the Postgres server
    /// verbatim; it is the caller's responsibility to ensure it uses the
    /// proper format. See the
    /// [Postgres documentation](http://www.postgresql.org/docs/9.4/static/sql-copy.html)
    /// for details.
    ///
    /// If the statement is not a `COPY FROM STDIN` statement it will still be
    /// executed and this method will return an error.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// conn.batch_execute("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR)").unwrap();
    /// let stmt = conn.prepare("COPY people FROM STDIN").unwrap();
    /// stmt.copy_in(&[], &mut "1\tjohn\n2\tjane\n".as_bytes()).unwrap();
    /// ```
    pub fn copy_in<R: ReadWithInfo>(&self, params: &[&ToSql], r: &mut R) -> Result<u64> {
        try!(self.inner_execute("", 0, params));
        let mut conn = self.conn.conn.borrow_mut();

        match try!(conn.read_message()) {
            CopyInResponse { .. } => {}
            _ => {
                loop {
                    match try!(conn.read_message()) {
                        ReadyForQuery { .. } => {
                            return Err(Error::IoError(io::Error::new(
                                        io::ErrorKind::InvalidInput,
                                        "called `copy_in` on a non-`COPY FROM STDIN` statement")));
                        }
                        _ => {}
                    }
                }
            }
        }

        let mut buf = [0; 16 * 1024];
        loop {
            match fill_copy_buf(&mut buf, r, &SessionInfo::new(&conn)) {
                Ok(0) => break,
                Ok(len) => {
                    try_desync!(conn, conn.stream.write_message(
                        &CopyData {
                            data: &buf[..len],
                        }));
                }
                Err(err) => {
                    try!(conn.write_messages(&[
                        CopyFail {
                            message: "",
                        },
                        CopyDone,
                        Sync]));
                    match try!(conn.read_message()) {
                        ErrorResponse { .. } => { /* expected from the CopyFail */ }
                        _ => {
                            conn.desynchronized = true;
                            return Err(Error::IoError(bad_response()));
                        }
                    }
                    try!(conn.wait_for_ready());
                    return Err(Error::IoError(err));
                }
            }
        }

        try!(conn.write_messages(&[CopyDone, Sync]));

        let num = match try!(conn.read_message()) {
            CommandComplete { tag } => util::parse_update_count(tag),
            ErrorResponse { fields } => {
                try!(conn.wait_for_ready());
                return DbError::new(fields);
            }
            _ => {
                conn.desynchronized = true;
                return Err(Error::IoError(bad_response()));
            }
        };

        try!(conn.wait_for_ready());
        Ok(num)
    }

    /// Consumes the statement, clearing it from the Postgres session.
    ///
    /// If this statement was created via the `prepare_cached` method, `finish`
    /// does nothing.
    ///
    /// Functionally identical to the `Drop` implementation of the
    /// `Statement` except that it returns any error to the caller.
    pub fn finish(mut self) -> Result<()> {
        self.finish_inner()
    }
}

fn fill_copy_buf<R: ReadWithInfo>(buf: &mut [u8], r: &mut R, info: &SessionInfo)
                                  -> io::Result<usize> {
    let mut nread = 0;
    while nread < buf.len() {
        match r.read_with_info(&mut buf[nread..], info) {
            Ok(0) => break,
            Ok(n) => nread += n,
            Err(ref e) if e.kind() == io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e),
        }
    }
    Ok(nread)
}

/// Information about a column of the result of a query.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Column {
    name: String,
    type_: Type
}

impl ColumnNew for Column {
    fn new(name: String, type_: Type) -> Column {
        Column {
            name: name,
            type_: type_,
        }
    }
}

impl Column {
    /// The name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The type of the data in the column.
    pub fn type_(&self) -> &Type {
        &self.type_
    }
}



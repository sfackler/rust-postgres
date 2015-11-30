//! Prepared statements

use std::cell::{Cell, RefMut};
use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Read, Write};

use error::{Error, DbError};
use types::{SessionInfo, Type, ToSql, IsNull};
use message::FrontendMessage::*;
use message::BackendMessage::*;
use message::WriteMessage;
use util;
use rows::{Rows, LazyRows};
use {read_rows, bad_response, Connection, Transaction, StatementInternals, Result, RowsNew};
use {InnerConnection, SessionInfoNew, LazyRowsNew, DbErrorNew, ColumnNew};

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
        fmt.debug_struct("Statement")
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
           finished: bool)
           -> Statement<'conn> {
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
        debug!("executing statement {} with parameters: {:?}",
               self.name,
               params);
        let mut values = vec![];
        for (param, ty) in params.iter().zip(self.param_types.iter()) {
            let mut buf = vec![];
            match try!(param.to_sql_checked(ty, &mut buf, &SessionInfo::new(&*conn))) {
                IsNull::Yes => values.push(None),
                IsNull::No => values.push(Some(buf)),
            }
        }

        try!(conn.write_messages(&[Bind {
                                       portal: portal_name,
                                       statement: &self.name,
                                       formats: &[1],
                                       values: &values,
                                       result_formats: &[1],
                                   },
                                   Execute {
                                       portal: portal_name,
                                       max_rows: row_limit,
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

    fn inner_query<'a>(&'a self,
                       portal_name: &str,
                       row_limit: i32,
                       params: &[&ToSql])
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
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// # let bar = 1i32;
    /// # let baz = true;
    /// let stmt = conn.prepare("UPDATE foo SET bar = $1 WHERE baz = $2").unwrap();
    /// let rows_updated = stmt.execute(&[&bar, &baz]).unwrap();
    /// println!("{} rows updated", rows_updated);
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
                    try!(conn.write_messages(&[CopyFail {
                                                   message: "COPY queries cannot be directly \
                                                             executed",
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
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number
    /// expected.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// let stmt = conn.prepare("SELECT foo FROM bar WHERE baz = $1").unwrap();
    /// # let baz = true;
    /// for row in stmt.query(&[&baz]).unwrap() {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// ```
    pub fn query<'a>(&'a self, params: &[&ToSql]) -> Result<Rows<'a>> {
        check_desync!(self.conn);
        self.inner_query("", 0, params).map(|(buf, _)| Rows::new(self, buf.into_iter().collect()))
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
    /// # Panics
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

        let (format, column_formats) = match try!(conn.read_message()) {
            CopyInResponse { format, column_formats } => (format, column_formats),
            ErrorResponse { fields } => {
                try!(conn.wait_for_ready());
                return DbError::new(fields);
            }
            _ => {
                loop {
                    match try!(conn.read_message()) {
                        ReadyForQuery { .. } => {
                            return Err(Error::IoError(io::Error::new(io::ErrorKind::InvalidInput,
                                                                     "called `copy_in` on a \
                                                                      non-`COPY FROM STDIN` \
                                                                      statement")));
                        }
                        _ => {}
                    }
                }
            }
        };

        let mut info = CopyInfo {
            conn: conn,
            format: Format::from_u16(format as u16),
            column_formats: column_formats.iter().map(|&f| Format::from_u16(f)).collect(),
        };

        let mut buf = [0; 16 * 1024];
        loop {
            match fill_copy_buf(&mut buf, r, &info) {
                Ok(0) => break,
                Ok(len) => {
                    try_desync!(info.conn,
                                info.conn.stream.write_message(&CopyData { data: &buf[..len] }));
                }
                Err(err) => {
                    try!(info.conn.write_messages(&[CopyFail { message: "" }, CopyDone, Sync]));
                    match try!(info.conn.read_message()) {
                        ErrorResponse { .. } => {
                            // expected from the CopyFail
                        }
                        _ => {
                            info.conn.desynchronized = true;
                            return Err(Error::IoError(bad_response()));
                        }
                    }
                    try!(info.conn.wait_for_ready());
                    return Err(Error::IoError(err));
                }
            }
        }

        try!(info.conn.write_messages(&[CopyDone, Sync]));

        let num = match try!(info.conn.read_message()) {
            CommandComplete { tag } => util::parse_update_count(tag),
            ErrorResponse { fields } => {
                try!(info.conn.wait_for_ready());
                return DbError::new(fields);
            }
            _ => {
                info.conn.desynchronized = true;
                return Err(Error::IoError(bad_response()));
            }
        };

        try!(info.conn.wait_for_ready());
        Ok(num)
    }

    /// Executes a `COPY TO STDOUT` statement, passing the resulting data to
    /// the provided writer and returning the number of rows received.
    ///
    /// See the [Postgres documentation](http://www.postgresql.org/docs/9.4/static/sql-copy.html)
    /// for details on the data format.
    ///
    /// If the statement is not a `COPY TO STDOUT` statement it will still be
    /// executed and this method will return an error.
    ///
    /// # Examples
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", &SslMode::None).unwrap();
    /// conn.batch_execute("
    ///         CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR);
    ///         INSERT INTO people (id, name) VALUES (1, 'john'), (2, 'jane');").unwrap();
    /// let stmt = conn.prepare("COPY people TO STDOUT").unwrap();
    /// let mut buf = vec![];
    /// let mut r = stmt.copy_out(&[], &mut buf).unwrap();
    /// assert_eq!(buf, b"1\tjohn\n2\tjane\n");
    /// ```
    pub fn copy_out<'a, W: WriteWithInfo>(&'a self, params: &[&ToSql], w: &mut W) -> Result<u64> {
        try!(self.inner_execute("", 0, params));
        let mut conn = self.conn.conn.borrow_mut();

        let (format, column_formats) = match try!(conn.read_message()) {
            CopyOutResponse { format, column_formats } => (format, column_formats),
            CopyInResponse { .. } => {
                try!(conn.write_messages(&[CopyFail { message: "" }, CopyDone, Sync]));
                match try!(conn.read_message()) {
                    ErrorResponse { .. } => {
                        // expected from the CopyFail
                    }
                    _ => {
                        conn.desynchronized = true;
                        return Err(Error::IoError(bad_response()));
                    }
                }
                try!(conn.wait_for_ready());
                return Err(Error::IoError(io::Error::new(io::ErrorKind::InvalidInput,
                                                         "called `copy_out` on a non-`COPY TO \
                                                          STDOUT` statement")));
            }
            ErrorResponse { fields } => {
                try!(conn.wait_for_ready());
                return DbError::new(fields);
            }
            _ => {
                loop {
                    match try!(conn.read_message()) {
                        ReadyForQuery { .. } => {
                            return Err(Error::IoError(io::Error::new(io::ErrorKind::InvalidInput,
                                                                     "called `copy_out` on a \
                                                                      non-`COPY TO STDOUT` \
                                                                      statement")));
                        }
                        _ => {}
                    }
                }
            }
        };

        let mut info = CopyInfo {
            conn: conn,
            format: Format::from_u16(format as u16),
            column_formats: column_formats.iter().map(|&f| Format::from_u16(f)).collect(),
        };

        let count;
        loop {
            match try!(info.conn.read_message()) {
                BCopyData { data } => {
                    let mut data = &data[..];
                    while !data.is_empty() {
                        match w.write_with_info(data, &info) {
                            Ok(n) => data = &data[n..],
                            Err(e) => {
                                loop {
                                    match try!(info.conn.read_message()) {
                                        ReadyForQuery { .. } => return Err(Error::IoError(e)),
                                        _ => {}
                                    }
                                }
                            }
                        }
                    }
                }
                BCopyDone => {}
                CommandComplete { tag } => {
                    count = util::parse_update_count(tag);
                    break;
                }
                ErrorResponse { fields } => {
                    loop {
                        match try!(info.conn.read_message()) {
                            ReadyForQuery { .. } => return DbError::new(fields),
                            _ => {}
                        }
                    }
                }
                _ => {
                    loop {
                        match try!(info.conn.read_message()) {
                            ReadyForQuery { .. } => return Err(Error::IoError(bad_response())),
                            _ => {}
                        }
                    }
                }
            }
        }

        try!(info.conn.wait_for_ready());
        Ok(count)
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

fn fill_copy_buf<R: ReadWithInfo>(buf: &mut [u8], r: &mut R, info: &CopyInfo) -> io::Result<usize> {
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
    type_: Type,
}

impl ColumnNew for Column {
    fn new(name: String, type_: Type) -> Column {
        Column {
            name: name,
            type_: type_,
        }
    }
}

/// A struct containing information relevant for a `COPY` operation.
pub struct CopyInfo<'a> {
    conn: RefMut<'a, InnerConnection>,
    format: Format,
    column_formats: Vec<Format>,
}

impl<'a> CopyInfo<'a> {
    /// Returns the format of the overall data.
    pub fn format(&self) -> Format {
        self.format
    }

    /// Returns the format of the individual columns.
    pub fn column_formats(&self) -> &[Format] {
        &self.column_formats
    }

    /// Returns session info for the associated connection.
    pub fn session_info<'b>(&'b self) -> SessionInfo<'b> {
        SessionInfo::new(&*self.conn)
    }
}

/// Like `Read` except that a `CopyInfo` object is provided as well.
///
/// All types that implement `Read` also implement this trait.
pub trait ReadWithInfo {
    /// Like `Read::read`.
    fn read_with_info(&mut self, buf: &mut [u8], info: &CopyInfo) -> io::Result<usize>;
}

impl<R: Read> ReadWithInfo for R {
    fn read_with_info(&mut self, buf: &mut [u8], _: &CopyInfo) -> io::Result<usize> {
        self.read(buf)
    }
}

/// Like `Write` except that a `CopyInfo` object is provided as well.
///
/// All types that implement `Write` also implement this trait.
pub trait WriteWithInfo {
    /// Like `Write::write`.
    fn write_with_info(&mut self, buf: &[u8], info: &CopyInfo) -> io::Result<usize>;
}

impl<W: Write> WriteWithInfo for W {
    fn write_with_info(&mut self, buf: &[u8], _: &CopyInfo) -> io::Result<usize> {
        self.write(buf)
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

/// The format of a portion of COPY query data.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
pub enum Format {
    /// A text based format.
    Text,
    /// A binary format.
    Binary,
}

impl Format {
    fn from_u16(value: u16) -> Format {
        match value {
            0 => Format::Text,
            _ => Format::Binary,
        }
    }
}

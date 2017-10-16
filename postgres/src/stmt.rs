//! Prepared statements

use fallible_iterator::FallibleIterator;
use std::cell::Cell;
use std::collections::VecDeque;
use std::fmt;
use std::io::{self, Read, Write};
use std::sync::Arc;
use postgres_protocol::message::{backend, frontend};
use postgres_shared::rows::RowData;

#[doc(inline)]
pub use postgres_shared::stmt::Column;

use types::{Type, ToSql};
use rows::{Rows, LazyRows};
use transaction::Transaction;
use {bad_response, err, Connection, Result, StatementInfo};

/// A prepared statement.
pub struct Statement<'conn> {
    conn: &'conn Connection,
    info: Arc<StatementInfo>,
    next_portal_id: Cell<u32>,
    finished: bool,
}

impl<'a> fmt::Debug for Statement<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt::Debug::fmt(&*self.info, fmt)
    }
}

impl<'conn> Drop for Statement<'conn> {
    fn drop(&mut self) {
        let _ = self.finish_inner();
    }
}

impl<'conn> Statement<'conn> {
    pub(crate) fn new(
        conn: &'conn Connection,
        info: Arc<StatementInfo>,
        next_portal_id: Cell<u32>,
        finished: bool,
    ) -> Statement<'conn> {
        Statement {
            conn: conn,
            info: info,
            next_portal_id: next_portal_id,
            finished: finished,
        }
    }

    pub(crate) fn info(&self) -> &Arc<StatementInfo> {
        &self.info
    }

    pub(crate) fn conn(&self) -> &'conn Connection {
        self.conn
    }

    pub(crate) fn into_query(self, params: &[&ToSql]) -> Result<Rows> {
        check_desync!(self.conn);
        let mut rows = vec![];
        self.inner_query("", 0, params, |row| rows.push(row))?;
        Ok(Rows::new(&self, rows))
    }

    fn finish_inner(&mut self) -> Result<()> {
        if self.finished {
            Ok(())
        } else {
            self.finished = true;
            let mut conn = self.conn.0.borrow_mut();
            check_desync!(conn);
            conn.close_statement(&self.info.name, b'S')
        }
    }

    #[allow(type_complexity)]
    fn inner_query<F>(
        &self,
        portal_name: &str,
        row_limit: i32,
        params: &[&ToSql],
        acceptor: F,
    ) -> Result<bool>
    where
        F: FnMut(RowData),
    {
        let mut conn = self.conn.0.borrow_mut();

        conn.raw_execute(
            &self.info.name,
            portal_name,
            row_limit,
            self.param_types(),
            params,
        )?;

        conn.read_rows(acceptor)
    }

    /// Returns a slice containing the expected parameter types.
    pub fn param_types(&self) -> &[Type] {
        &self.info.param_types
    }

    /// Returns a slice describing the columns of the result of the query.
    pub fn columns(&self) -> &[Column] {
        &self.info.columns
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
    /// # use postgres::{Connection, TlsMode};
    /// # let conn = Connection::connect("", TlsMode::None).unwrap();
    /// # let bar = 1i32;
    /// # let baz = true;
    /// let stmt = conn.prepare("UPDATE foo SET bar = $1 WHERE baz = $2").unwrap();
    /// let rows_updated = stmt.execute(&[&bar, &baz]).unwrap();
    /// println!("{} rows updated", rows_updated);
    /// ```
    pub fn execute(&self, params: &[&ToSql]) -> Result<u64> {
        let mut conn = self.conn.0.borrow_mut();
        check_desync!(conn);
        conn.raw_execute(
            &self.info.name,
            "",
            0,
            self.param_types(),
            params,
        )?;

        let num;
        loop {
            match conn.read_message()? {
                backend::Message::DataRow(_) => {}
                backend::Message::ErrorResponse(body) => {
                    conn.wait_for_ready()?;
                    return Err(err(&mut body.fields()));
                }
                backend::Message::CommandComplete(body) => {
                    num = parse_update_count(body.tag()?);
                    break;
                }
                backend::Message::EmptyQueryResponse => {
                    num = 0;
                    break;
                }
                backend::Message::CopyInResponse(_) => {
                    conn.stream.write_message(|buf| {
                        frontend::copy_fail("COPY queries cannot be directly executed", buf)
                    })?;
                    conn.stream.write_message(
                        |buf| Ok::<(), io::Error>(frontend::sync(buf)),
                    )?;
                    conn.stream.flush()?;
                }
                backend::Message::CopyOutResponse(_) => {
                    loop {
                        match conn.read_message()? {
                            backend::Message::CopyDone => break,
                            backend::Message::ErrorResponse(body) => {
                                conn.wait_for_ready()?;
                                return Err(err(&mut body.fields()));
                            }
                            _ => {}
                        }
                    }
                    num = 0;
                    break;
                }
                _ => {
                    conn.desynchronized = true;
                    return Err(bad_response().into());
                }
            }
        }
        conn.wait_for_ready()?;

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
    /// # use postgres::{Connection, TlsMode};
    /// # let conn = Connection::connect("", TlsMode::None).unwrap();
    /// let stmt = conn.prepare("SELECT foo FROM bar WHERE baz = $1").unwrap();
    /// # let baz = true;
    /// for row in &stmt.query(&[&baz]).unwrap() {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// ```
    pub fn query(&self, params: &[&ToSql]) -> Result<Rows> {
        check_desync!(self.conn);
        let mut rows = vec![];
        self.inner_query("", 0, params, |row| rows.push(row))?;
        Ok(Rows::new(self, rows))
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
    ///
    /// # Examples
    ///
    /// ```no_run
    /// extern crate fallible_iterator;
    /// extern crate postgres;
    ///
    /// use fallible_iterator::FallibleIterator;
    /// # use postgres::{Connection, TlsMode};
    ///
    /// # fn main() {
    /// # let conn = Connection::connect("", TlsMode::None).unwrap();
    /// let stmt = conn.prepare("SELECT foo FROM bar WHERE baz = $1").unwrap();
    /// let trans = conn.transaction().unwrap();
    /// # let baz = true;
    /// let mut rows = stmt.lazy_query(&trans, &[&baz], 100).unwrap();
    ///
    /// while let Some(row) = rows.next().unwrap() {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// # }
    /// ```
    pub fn lazy_query<'trans, 'stmt>(
        &'stmt self,
        trans: &'trans Transaction,
        params: &[&ToSql],
        row_limit: i32,
    ) -> Result<LazyRows<'trans, 'stmt>> {
        assert!(
            self.conn as *const _ == trans.conn() as *const _,
            "the `Transaction` passed to `lazy_query` must be associated with the same \
                 `Connection` as the `Statement`"
        );
        let conn = self.conn.0.borrow();
        check_desync!(conn);
        assert!(
            conn.trans_depth == trans.depth(),
            "`lazy_query` must be passed the active transaction"
        );
        drop(conn);

        let id = self.next_portal_id.get();
        self.next_portal_id.set(id + 1);
        let portal_name = format!("{}p{}", self.info.name, id);

        let mut rows = VecDeque::new();
        let more_rows = self.inner_query(
            &portal_name,
            row_limit,
            params,
            |row| rows.push_back(row),
        )?;
        Ok(LazyRows::new(
            self,
            rows,
            portal_name,
            row_limit,
            more_rows,
            false,
            trans,
        ))
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
    /// # use postgres::{Connection, TlsMode};
    /// # let conn = Connection::connect("", TlsMode::None).unwrap();
    /// conn.batch_execute("CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR)").unwrap();
    /// let stmt = conn.prepare("COPY people FROM STDIN").unwrap();
    /// stmt.copy_in(&[], &mut "1\tjohn\n2\tjane\n".as_bytes()).unwrap();
    /// ```
    pub fn copy_in<R: ReadWithInfo>(&self, params: &[&ToSql], r: &mut R) -> Result<u64> {
        let mut conn = self.conn.0.borrow_mut();
        conn.raw_execute(
            &self.info.name,
            "",
            0,
            self.param_types(),
            params,
        )?;

        let (format, column_formats) = match conn.read_message()? {
            backend::Message::CopyInResponse(body) => {
                let format = body.format();
                let column_formats = body.column_formats().map(|f| Format::from_u16(f)).collect()?;
                (format, column_formats)
            }
            backend::Message::ErrorResponse(body) => {
                conn.wait_for_ready()?;
                return Err(err(&mut body.fields()));
            }
            _ => {
                loop {
                    if let backend::Message::ReadyForQuery(_) = conn.read_message()? {
                        return Err(
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "called `copy_in` on a non-`COPY FROM STDIN` statement",
                            ).into(),
                        );
                    }
                }
            }
        };

        let info = CopyInfo {
            format: Format::from_u16(format as u16),
            column_formats: column_formats,
        };

        let mut buf = [0; 16 * 1024];
        loop {
            match fill_copy_buf(&mut buf, r, &info) {
                Ok(0) => break,
                Ok(len) => {
                    conn.stream.write_message(
                        |out| frontend::copy_data(&buf[..len], out),
                    )?;
                }
                Err(err) => {
                    conn.stream.write_message(
                        |buf| frontend::copy_fail("", buf),
                    )?;
                    conn.stream.write_message(|buf| {
                        Ok::<(), io::Error>(frontend::copy_done(buf))
                    })?;
                    conn.stream.write_message(
                        |buf| Ok::<(), io::Error>(frontend::sync(buf)),
                    )?;
                    conn.stream.flush()?;
                    match conn.read_message()? {
                        backend::Message::ErrorResponse(_) => {
                            // expected from the CopyFail
                        }
                        _ => {
                            conn.desynchronized = true;
                            return Err(bad_response().into());
                        }
                    }
                    conn.wait_for_ready()?;
                    return Err(err.into());
                }
            }
        }

        conn.stream.write_message(|buf| {
            Ok::<(), io::Error>(frontend::copy_done(buf))
        })?;
        conn.stream.write_message(
            |buf| Ok::<(), io::Error>(frontend::sync(buf)),
        )?;
        conn.stream.flush()?;

        let num = match conn.read_message()? {
            backend::Message::CommandComplete(body) => parse_update_count(body.tag()?),
            backend::Message::ErrorResponse(body) => {
                conn.wait_for_ready()?;
                return Err(err(&mut body.fields()));
            }
            _ => {
                conn.desynchronized = true;
                return Err(bad_response().into());
            }
        };

        conn.wait_for_ready()?;
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
    /// # use postgres::{Connection, TlsMode};
    /// # let conn = Connection::connect("", TlsMode::None).unwrap();
    /// conn.batch_execute("
    ///         CREATE TABLE people (id INT PRIMARY KEY, name VARCHAR);
    ///         INSERT INTO people (id, name) VALUES (1, 'john'), (2, 'jane');").unwrap();
    /// let stmt = conn.prepare("COPY people TO STDOUT").unwrap();
    /// let mut buf = vec![];
    /// stmt.copy_out(&[], &mut buf).unwrap();
    /// assert_eq!(buf, b"1\tjohn\n2\tjane\n");
    /// ```
    pub fn copy_out<'a, W: WriteWithInfo>(&'a self, params: &[&ToSql], w: &mut W) -> Result<u64> {
        let mut conn = self.conn.0.borrow_mut();
        conn.raw_execute(
            &self.info.name,
            "",
            0,
            self.param_types(),
            params,
        )?;

        let (format, column_formats) = match conn.read_message()? {
            backend::Message::CopyOutResponse(body) => {
                let format = body.format();
                let column_formats = body.column_formats().map(|f| Format::from_u16(f)).collect()?;
                (format, column_formats)
            }
            backend::Message::CopyInResponse(_) => {
                conn.stream.write_message(
                    |buf| frontend::copy_fail("", buf),
                )?;
                conn.stream.write_message(|buf| {
                    Ok::<(), io::Error>(frontend::copy_done(buf))
                })?;
                conn.stream.write_message(
                    |buf| Ok::<(), io::Error>(frontend::sync(buf)),
                )?;
                conn.stream.flush()?;
                match conn.read_message()? {
                    backend::Message::ErrorResponse(_) => {
                        // expected from the CopyFail
                    }
                    _ => {
                        conn.desynchronized = true;
                        return Err(bad_response().into());
                    }
                }
                conn.wait_for_ready()?;
                return Err(
                    io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "called `copy_out` on a non-`COPY TO STDOUT` statement",
                    ).into(),
                );
            }
            backend::Message::ErrorResponse(body) => {
                conn.wait_for_ready()?;
                return Err(err(&mut body.fields()));
            }
            _ => {
                loop {
                    if let backend::Message::ReadyForQuery(_) = conn.read_message()? {
                        return Err(
                            io::Error::new(
                                io::ErrorKind::InvalidInput,
                                "called `copy_out` on a  non-`COPY TO STDOUT` statement",
                            ).into(),
                        );
                    }
                }
            }
        };

        let info = CopyInfo {
            format: Format::from_u16(format as u16),
            column_formats: column_formats,
        };

        let count;
        loop {
            match conn.read_message()? {
                backend::Message::CopyData(body) => {
                    let mut data = body.data();
                    while !data.is_empty() {
                        match w.write_with_info(data, &info) {
                            Ok(n) => data = &data[n..],
                            Err(e) => {
                                loop {
                                    if let backend::Message::ReadyForQuery(_) =
                                        conn.read_message()?
                                    {
                                        return Err(e.into());
                                    }
                                }
                            }
                        }
                    }
                }
                backend::Message::CopyDone => {}
                backend::Message::CommandComplete(body) => {
                    count = parse_update_count(body.tag()?);
                    break;
                }
                backend::Message::ErrorResponse(body) => {
                    loop {
                        if let backend::Message::ReadyForQuery(_) = conn.read_message()? {
                            return Err(err(&mut body.fields()));
                        }
                    }
                }
                _ => {
                    loop {
                        if let backend::Message::ReadyForQuery(_) = conn.read_message()? {
                            return Err(bad_response().into());
                        }
                    }
                }
            }
        }

        conn.wait_for_ready()?;
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

/// A struct containing information relevant for a `COPY` operation.
pub struct CopyInfo {
    format: Format,
    column_formats: Vec<Format>,
}

impl CopyInfo {
    /// Returns the format of the overall data.
    pub fn format(&self) -> Format {
        self.format
    }

    /// Returns the format of the individual columns.
    pub fn column_formats(&self) -> &[Format] {
        &self.column_formats
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

fn parse_update_count(tag: &str) -> u64 {
    tag.split(' ').last().unwrap().parse().unwrap_or(0)
}

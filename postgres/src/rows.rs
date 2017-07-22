//! Query result rows.

use fallible_iterator::FallibleIterator;
use postgres_protocol::message::frontend;
use postgres_shared::rows::RowData;
use std::collections::VecDeque;
use std::fmt;
use std::io;
use std::ops::Deref;
use std::slice;
use std::sync::Arc;

#[doc(inline)]
pub use postgres_shared::rows::RowIndex;

use {Error, Result, StatementInfo};
use error;
use transaction::Transaction;
use types::{FromSql, WrongType};
use stmt::{Statement, Column};

enum MaybeOwned<'a, T: 'a> {
    Borrowed(&'a T),
    Owned(T),
}

impl<'a, T> Deref for MaybeOwned<'a, T> {
    type Target = T;

    fn deref(&self) -> &T {
        match *self {
            MaybeOwned::Borrowed(s) => s,
            MaybeOwned::Owned(ref s) => s,
        }
    }
}

/// The resulting rows of a query.
pub struct Rows {
    stmt_info: Arc<StatementInfo>,
    data: Vec<RowData>,
}

impl fmt::Debug for Rows {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Rows")
            .field("columns", &self.columns())
            .field("rows", &self.data.len())
            .finish()
    }
}

impl Rows {
    pub(crate) fn new(stmt: &Statement, data: Vec<RowData>) -> Rows {
        Rows {
            stmt_info: stmt.info().clone(),
            data: data,
        }
    }

    /// Returns a slice describing the columns of the `Rows`.
    pub fn columns(&self) -> &[Column] {
        &self.stmt_info.columns[..]
    }

    /// Returns the number of rows present.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Determines if there are any rows present.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a specific `Row`.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of bounds.
    pub fn get<'a>(&'a self, idx: usize) -> Row<'a> {
        Row {
            stmt_info: &self.stmt_info,
            data: MaybeOwned::Borrowed(&self.data[idx]),
        }
    }

    /// Returns an iterator over the `Row`s.
    pub fn iter<'a>(&'a self) -> Iter<'a> {
        Iter {
            stmt_info: &self.stmt_info,
            iter: self.data.iter(),
        }
    }
}

impl<'a> IntoIterator for &'a Rows {
    type Item = Row<'a>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

/// An iterator over `Row`s.
pub struct Iter<'a> {
    stmt_info: &'a StatementInfo,
    iter: slice::Iter<'a, RowData>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Row<'a>> {
        self.iter.next().map(|row| {
            Row {
                stmt_info: self.stmt_info,
                data: MaybeOwned::Borrowed(row),
            }
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    fn next_back(&mut self) -> Option<Row<'a>> {
        self.iter.next_back().map(|row| {
            Row {
                stmt_info: self.stmt_info,
                data: MaybeOwned::Borrowed(row),
            }
        })
    }
}

impl<'a> ExactSizeIterator for Iter<'a> {}

/// A single result row of a query.
pub struct Row<'a> {
    stmt_info: &'a StatementInfo,
    data: MaybeOwned<'a, RowData>,
}

impl<'a> fmt::Debug for Row<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Row")
            .field("statement", self.stmt_info)
            .finish()
    }
}

impl<'a> Row<'a> {
    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Determines if there are any values in the row.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a slice describing the columns of the `Row`.
    pub fn columns(&self) -> &[Column] {
        &self.stmt_info.columns[..]
    }

    /// Retrieves the contents of a field of the row.
    ///
    /// A field can be accessed by the name or index of its column, though
    /// access by index is more efficient. Rows are 0-indexed.
    ///
    /// # Panics
    ///
    /// Panics if the index does not reference a column or the return type is
    /// not compatible with the Postgres type.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use postgres::{Connection, TlsMode};
    /// # let conn = Connection::connect("", TlsMode::None).unwrap();
    /// let stmt = conn.prepare("SELECT foo, bar from BAZ").unwrap();
    /// for row in &stmt.query(&[]).unwrap() {
    ///     let foo: i32 = row.get(0);
    ///     let bar: String = row.get("bar");
    ///     println!("{}: {}", foo, bar);
    /// }
    /// ```
    pub fn get<I, T>(&self, idx: I) -> T
    where
        I: RowIndex + fmt::Debug,
        T: FromSql,
    {
        match self.get_inner(&idx) {
            Some(Ok(ok)) => ok,
            Some(Err(err)) => panic!("error retrieving column {:?}: {:?}", idx, err),
            None => panic!("no such column {:?}", idx),
        }
    }

    /// Retrieves the contents of a field of the row.
    ///
    /// A field can be accessed by the name or index of its column, though
    /// access by index is more efficient. Rows are 0-indexed.
    ///
    /// Returns `None` if the index does not reference a column, `Some(Err(..))`
    /// if there was an error converting the result value, and `Some(Ok(..))`
    /// on success.
    pub fn get_opt<I, T>(&self, idx: I) -> Option<Result<T>>
    where
        I: RowIndex,
        T: FromSql,
    {
        self.get_inner(&idx)
    }

    fn get_inner<I, T>(&self, idx: &I) -> Option<Result<T>>
    where
        I: RowIndex,
        T: FromSql,
    {
        let idx = match idx.__idx(&self.stmt_info.columns) {
            Some(idx) => idx,
            None => return None,
        };

        let ty = self.stmt_info.columns[idx].type_();
        if !<T as FromSql>::accepts(ty) {
            return Some(Err(error::conversion(Box::new(WrongType::new(ty.clone())))));
        }
        let value = FromSql::from_sql_nullable(ty, self.data.get(idx));
        Some(value.map_err(error::conversion))
    }

    /// Retrieves the specified field as a raw buffer of Postgres data.
    ///
    /// # Panics
    ///
    /// Panics if the index does not reference a column.
    pub fn get_bytes<I>(&self, idx: I) -> Option<&[u8]>
    where
        I: RowIndex + fmt::Debug,
    {
        match idx.__idx(&self.stmt_info.columns) {
            Some(idx) => self.data.get(idx),
            None => panic!("invalid index {:?}", idx),
        }
    }
}

/// A lazily-loaded iterator over the resulting rows of a query.
pub struct LazyRows<'trans, 'stmt> {
    stmt: &'stmt Statement<'stmt>,
    data: VecDeque<RowData>,
    name: String,
    row_limit: i32,
    more_rows: bool,
    finished: bool,
    _trans: &'trans Transaction<'trans>,
}

impl<'a, 'b> Drop for LazyRows<'a, 'b> {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.finish_inner();
        }
    }
}

impl<'a, 'b> fmt::Debug for LazyRows<'a, 'b> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("LazyRows")
            .field("name", &self.name)
            .field("row_limit", &self.row_limit)
            .field("remaining_rows", &self.data.len())
            .field("more_rows", &self.more_rows)
            .finish()
    }
}

impl<'trans, 'stmt> LazyRows<'trans, 'stmt> {
    pub(crate) fn new(
        stmt: &'stmt Statement<'stmt>,
        data: VecDeque<RowData>,
        name: String,
        row_limit: i32,
        more_rows: bool,
        finished: bool,
        trans: &'trans Transaction<'trans>,
    ) -> LazyRows<'trans, 'stmt> {
        LazyRows {
            stmt: stmt,
            data: data,
            name: name,
            row_limit: row_limit,
            more_rows: more_rows,
            finished: finished,
            _trans: trans,
        }
    }

    fn finish_inner(&mut self) -> Result<()> {
        let mut conn = self.stmt.conn().0.borrow_mut();
        check_desync!(conn);
        conn.close_statement(&self.name, b'P')
    }

    fn execute(&mut self) -> Result<()> {
        let mut conn = self.stmt.conn().0.borrow_mut();

        conn.stream.write_message(|buf| {
            frontend::execute(&self.name, self.row_limit, buf)
        })?;
        conn.stream.write_message(
            |buf| Ok::<(), io::Error>(frontend::sync(buf)),
        )?;
        conn.stream.flush()?;
        conn.read_rows(|row| self.data.push_back(row)).map(
            |more_rows| {
                self.more_rows = more_rows
            },
        )
    }

    /// Returns a slice describing the columns of the `LazyRows`.
    pub fn columns(&self) -> &[Column] {
        self.stmt.columns()
    }

    /// Consumes the `LazyRows`, cleaning up associated state.
    ///
    /// Functionally identical to the `Drop` implementation on `LazyRows`
    /// except that it returns any error to the caller.
    pub fn finish(mut self) -> Result<()> {
        self.finish_inner()
    }
}

impl<'trans, 'stmt> FallibleIterator for LazyRows<'trans, 'stmt> {
    type Item = Row<'stmt>;
    type Error = Error;

    fn next(&mut self) -> Result<Option<Row<'stmt>>> {
        if self.data.is_empty() && self.more_rows {
            self.execute()?;
        }

        let row = self.data.pop_front().map(|r| {
            Row {
                stmt_info: &**self.stmt.info(),
                data: MaybeOwned::Owned(r),
            }
        });

        Ok(row)
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let lower = self.data.len();
        let upper = if self.more_rows { None } else { Some(lower) };
        (lower, upper)
    }
}

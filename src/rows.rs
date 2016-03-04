//! Query result rows.

use std::ascii::AsciiExt;
use std::borrow::Cow;
use std::collections::VecDeque;
use std::fmt;
use std::ops::Deref;
use std::slice;

use {Result, SessionInfoNew, RowsNew, LazyRowsNew, StatementInternals, WrongTypeNew};
use transaction::Transaction;
use types::{FromSql, SessionInfo, WrongType};
use stmt::{Statement, Column};
use error::Error;
use message::Frontend;

enum StatementContainer<'a> {
    Borrowed(&'a Statement<'a>),
    Owned(Statement<'a>),
}

impl<'a> Deref for StatementContainer<'a> {
    type Target = Statement<'a>;

    fn deref(&self) -> &Statement<'a> {
        match *self {
            StatementContainer::Borrowed(s) => s,
            StatementContainer::Owned(ref s) => s,
        }
    }
}

/// The resulting rows of a query.
pub struct Rows<'stmt> {
    stmt: StatementContainer<'stmt>,
    data: Vec<Vec<Option<Vec<u8>>>>,
}

impl<'a> RowsNew<'a> for Rows<'a> {
    fn new(stmt: &'a Statement<'a>, data: Vec<Vec<Option<Vec<u8>>>>) -> Rows<'a> {
        Rows {
            stmt: StatementContainer::Borrowed(stmt),
            data: data,
        }
    }

    fn new_owned(stmt: Statement<'a>, data: Vec<Vec<Option<Vec<u8>>>>) -> Rows<'a> {
        Rows {
            stmt: StatementContainer::Owned(stmt),
            data: data,
        }
    }
}

impl<'a> fmt::Debug for Rows<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Rows")
           .field("columns", &self.columns())
           .field("rows", &self.data.len())
           .finish()
    }
}

impl<'stmt> Rows<'stmt> {
    /// Returns a slice describing the columns of the `Rows`.
    pub fn columns(&self) -> &[Column] {
        self.stmt.columns()
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
            stmt: &*self.stmt,
            data: Cow::Borrowed(&self.data[idx]),
        }
    }

    /// Returns an iterator over the `Row`s.
    pub fn iter<'a>(&'a self) -> Iter<'a> {
        Iter {
            stmt: &*self.stmt,
            iter: self.data.iter(),
        }
    }
}

impl<'a> IntoIterator for &'a Rows<'a> {
    type Item = Row<'a>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

/// An iterator over `Row`s.
pub struct Iter<'a> {
    stmt: &'a Statement<'a>,
    iter: slice::Iter<'a, Vec<Option<Vec<u8>>>>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = Row<'a>;

    fn next(&mut self) -> Option<Row<'a>> {
        self.iter.next().map(|row| {
            Row {
                stmt: &*self.stmt,
                data: Cow::Borrowed(row),
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
                stmt: &*self.stmt,
                data: Cow::Borrowed(row),
            }
        })
    }
}

impl<'a> ExactSizeIterator for Iter<'a> {}

/// A single result row of a query.
pub struct Row<'a> {
    stmt: &'a Statement<'a>,
    data: Cow<'a, [Option<Vec<u8>>]>,
}

impl<'a> fmt::Debug for Row<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Row")
           .field("statement", self.stmt)
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
        self.stmt.columns()
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
    /// # use postgres::{Connection, SslMode};
    /// # let conn = Connection::connect("", SslMode::None).unwrap();
    /// let stmt = conn.prepare("SELECT foo, bar from BAZ").unwrap();
    /// for row in &stmt.query(&[]).unwrap() {
    ///     let foo: i32 = row.get(0);
    ///     let bar: String = row.get("bar");
    ///     println!("{}: {}", foo, bar);
    /// }
    /// ```
    pub fn get<I, T>(&self, idx: I) -> T
        where I: RowIndex + fmt::Debug,
              T: FromSql
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
        where I: RowIndex,
              T: FromSql
    {
        self.get_inner(&idx)
    }

    fn get_inner<I, T>(&self, idx: &I) -> Option<Result<T>>
        where I: RowIndex,
              T: FromSql
    {
        let idx = match idx.idx(self.stmt) {
            Some(idx) => idx,
            None => return None,
        };

        let ty = self.stmt.columns()[idx].type_();
        if !<T as FromSql>::accepts(ty) {
            return Some(Err(Error::Conversion(Box::new(WrongType::new(ty.clone())))));
        }
        let conn = self.stmt.conn().conn.borrow();
        let value = match self.data[idx] {
            Some(ref data) => FromSql::from_sql(ty, &mut &**data, &SessionInfo::new(&*conn)),
            None => FromSql::from_sql_null(ty, &SessionInfo::new(&*conn)),
        };
        Some(value)
    }

    /// Retrieves the specified field as a raw buffer of Postgres data.
    ///
    /// # Panics
    ///
    /// Panics if the index does not reference a column.
    pub fn get_bytes<I>(&self, idx: I) -> Option<&[u8]>
        where I: RowIndex + fmt::Debug
    {
        match idx.idx(self.stmt) {
            Some(idx) => self.data[idx].as_ref().map(|e| &**e),
            None => panic!("invalid index {:?}", idx),
        }
    }
}

/// A trait implemented by types that can index into columns of a row.
pub trait RowIndex {
    /// Returns the index of the appropriate column, or `None` if no such
    /// column exists.
    fn idx(&self, stmt: &Statement) -> Option<usize>;
}

impl RowIndex for usize {
    #[inline]
    fn idx(&self, stmt: &Statement) -> Option<usize> {
        if *self >= stmt.columns().len() {
            None
        } else {
            Some(*self)
        }
    }
}

impl<'a> RowIndex for &'a str {
    #[inline]
    fn idx(&self, stmt: &Statement) -> Option<usize> {
        if let Some(idx) = stmt.columns().iter().position(|d| d.name() == *self) {
            return Some(idx);
        };

        // FIXME ASCII-only case insensitivity isn't really the right thing to
        // do. Postgres itself uses a dubious wrapper around tolower and JDBC
        // uses the US locale.
        stmt.columns().iter().position(|d| d.name().eq_ignore_ascii_case(*self))
    }
}

/// A lazily-loaded iterator over the resulting rows of a query.
pub struct LazyRows<'trans, 'stmt> {
    stmt: &'stmt Statement<'stmt>,
    data: VecDeque<Vec<Option<Vec<u8>>>>,
    name: String,
    row_limit: i32,
    more_rows: bool,
    finished: bool,
    _trans: &'trans Transaction<'trans>,
}

impl<'trans, 'stmt> LazyRowsNew<'trans, 'stmt> for LazyRows<'trans, 'stmt> {
    fn new(stmt: &'stmt Statement<'stmt>,
           data: VecDeque<Vec<Option<Vec<u8>>>>,
           name: String,
           row_limit: i32,
           more_rows: bool,
           finished: bool,
           trans: &'trans Transaction<'trans>)
           -> LazyRows<'trans, 'stmt> {
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
    fn finish_inner(&mut self) -> Result<()> {
        let mut conn = self.stmt.conn().conn.borrow_mut();
        check_desync!(conn);
        conn.close_statement(&self.name, b'P')
    }

    fn execute(&mut self) -> Result<()> {
        let mut conn = self.stmt.conn().conn.borrow_mut();

        try!(conn.write_messages(&[Frontend::Execute {
                                       portal: &self.name,
                                       max_rows: self.row_limit,
                                   },
                                   Frontend::Sync]));
        conn.read_rows(&mut self.data).map(|more_rows| self.more_rows = more_rows)
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

impl<'trans, 'stmt> Iterator for LazyRows<'trans, 'stmt> {
    type Item = Result<Row<'stmt>>;

    fn next(&mut self) -> Option<Result<Row<'stmt>>> {
        if self.data.is_empty() && self.more_rows {
            if let Err(err) = self.execute() {
                return Some(Err(err));
            }
        }

        self.data.pop_front().map(|r| {
            Ok(Row {
                stmt: self.stmt,
                data: Cow::Owned(r),
            })
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let lower = self.data.len();
        let upper = if self.more_rows {
            None
        } else {
            Some(lower)
        };
        (lower, upper)
    }
}

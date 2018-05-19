//! Query result rows.

use postgres_shared::rows::RowData;
use std::fmt;
use std::slice;
use std::str;

#[doc(inline)]
pub use postgres_shared::rows::RowIndex;

use stmt::{Column};
use {Result, error};

/// The resulting rows of a query.
pub struct TextRows {
    columns: Vec<Column>,
    data: Vec<RowData>,
}

impl fmt::Debug for TextRows {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("TextRows")
            .field("columns", &self.columns())
            .field("rows", &self.data.len())
            .finish()
    }
}

impl TextRows {
    pub(crate) fn new(columns: Vec<Column>, data: Vec<RowData>) -> TextRows {
        TextRows {
            columns: columns,
            data: data,
        }
    }

    /// Returns a slice describing the columns of the `TextRows`.
    pub fn columns(&self) -> &[Column] {
        &self.columns[..]
    }

    /// Returns the number of rows present.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Determines if there are any rows present.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a specific `TextRow`.
    ///
    /// # Panics
    ///
    /// Panics if `idx` is out of bounds.
    pub fn get<'a>(&'a self, idx: usize) -> TextRow<'a> {
        TextRow {
            columns: &self.columns,
            data: &self.data[idx],
        }
    }

    /// Returns an iterator over the `TextRow`s.
    pub fn iter<'a>(&'a self) -> Iter<'a> {
        Iter {
            columns: self.columns(),
            iter: self.data.iter(),
        }
    }
}

impl<'a> IntoIterator for &'a TextRows {
    type Item = TextRow<'a>;
    type IntoIter = Iter<'a>;

    fn into_iter(self) -> Iter<'a> {
        self.iter()
    }
}

/// An iterator over `TextRow`s.
pub struct Iter<'a> {
    columns: &'a [Column],
    iter: slice::Iter<'a, RowData>,
}

impl<'a> Iterator for Iter<'a> {
    type Item = TextRow<'a>;

    fn next(&mut self) -> Option<TextRow<'a>> {
        self.iter.next().map(|row| {
            TextRow {
                columns: self.columns,
                data: row,
            }
        })
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.iter.size_hint()
    }
}

impl<'a> DoubleEndedIterator for Iter<'a> {
    fn next_back(&mut self) -> Option<TextRow<'a>> {
        self.iter.next_back().map(|row| {
            TextRow {
                columns: self.columns,
                data: row,
            }
        })
    }
}

impl<'a> ExactSizeIterator for Iter<'a> {}

/// A single result row of a query.
pub struct TextRow<'a> {
    columns: &'a [Column],
    data: &'a RowData,
}

impl<'a> fmt::Debug for TextRow<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("TextRow")
            .field("columns", &self.columns)
            .finish()
    }
}

impl<'a> TextRow<'a> {
    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Determines if there are any values in the row.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns a slice describing the columns of the `TextRow`.
    pub fn columns(&self) -> &[Column] {
        self.columns
    }

    /// Retrieve the contents of a field of a row
    ///
    /// A field can be accessed by the name or index of its column, though
    /// access by index is more efficient. Rows are 0-indexed.
    ///
    /// # Panics
    ///
    /// Panics if the index does not reference a column
    pub fn get<I>(&self, idx: I) -> &str
    where
        I: RowIndex + fmt::Debug,
    {
        match self.get_inner(&idx) {
            Some(Ok(value)) => value,
            Some(Err(err)) => panic!("error retrieving column {:?}: {:?}", idx, err),
            None => panic!("no such column {:?}", idx),
        }
    }

    /// Retrieves the contents of a field of the row.
    ///
    /// A field can be accessed by the name or index of its column, though
    /// access by index is more efficient. Rows are 0-indexed.
    ///
    /// Returns None if the index does not reference a column, Some(Err(..)) if
    /// there was an error parsing the result as UTF-8, and Some(Ok(..)) on
    /// success.
    pub fn get_opt<I>(&self, idx: I) -> Option<Result<&str>>
    where
        I: RowIndex,
    {
        self.get_inner(&idx)
    }

    fn get_inner<I>(&self, idx: &I) -> Option<Result<&str>>
    where
        I: RowIndex,
    {
        let idx = match idx.__idx(self.columns) {
            Some(idx) => idx,
            None => return None,
        };

        self.data.get(idx)
            .map(|s| str::from_utf8(s).map_err(|e| error::conversion(Box::new(e))))
    }
}

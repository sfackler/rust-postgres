use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::DataRowBody;
use std::ascii::AsciiExt;
use std::io;
use std::ops::Range;

use stmt::Column;

/// A trait implemented by types that can index into columns of a row.
pub trait RowIndex {
    /// Returns the index of the appropriate column, or `None` if no such
    /// column exists.
    fn idx(&self, stmt: &[Column]) -> Option<usize>;
}

impl RowIndex for usize {
    #[inline]
    fn idx(&self, stmt: &[Column]) -> Option<usize> {
        if *self >= stmt.len() {
            None
        } else {
            Some(*self)
        }
    }
}

impl<'a> RowIndex for str {
    #[inline]
    fn idx(&self, stmt: &[Column]) -> Option<usize> {
        if let Some(idx) = stmt.iter().position(|d| d.name() == self) {
            return Some(idx);
        };

        // FIXME ASCII-only case insensitivity isn't really the right thing to
        // do. Postgres itself uses a dubious wrapper around tolower and JDBC
        // uses the US locale.
        stmt.iter().position(
            |d| d.name().eq_ignore_ascii_case(self),
        )
    }
}

impl<'a, T: ?Sized> RowIndex for &'a T
where
    T: RowIndex,
{
    #[inline]
    fn idx(&self, columns: &[Column]) -> Option<usize> {
        T::idx(*self, columns)
    }
}

#[doc(hidden)]
pub struct RowData {
    body: DataRowBody,
    ranges: Vec<Option<Range<usize>>>,
}

impl RowData {
    pub fn new(body: DataRowBody) -> io::Result<RowData> {
        let ranges = body.ranges().collect()?;
        Ok(RowData {
            body: body,
            ranges: ranges,
        })
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn get(&self, index: usize) -> Option<&[u8]> {
        match &self.ranges[index] {
            &Some(ref range) => Some(&self.body.buffer()[range.clone()]),
            &None => None,
        }
    }
}

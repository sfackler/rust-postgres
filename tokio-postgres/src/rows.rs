use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::DataRowBody;
use std::io;
use std::ops::Range;

use crate::rows::sealed::Sealed;
use crate::stmt::Column;

mod sealed {
    use crate::stmt::Column;

    pub trait Sealed {
        fn __idx(&self, stmt: &[Column]) -> Option<usize>;
    }
}

/// A trait implemented by types that can index into columns of a row.
///
/// This cannot be implemented outside of this crate.
pub trait RowIndex: Sealed {}

impl Sealed for usize {
    #[inline]
    fn __idx(&self, stmt: &[Column]) -> Option<usize> {
        if *self >= stmt.len() {
            None
        } else {
            Some(*self)
        }
    }
}

impl RowIndex for usize {}

impl Sealed for str {
    #[inline]
    fn __idx(&self, stmt: &[Column]) -> Option<usize> {
        if let Some(idx) = stmt.iter().position(|d| d.name() == self) {
            return Some(idx);
        };

        // FIXME ASCII-only case insensitivity isn't really the right thing to
        // do. Postgres itself uses a dubious wrapper around tolower and JDBC
        // uses the US locale.
        stmt.iter()
            .position(|d| d.name().eq_ignore_ascii_case(self))
    }
}

impl RowIndex for str {}

impl<'a, T> Sealed for &'a T
where
    T: ?Sized + Sealed,
{
    #[inline]
    fn __idx(&self, columns: &[Column]) -> Option<usize> {
        T::__idx(*self, columns)
    }
}

impl<'a, T> RowIndex for &'a T where T: ?Sized + Sealed {}

pub(crate) struct RowData {
    body: DataRowBody,
    ranges: Vec<Option<Range<usize>>>,
}

impl RowData {
    pub fn parse(body: DataRowBody) -> io::Result<RowData> {
        let ranges = body.ranges().collect()?;
        Ok(RowData { body, ranges })
    }

    pub fn get(&self, index: usize) -> Option<&[u8]> {
        self.ranges[index].clone().map(|r| &self.body.buffer()[r])
    }
}

#![allow(unknown_lints)] // for clippy

extern crate hex;
extern crate fallible_iterator;
extern crate phf;
extern crate postgres_protocol;

use fallible_iterator::{FallibleIterator, FromFallibleIterator};
use std::ascii::AsciiExt;
use std::ops::Range;

use types::Type;

pub mod error;
pub mod params;
pub mod types;

pub struct RowData {
    buf: Vec<u8>,
    indices: Vec<Option<Range<usize>>>,
}

impl<'a> FromFallibleIterator<Option<&'a [u8]>> for RowData {
    fn from_fallible_iterator<I>(mut it: I) -> Result<RowData, I::Error>
        where I: FallibleIterator<Item = Option<&'a [u8]>>
    {
        let mut row = RowData {
            buf: vec![],
            indices: Vec::with_capacity(it.size_hint().0),
        };

        while let Some(cell) = try!(it.next()) {
            let index = match cell {
                Some(cell) =>  {
                    let base = row.buf.len();
                    row.buf.extend_from_slice(cell);
                    Some(base..row.buf.len())
                }
                None => None,
            };
            row.indices.push(index);
        }

        Ok(row)
    }
}

impl RowData {
    pub fn len(&self) -> usize {
        self.indices.len()
    }

    pub fn get(&self, index: usize) -> Option<&[u8]> {
        match &self.indices[index] {
            &Some(ref range) => Some(&self.buf[range.clone()]),
            &None => None,
        }
    }
}

pub struct Column {
    name: String,
    type_: Type,
}

impl Column {
    #[doc(hidden)]
    pub fn new(name: String, type_: Type) -> Column {
        Column {
            name: name,
            type_: type_,
        }
    }

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_(&self) -> &Type {
        &self.type_
    }
}

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
        stmt.iter().position(|d| d.name().eq_ignore_ascii_case(self))
    }
}

impl<'a, T: ?Sized> RowIndex for &'a T
    where T: RowIndex
{
    #[inline]
    fn idx(&self, columns: &[Column]) -> Option<usize> {
        T::idx(*self, columns)
    }
}

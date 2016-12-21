#![allow(unknown_lints)] // for clippy

extern crate hex;
extern crate fallible_iterator;
extern crate phf;
extern crate postgres_protocol;

use fallible_iterator::{FallibleIterator, FromFallibleIterator};
use std::ops::Range;

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

use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::DataRowBody;
use std::fmt;
use std::ops::Range;
use std::str;

use crate::proto;
use crate::row::sealed::Sealed;
use crate::stmt::Column;
use crate::types::{FromSql, WrongType};
use crate::Error;

mod sealed {
    pub trait Sealed {}
}

/// A trait implemented by types that can index into columns of a row.
///
/// This cannot be implemented outside of this crate.
pub trait RowIndex: Sealed {
    #[doc(hidden)]
    fn __idx(&self, columns: &[Column]) -> Option<usize>;
}

impl Sealed for usize {}

impl RowIndex for usize {
    #[inline]
    fn __idx(&self, columns: &[Column]) -> Option<usize> {
        if *self >= columns.len() {
            None
        } else {
            Some(*self)
        }
    }
}

impl Sealed for str {}

impl RowIndex for str {
    #[inline]
    fn __idx(&self, columns: &[Column]) -> Option<usize> {
        if let Some(idx) = columns.iter().position(|d| d.name() == self) {
            return Some(idx);
        };

        // FIXME ASCII-only case insensitivity isn't really the right thing to
        // do. Postgres itself uses a dubious wrapper around tolower and JDBC
        // uses the US locale.
        columns
            .iter()
            .position(|d| d.name().eq_ignore_ascii_case(self))
    }
}

impl<'a, T> Sealed for &'a T where T: ?Sized + Sealed {}

impl<'a, T> RowIndex for &'a T
where
    T: ?Sized + RowIndex,
{
    #[inline]
    fn __idx(&self, columns: &[Column]) -> Option<usize> {
        T::__idx(*self, columns)
    }
}

pub struct Row {
    statement: proto::Statement,
    body: DataRowBody,
    ranges: Vec<Option<Range<usize>>>,
}

impl Row {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(statement: proto::Statement, body: DataRowBody) -> Result<Row, Error> {
        let ranges = body.ranges().collect().map_err(Error::parse)?;
        Ok(Row {
            statement,
            body,
            ranges,
        })
    }

    pub fn columns(&self) -> &[Column] {
        self.statement.columns()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.columns().len()
    }

    pub fn get<'a, I, T>(&'a self, idx: I) -> T
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'a>,
    {
        match self.get_inner(&idx) {
            Ok(Some(ok)) => ok,
            Err(err) => panic!("error retrieving column {}: {}", idx, err),
            Ok(None) => panic!("no such column {}", idx),
        }
    }

    pub fn try_get<'a, I, T>(&'a self, idx: I) -> Result<Option<T>, Error>
    where
        I: RowIndex,
        T: FromSql<'a>,
    {
        self.get_inner(&idx)
    }

    fn get_inner<'a, I, T>(&'a self, idx: &I) -> Result<Option<T>, Error>
    where
        I: RowIndex,
        T: FromSql<'a>,
    {
        let idx = match idx.__idx(self.columns()) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let ty = self.columns()[idx].type_();
        if !T::accepts(ty) {
            return Err(Error::from_sql(Box::new(WrongType::new(ty.clone()))));
        }

        let buf = self.ranges[idx].clone().map(|r| &self.body.buffer()[r]);
        let value = FromSql::from_sql_nullable(ty, buf);
        value.map(Some).map_err(Error::from_sql)
    }
}

pub struct StringRow {
    body: DataRowBody,
    ranges: Vec<Option<Range<usize>>>,
}

impl StringRow {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(body: DataRowBody) -> Result<StringRow, Error> {
        let ranges = body.ranges().collect().map_err(Error::parse)?;
        Ok(StringRow { body, ranges })
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn len(&self) -> usize {
        self.ranges.len()
    }

    pub fn get(&self, idx: usize) -> Option<&str> {
        match self.try_get(idx) {
            Ok(Some(ok)) => ok,
            Err(err) => panic!("error retrieving column {}: {}", idx, err),
            Ok(None) => panic!("no such column {}", idx),
        }
    }

    #[allow(clippy::option_option)] // FIXME
    pub fn try_get(&self, idx: usize) -> Result<Option<Option<&str>>, Error> {
        let buf = match self.ranges.get(idx) {
            Some(range) => range.clone().map(|r| &self.body.buffer()[r]),
            None => return Ok(None),
        };

        let v = match buf {
            Some(buf) => {
                let s = str::from_utf8(buf).map_err(|e| Error::from_sql(Box::new(e)))?;
                Some(s)
            }
            None => None,
        };

        Ok(Some(v))
    }
}

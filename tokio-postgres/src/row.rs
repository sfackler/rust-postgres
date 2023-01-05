//! Rows.

use crate::row::sealed::{AsName, Sealed};
use crate::simple_query::SimpleColumn;
use crate::statement::{Column, RowDescription};
use crate::types::{FromSql, Type, WrongType};
use crate::Error;
use bytes::{BufMut, BytesMut};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::DataRowBody;
use postgres_types::{IsNull, ToSql};
use std::fmt;
use std::ops::Range;
use std::str;
use std::sync::Arc;

mod sealed {
    pub trait Sealed {}

    pub trait AsName {
        fn as_name(&self) -> &str;
    }
}

impl AsName for Column {
    fn as_name(&self) -> &str {
        self.name()
    }
}

impl AsName for String {
    fn as_name(&self) -> &str {
        self
    }
}

/// A trait implemented by types that can index into columns of a row.
///
/// This cannot be implemented outside of this crate.
pub trait RowIndex: Sealed {
    #[doc(hidden)]
    fn __idx<T>(&self, columns: &[T]) -> Option<usize>
    where
        T: AsName;
}

impl Sealed for usize {}

impl RowIndex for usize {
    #[inline]
    fn __idx<T>(&self, columns: &[T]) -> Option<usize>
    where
        T: AsName,
    {
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
    fn __idx<T>(&self, columns: &[T]) -> Option<usize>
    where
        T: AsName,
    {
        if let Some(idx) = columns.iter().position(|d| d.as_name() == self) {
            return Some(idx);
        };

        // FIXME ASCII-only case insensitivity isn't really the right thing to
        // do. Postgres itself uses a dubious wrapper around tolower and JDBC
        // uses the US locale.
        columns
            .iter()
            .position(|d| d.as_name().eq_ignore_ascii_case(self))
    }
}

impl<'a, T> Sealed for &'a T where T: ?Sized + Sealed {}

impl<'a, T> RowIndex for &'a T
where
    T: ?Sized + RowIndex,
{
    #[inline]
    fn __idx<U>(&self, columns: &[U]) -> Option<usize>
    where
        U: AsName,
    {
        T::__idx(*self, columns)
    }
}

/// A row of data returned from the database by a query.
pub struct Row {
    description: Arc<dyn RowDescription>,
    body: DataRowBody,
    ranges: Vec<Option<Range<usize>>>,
}

impl fmt::Debug for Row {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Row")
            .field("columns", &self.columns())
            .finish()
    }
}

impl Row {
    pub(crate) fn new(
        description: Arc<dyn RowDescription>,
        body: DataRowBody,
    ) -> Result<Row, Error> {
        let ranges = body.ranges().collect().map_err(Error::parse)?;
        Ok(Row {
            description,
            body,
            ranges,
        })
    }

    /// Returns description about the data in the row.
    pub fn description(&self) -> Arc<dyn RowDescription> {
        self.description.clone()
    }

    /// Returns information about the columns of data in the row.
    pub fn columns(&self) -> &[Column] {
        self.description.columns()
    }

    /// Determines if the row contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.columns().len()
    }

    /// Deserializes a value from the row.
    ///
    /// The value can be specified either by its numeric index in the row, or by its column name.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified type.
    pub fn get<'a, I, T>(&'a self, idx: I) -> T
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'a>,
    {
        match self.get_inner(&idx) {
            Ok(ok) => ok,
            Err(err) => panic!("error retrieving column {}: {}", idx, err),
        }
    }

    /// Like `Row::get`, but returns a `Result` rather than panicking.
    pub fn try_get<'a, I, T>(&'a self, idx: I) -> Result<T, Error>
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'a>,
    {
        self.get_inner(&idx)
    }

    fn get_inner<'a, I, T>(&'a self, idx: &I) -> Result<T, Error>
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'a>,
    {
        let idx = match idx.__idx(self.columns()) {
            Some(idx) => idx,
            None => return Err(Error::column(idx.to_string())),
        };

        let ty = self.columns()[idx].type_();
        if !T::accepts(ty) {
            return Err(Error::from_sql(
                Box::new(WrongType::new::<T>(ty.clone())),
                idx,
            ));
        }

        FromSql::from_sql_nullable(ty, self.col_buffer(idx)).map_err(|e| Error::from_sql(e, idx))
    }

    /// Get the raw bytes for the column at the given index.
    fn col_buffer(&self, idx: usize) -> Option<&[u8]> {
        let range = self.ranges[idx].to_owned()?;
        Some(&self.body.buffer()[range])
    }
}

impl AsName for SimpleColumn {
    fn as_name(&self) -> &str {
        self.name()
    }
}

/// A row of data returned from the database by a simple query.
#[derive(Debug)]
pub struct SimpleQueryRow {
    columns: Arc<[SimpleColumn]>,
    body: DataRowBody,
    ranges: Vec<Option<Range<usize>>>,
}

impl SimpleQueryRow {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(
        columns: Arc<[SimpleColumn]>,
        body: DataRowBody,
    ) -> Result<SimpleQueryRow, Error> {
        let ranges = body.ranges().collect().map_err(Error::parse)?;
        Ok(SimpleQueryRow {
            columns,
            body,
            ranges,
        })
    }

    /// Returns information about the columns of data in the row.
    pub fn columns(&self) -> &[SimpleColumn] {
        &self.columns
    }

    /// Determines if the row contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of values in the row.
    pub fn len(&self) -> usize {
        self.columns.len()
    }

    /// Returns a value from the row.
    ///
    /// The value can be specified either by its numeric index in the row, or by its column name.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified type.
    pub fn get<I>(&self, idx: I) -> Option<&str>
    where
        I: RowIndex + fmt::Display,
    {
        match self.get_inner(&idx) {
            Ok(ok) => ok,
            Err(err) => panic!("error retrieving column {}: {}", idx, err),
        }
    }

    /// Like `SimpleQueryRow::get`, but returns a `Result` rather than panicking.
    pub fn try_get<I>(&self, idx: I) -> Result<Option<&str>, Error>
    where
        I: RowIndex + fmt::Display,
    {
        self.get_inner(&idx)
    }

    fn get_inner<I>(&self, idx: &I) -> Result<Option<&str>, Error>
    where
        I: RowIndex + fmt::Display,
    {
        let idx = match idx.__idx(&self.columns) {
            Some(idx) => idx,
            None => return Err(Error::column(idx.to_string())),
        };

        let buf = self.ranges[idx].clone().map(|r| &self.body.buffer()[r]);
        FromSql::from_sql_nullable(&Type::TEXT, buf).map_err(|e| Error::from_sql(e, idx))
    }
}
/// Builder for building a [`Row`].
pub struct RowBuilder {
    desc: Arc<dyn RowDescription>,
    buf: BytesMut,
    n: usize,
}

impl RowBuilder {
    /// Creates a new builder using the provided row description.
    pub fn new(desc: Arc<dyn RowDescription>) -> Self {
        Self {
            desc,
            buf: BytesMut::new(),
            n: 0,
        }
    }

    /// Appends a column's value and returns a value indicates if this value should be represented
    /// as NULL.
    pub fn push(&mut self, value: Option<impl ToSql>) -> Result<IsNull, Error> {
        let columns = self.desc.columns();

        if columns.len() == self.n {
            return Err(Error::column(
                "exceeded expected number of columns".to_string(),
            ));
        }

        let db_type = columns[self.n].type_();
        let start = self.buf.len();

        // Reserve 4 bytes for the length of the binary data to be written
        self.buf.put_i32(-1i32);

        let is_null = value
            .to_sql(db_type, &mut self.buf)
            .map_err(|e| Error::to_sql(e, self.n))?;

        // Calculate the length of data just written.
        if is_null == IsNull::No {
            let len = (self.buf.len() - start - 4) as i32;
            // Update the length of data
            self.buf[start..start + 4].copy_from_slice(&len.to_be_bytes());
        };

        self.n += 1;
        Ok(is_null)
    }

    /// Builds the row.
    pub fn build(self) -> Result<Row, Error> {
        Row::new(
            self.desc.clone(),
            DataRowBody::new(self.buf.freeze(), self.n as u16),
        )
    }
}

#[cfg(test)]
mod tests {
    use postgres_types::IsNull;

    use super::*;
    use std::net::IpAddr;

    struct TestRowDescription {
        columns: Vec<Column>,
    }

    impl RowDescription for TestRowDescription {
        fn columns(&self) -> &[Column] {
            &self.columns
        }
    }

    #[test]
    fn test_row_builder() {
        let mut builder = RowBuilder::new(Arc::new(TestRowDescription {
            columns: vec![
                Column::new("id".to_string(), Type::INT8),
                Column::new("name".to_string(), Type::VARCHAR),
                Column::new("ip".to_string(), Type::INET),
            ],
        }));

        let expected_id = 1234i64;
        let is_null = builder.push(Some(expected_id)).unwrap();
        assert_eq!(IsNull::No, is_null);

        let expected_name = "row builder";
        let is_null = builder.push(Some(expected_name)).unwrap();
        assert_eq!(IsNull::No, is_null);

        let is_null = builder.push(None::<IpAddr>).unwrap();
        assert_eq!(IsNull::Yes, is_null);

        let row = builder.build().unwrap();

        let actual_id: i64 = row.try_get("id").unwrap();
        assert_eq!(expected_id, actual_id);

        let actual_name: String = row.try_get("name").unwrap();
        assert_eq!(expected_name, actual_name);

        let actual_dt: Option<IpAddr> = row.try_get("ip").unwrap();
        assert_eq!(None, actual_dt);
    }
}

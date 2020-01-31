//! Rows.

use crate::row::sealed::{AsName, Sealed};
use crate::statement::Column;
use crate::types::{Field, FromSql, Kind, Type, WrongType};
use crate::{Error, Statement};
use byteorder::{BigEndian, ByteOrder};
use fallible_iterator::FallibleIterator;
use postgres_protocol::message::backend::{DataRowBody, DataRowRanges};
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

impl AsName for Field {
    fn as_name(&self) -> &str {
        self.name()
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
    statement: Statement,
    body: DataRowBody,
    ranges: Vec<Option<Range<usize>>>,
}

impl Row {
    pub(crate) fn new(statement: Statement, body: DataRowBody) -> Result<Row, Error> {
        let ranges = body.ranges().collect().map_err(Error::parse)?;
        Ok(Row {
            statement,
            body,
            ranges,
        })
    }

    /// Returns information about the columns of data in the row.
    pub fn columns(&self) -> &[Column] {
        self.statement.columns()
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

        let buf = self.ranges[idx].clone().map(|r| &self.body.buffer()[r]);
        FromSql::from_sql_nullable(ty, buf).map_err(|e| Error::from_sql(e, idx))
    }
}

/// PostgreSQL composite type.
/// Fields of a type can be accessed using `CompositeType::get` and `CompositeType::try_get` methods.
pub struct CompositeType<'a> {
    type_: Type,
    body: &'a [u8],
    ranges: Vec<Option<Range<usize>>>,
}

impl<'a> FromSql<'a> for CompositeType<'a> {
    fn from_sql(
        type_: &Type,
        raw: &'a [u8],
    ) -> Result<CompositeType<'a>, Box<dyn std::error::Error + Sync + Send>> {
        match *type_.kind() {
            Kind::Composite(_) => {
                let composite_type = CompositeType::new(type_.clone(), raw)?;
                Ok(composite_type)
            }
            _ => Err(format!("expected composite type, got {}", type_).into()),
        }
    }
    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Composite(_) => true,
            _ => false,
        }
    }
}

fn composite_type_fields(type_: &Type) -> &[Field] {
    match type_.kind() {
        Kind::Composite(ref fields) => fields,
        _ => unreachable!(),
    }
}

impl<'a> CompositeType<'a> {
    pub(crate) fn new(
        type_: Type,
        body: &'a [u8],
    ) -> Result<CompositeType<'a>, Box<dyn std::error::Error + Sync + Send>> {
        let fields: &[Field] = composite_type_fields(&type_);
        if body.len() < 4 {
            let message = format!("invalid composite type body length: {}", body.len());
            return Err(message.into());
        }
        let num_fields: i32 = BigEndian::read_i32(&body[0..4]);
        if num_fields as usize != fields.len() {
            let message = format!("invalid field count: {} vs {}", num_fields, fields.len());
            return Err(message.into());
        }
        let ranges =
            DataRowRanges::composite_type_ranges(&body[4..], body.len(), num_fields as u16)
                .collect()
                .map_err(Error::parse)?;

        Ok(CompositeType {
            type_,
            body,
            ranges,
        })
    }

    /// Returns information about the fields of the composite type.
    pub fn fields(&self) -> &[Field] {
        composite_type_fields(&self.type_)
    }

    /// Determines if the composite contains no values.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the number of fields of the composite type.
    pub fn len(&self) -> usize {
        self.fields().len()
    }

    /// Deserializes a value from the composite type.
    ///
    /// The value can be specified either by its numeric index, or by its field name.
    ///
    /// # Panics
    ///
    /// Panics if the index is out of bounds or if the value cannot be converted to the specified type.
    pub fn get<'b, I, T>(&'b self, idx: I) -> T
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'b>,
    {
        match self.get_inner(&idx) {
            Ok(ok) => ok,
            Err(err) => panic!("error retrieving column {}: {}", idx, err),
        }
    }

    /// Like `CompositeType::get`, but returns a `Result` rather than panicking.
    pub fn try_get<'b, I, T>(&'b self, idx: I) -> Result<T, Error>
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'b>,
    {
        self.get_inner(&idx)
    }

    fn get_inner<'b, I, T>(&'b self, idx: &I) -> Result<T, Error>
    where
        I: RowIndex + fmt::Display,
        T: FromSql<'b>,
    {
        let idx = match idx.__idx(self.fields()) {
            Some(idx) => idx,
            None => return Err(Error::column(idx.to_string())),
        };

        let ty = self.fields()[idx].type_();
        if !T::accepts(ty) {
            return Err(Error::from_sql(
                Box::new(WrongType::new::<T>(ty.clone())),
                idx,
            ));
        }

        let buf = self.ranges[idx].clone().map(|r| &self.body[r]);
        FromSql::from_sql_nullable(ty, buf).map_err(|e| Error::from_sql(e, idx))
    }
}

/// A row of data returned from the database by a simple query.
pub struct SimpleQueryRow {
    columns: Arc<[String]>,
    body: DataRowBody,
    ranges: Vec<Option<Range<usize>>>,
}

impl SimpleQueryRow {
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(columns: Arc<[String]>, body: DataRowBody) -> Result<SimpleQueryRow, Error> {
        let ranges = body.ranges().collect().map_err(Error::parse)?;
        Ok(SimpleQueryRow {
            columns,
            body,
            ranges,
        })
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

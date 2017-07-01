//! Postgres rows.

use postgres_shared::rows::RowData;
use postgres_shared::stmt::Column;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

#[doc(inline)]
pub use postgres_shared::rows::RowIndex;

use RowNew;
use types::{WrongType, FromSql};

/// A row from Postgres.
pub struct Row {
    columns: Arc<Vec<Column>>,
    data: RowData,
}

impl RowNew for Row {
    fn new(columns: Arc<Vec<Column>>, data: RowData) -> Row {
        Row {
            columns: columns,
            data: data,
        }
    }
}

impl Row {
    /// Returns information about the columns in the row.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    /// Returns the number of values in the row
    pub fn len(&self) -> usize {
        self.columns.len()
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
    pub fn get<T, I>(&self, idx: I) -> T
    where
        T: FromSql,
        I: RowIndex + fmt::Debug,
    {
        match self.try_get(&idx) {
            Ok(Some(v)) => v,
            Ok(None) => panic!("no such column {:?}", idx),
            Err(e) => panic!("error retrieving row {:?}: {}", idx, e),
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
    pub fn try_get<T, I>(&self, idx: I) -> Result<Option<T>, Box<Error + Sync + Send>>
    where
        T: FromSql,
        I: RowIndex,
    {
        let idx = match idx.idx(&self.columns) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let ty = self.columns[idx].type_();
        if !T::accepts(ty) {
            return Err(Box::new(WrongType::new(ty.clone())));
        }

        T::from_sql_nullable(ty, self.data.get(idx)).map(Some)
    }
}

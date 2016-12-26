use postgres_shared::{RowData, Column};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

#[doc(inline)]
pub use postgres_shared::RowIndex;

use RowNew;
use types::{WrongType, FromSql, SessionInfo};

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
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }

    pub fn len(&self) -> usize {
        self.columns.len()
    }

    pub fn get<T, I>(&self, idx: I) -> T
        where T: FromSql,
              I: RowIndex + fmt::Debug
    {
        match self.try_get(&idx) {
            Ok(Some(v)) => v,
            Ok(None) => panic!("no such column {:?}", idx),
            Err(e) => panic!("error retrieving row {:?}: {}", idx, e),
        }
    }

    pub fn try_get<T, I>(&self, idx: I) -> Result<Option<T>, Box<Error + Sync + Send>>
        where T: FromSql,
              I: RowIndex
    {
        let idx = match idx.idx(&self.columns) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let ty = self.columns[idx].type_();
        if !T::accepts(ty) {
            return Err(Box::new(WrongType::new(ty.clone())));
        }

        // FIXME
        T::from_sql_nullable(ty, self.data.get(idx), &SessionInfo::new(&HashMap::new())).map(Some)
    }
}

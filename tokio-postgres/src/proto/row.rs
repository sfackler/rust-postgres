use postgres_protocol::message::backend::DataRowBody;
use std::fmt;

use proto::statement::Statement;
use rows::{RowData, RowIndex};
use types::{FromSql, WrongType};
use {Column, Error};

pub struct Row {
    statement: Statement,
    data: RowData,
}

impl Row {
    pub fn new(statement: Statement, data: DataRowBody) -> Result<Row, Error> {
        let data = RowData::new(data).map_err(Error::parse)?;
        Ok(Row { statement, data })
    }

    pub fn columns(&self) -> &[Column] {
        self.statement.columns()
    }

    pub fn len(&self) -> usize {
        self.columns().len()
    }

    pub fn get<'b, I, T>(&'b self, idx: I) -> T
    where
        I: RowIndex + fmt::Debug,
        T: FromSql<'b>,
    {
        match self.get_inner(&idx) {
            Ok(Some(ok)) => ok,
            Err(err) => panic!("error retrieving column {:?}: {:?}", idx, err),
            Ok(None) => panic!("no such column {:?}", idx),
        }
    }

    pub fn try_get<'b, I, T>(&'b self, idx: I) -> Result<Option<T>, Error>
    where
        I: RowIndex,
        T: FromSql<'b>,
    {
        self.get_inner(&idx)
    }

    fn get_inner<'b, I, T>(&'b self, idx: &I) -> Result<Option<T>, Error>
    where
        I: RowIndex,
        T: FromSql<'b>,
    {
        let idx = match idx.__idx(&self.columns()) {
            Some(idx) => idx,
            None => return Ok(None),
        };

        let ty = self.statement.columns()[idx].type_();
        if !<T as FromSql>::accepts(ty) {
            return Err(Error::from_sql(Box::new(WrongType::new(ty.clone()))));
        }
        let value = FromSql::from_sql_nullable(ty, self.data.get(idx));
        value.map(Some).map_err(Error::from_sql)
    }
}

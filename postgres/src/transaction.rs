use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row};

use crate::{Client, Query, Statement};

pub struct Transaction<'a> {
    client: &'a mut Client,
    done: bool,
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if !self.done {
            let _ = self.rollback_inner();
        }
    }
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(client: &'a mut Client) -> Transaction<'a> {
        Transaction {
            client,
            done: false,
        }
    }

    pub fn commit(mut self) -> Result<(), Error> {
        self.done = true;
        self.client.batch_execute("COMMIT")
    }

    pub fn rollback(mut self) -> Result<(), Error> {
        self.done = true;
        self.rollback_inner()
    }

    fn rollback_inner(&mut self) -> Result<(), Error> {
        self.client.batch_execute("ROLLBACK")
    }

    pub fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.client.prepare(query)
    }

    pub fn prepare_typed(&mut self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        self.client.prepare_typed(query, types)
    }

    pub fn execute<T>(&mut self, query: &T, params: &[&dyn ToSql]) -> Result<u64, Error>
    where
        T: ?Sized + Query,
    {
        self.client.execute(query, params)
    }

    pub fn query<T>(&mut self, query: &T, params: &[&dyn ToSql]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + Query,
    {
        self.client.query(query, params)
    }

    pub fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        self.client.batch_execute(query)
    }
}

use crate::types::ToSql;
use crate::{Error, Row, Statement, ToStatement, Transaction};

/// A trait allowing abstraction over connections and transactions.
pub trait GenericClient {
    /// Like `Client::execute`.
    fn execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement;

    /// Like `Client::query`.
    fn query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement;

    /// Like `Client::prepare`.
    fn prepare(&mut self, query: &str) -> Result<Statement, Error>;

    /// Like `Client::transaction`.
    fn transaction(&mut self) -> Result<Transaction<'_>, Error>;
}

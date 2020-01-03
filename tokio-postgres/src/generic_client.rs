use crate::types::ToSql;
use crate::{Error, Row, Statement, ToStatement, Transaction};
use async_trait::async_trait;

/// A trait allowing abstraction over connections and transactions.
#[async_trait]
pub trait GenericClient {
    /// Like `Client::execute`.
    async fn execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like `Client::query`.
    async fn query<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like `Client::prepare`.
    async fn prepare(&mut self, query: &str) -> Result<Statement, Error>;

    /// Like `Client::transaction`.
    async fn transaction(&mut self) -> Result<Transaction<'_>, Error>;
}

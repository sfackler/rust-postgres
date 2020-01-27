use crate::query::RowStream;
use crate::types::{ToSql, Type};
use crate::{Error, Row, Statement, ToStatement, Transaction};
use async_trait::async_trait;

/// A trait allowing abstraction over connections and transactions.
#[async_trait]
pub trait GenericClient {
    /// Like `Client::execute`.
    async fn execute<T>(&self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like `Client::execute_raw`.
    async fn execute_raw<'b, I, T>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
        I: IntoIterator<Item = &'b dyn ToSql> + Sync + Send,
        I::IntoIter: ExactSizeIterator;

    /// Like `Client::query`.
    async fn query<T>(&self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like `Client::query_one`.
    async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like `Client::query_opt`.
    async fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement + Sync + Send;

    /// Like `Client::query_raw`.
    async fn query_raw<'b, T, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement + Sync + Send,
        I: IntoIterator<Item = &'b dyn ToSql> + Sync + Send,
        I::IntoIter: ExactSizeIterator;

    /// Like `Client::prepare`.
    async fn prepare(&self, query: &str) -> Result<Statement, Error>;

    /// Like `Client::prepare_typed`.
    async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error>;

    /// Like `Client::transaction`.
    async fn transaction(&mut self) -> Result<Transaction<'_>, Error>;
}

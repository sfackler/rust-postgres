use crate::{Statement, Transaction};
use tokio_postgres::types::{ToSql};
use tokio_postgres::{Error, Row};

/// A trait allowing abstraction over connections and transactions
pub trait GenericConnection {
  /// Like `Client::execute`.
  fn execute(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>;

  /// Like `Client::query`.
  fn query(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>;

  /// Like `Client::prepare`.
  fn prepare(&mut self, query: &str) -> Result<Statement, Error>;

  /// Like `Client::transaction`.
  fn transaction(&mut self) -> Result<Transaction<'_>, Error>;
}

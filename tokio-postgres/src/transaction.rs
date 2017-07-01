//! Transactions.

use futures::{Future, BoxFuture};
use futures_state_stream::{StateStream, BoxStateStream};

use {Connection, TransactionNew};
use error::Error;
use stmt::Statement;
use types::ToSql;
use rows::Row;

/// An in progress Postgres transaction.
#[derive(Debug)]
pub struct Transaction(Connection);

impl TransactionNew for Transaction {
    fn new(c: Connection) -> Transaction {
        Transaction(c)
    }
}

impl Transaction {
    /// Like `Connection::batch_execute`.
    pub fn batch_execute(self, query: &str) -> BoxFuture<Transaction, Error<Transaction>> {
        self.0
            .batch_execute(query)
            .map(Transaction)
            .map_err(transaction_err)
            .boxed()
    }

    /// Like `Connection::prepare`.
    pub fn prepare(self, query: &str) -> BoxFuture<(Statement, Transaction), Error<Transaction>> {
        self.0
            .prepare(query)
            .map(|(s, c)| (s, Transaction(c)))
            .map_err(transaction_err)
            .boxed()
    }

    /// Like `Connection::execute`.
    pub fn execute(
        self,
        statement: &Statement,
        params: &[&ToSql],
    ) -> BoxFuture<(u64, Transaction), Error<Transaction>> {
        self.0
            .execute(statement, params)
            .map(|(n, c)| (n, Transaction(c)))
            .map_err(transaction_err)
            .boxed()
    }

    /// Like `Connection::query`.
    pub fn query(
        self,
        statement: &Statement,
        params: &[&ToSql],
    ) -> BoxStateStream<Row, Transaction, Error<Transaction>> {
        self.0
            .query(statement, params)
            .map_state(Transaction)
            .map_err(transaction_err)
            .boxed()
    }

    /// Commits the transaction.
    pub fn commit(self) -> BoxFuture<Connection, Error> {
        self.finish("COMMIT")
    }

    /// Rolls back the transaction.
    pub fn rollback(self) -> BoxFuture<Connection, Error> {
        self.finish("ROLLBACK")
    }

    fn finish(self, query: &str) -> BoxFuture<Connection, Error> {
        self.0.simple_query(query).map(|(_, c)| c).boxed()
    }
}

fn transaction_err(e: Error) -> Error<Transaction> {
    match e {
        Error::Io(e) => Error::Io(e),
        Error::Db(e, c) => Error::Db(e, Transaction(c)),
        Error::Conversion(e, c) => Error::Conversion(e, Transaction(c)),
    }
}

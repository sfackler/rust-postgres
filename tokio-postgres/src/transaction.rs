//! Transactions.

use futures::Future;
use futures_state_stream::StateStream;

use {Connection, BoxedFuture, BoxedStateStream};
use error::Error;
use stmt::Statement;
use types::ToSql;
use rows::Row;

/// An in progress Postgres transaction.
#[derive(Debug)]
pub struct Transaction(Connection);

impl Transaction {
    pub(crate) fn new(c: Connection) -> Transaction {
        Transaction(c)
    }

    /// Like `Connection::batch_execute`.
    pub fn batch_execute(
        self,
        query: &str,
    ) -> Box<Future<Item = Transaction, Error = (Error, Transaction)> + Send> {
        self.0
            .batch_execute(query)
            .map(Transaction)
            .map_err(transaction_err)
            .boxed2()
    }

    /// Like `Connection::prepare`.
    pub fn prepare(
        self,
        query: &str,
    ) -> Box<Future<Item = (Statement, Transaction), Error = (Error, Transaction)> + Send> {
        self.0
            .prepare(query)
            .map(|(s, c)| (s, Transaction(c)))
            .map_err(transaction_err)
            .boxed2()
    }

    /// Like `Connection::execute`.
    pub fn execute(
        self,
        statement: &Statement,
        params: &[&ToSql],
    ) -> Box<Future<Item = (u64, Transaction), Error = (Error, Transaction)> + Send> {
        self.0
            .execute(statement, params)
            .map(|(n, c)| (n, Transaction(c)))
            .map_err(transaction_err)
            .boxed2()
    }

    /// Like `Connection::query`.
    pub fn query(
        self,
        statement: &Statement,
        params: &[&ToSql],
    ) -> Box<StateStream<Item = Row, State = Transaction, Error = Error> + Send> {
        self.0
            .query(statement, params)
            .map_state(Transaction)
            .boxed2()
    }

    /// Commits the transaction.
    pub fn commit(self) -> Box<Future<Item = Connection, Error = (Error, Connection)> + Send> {
        self.finish("COMMIT")
    }

    /// Rolls back the transaction.
    pub fn rollback(self) -> Box<Future<Item = Connection, Error = (Error, Connection)> + Send> {
        self.finish("ROLLBACK")
    }

    fn finish(
        self,
        query: &str,
    ) -> Box<Future<Item = Connection, Error = (Error, Connection)> + Send> {
        self.0.simple_query(query).map(|(_, c)| c).boxed2()
    }
}

fn transaction_err((e, c): (Error, Connection)) -> (Error, Transaction) {
    (e, Transaction(c))
}

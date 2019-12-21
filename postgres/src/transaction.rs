use crate::{CopyInWriter, CopyOutReader, GenericConnection, Portal, RowIter, Rt, Statement, ToStatement};
use tokio::runtime::Runtime;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row, SimpleQueryMessage};

/// A representation of a PostgreSQL database transaction.
///
/// Transactions will implicitly roll back by default when dropped. Use the `commit` method to commit the changes made
/// in the transaction. Transactions can be nested, with inner transactions implemented via savepoints.
pub struct Transaction<'a> {
    runtime: &'a mut Runtime,
    transaction: tokio_postgres::Transaction<'a>,
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(
        runtime: &'a mut Runtime,
        transaction: tokio_postgres::Transaction<'a>,
    ) -> Transaction<'a> {
        Transaction {
            runtime,
            transaction,
        }
    }

    fn rt(&mut self) -> Rt<'_> {
        Rt(self.runtime)
    }

    /// Consumes the transaction, committing all changes made within it.
    pub fn commit(self) -> Result<(), Error> {
        self.runtime.block_on(self.transaction.commit())
    }

    /// Rolls the transaction back, discarding all changes made within it.
    ///
    /// This is equivalent to `Transaction`'s `Drop` implementation, but provides any error encountered to the caller.
    pub fn rollback(self) -> Result<(), Error> {
        self.runtime.block_on(self.transaction.rollback())
    }

    /// Like `Client::prepare`.
    pub fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.runtime.block_on(self.transaction.prepare(query))
    }

    /// Like `Client::prepare_typed`.
    pub fn prepare_typed(&mut self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        self.runtime
            .block_on(self.transaction.prepare_typed(query, types))
    }

    /// Like `Client::execute`.
    pub fn execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.runtime
            .block_on(self.transaction.execute(query, params))
    }

    /// Like `Client::query`.
    pub fn query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.runtime.block_on(self.transaction.query(query, params))
    }

    /// Like `Client::query_one`.
    pub fn query_one<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.runtime
            .block_on(self.transaction.query_one(query, params))
    }

    /// Like `Client::query_opt`.
    pub fn query_opt<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.runtime
            .block_on(self.transaction.query_opt(query, params))
    }

    /// Like `Client::query_raw`.
    pub fn query_raw<'b, T, I>(&mut self, query: &T, params: I) -> Result<RowIter<'_>, Error>
    where
        T: ?Sized + ToStatement,
        I: IntoIterator<Item = &'b dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let stream = self
            .runtime
            .block_on(self.transaction.query_raw(query, params))?;
        Ok(RowIter::new(self.rt(), stream))
    }

    /// Binds parameters to a statement, creating a "portal".
    ///
    /// Portals can be used with the `query_portal` method to page through the results of a query without being forced
    /// to consume them all immediately.
    ///
    /// Portals are automatically closed when the transaction they were created in is closed.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    pub fn bind<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Portal, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.runtime.block_on(self.transaction.bind(query, params))
    }

    /// Continues execution of a portal, returning the next set of rows.
    ///
    /// Unlike `query`, portals can be incrementally evaluated by limiting the number of rows returned in each call to
    /// `query_portal`. If the requested number is negative or 0, all remaining rows will be returned.
    pub fn query_portal(&mut self, portal: &Portal, max_rows: i32) -> Result<Vec<Row>, Error> {
        self.runtime
            .block_on(self.transaction.query_portal(portal, max_rows))
    }

    /// The maximally flexible version of `query_portal`.
    pub fn query_portal_raw(
        &mut self,
        portal: &Portal,
        max_rows: i32,
    ) -> Result<RowIter<'_>, Error> {
        let stream = self
            .runtime
            .block_on(self.transaction.query_portal_raw(portal, max_rows))?;
        Ok(RowIter::new(self.rt(), stream))
    }

    /// Like `Client::copy_in`.
    pub fn copy_in<T>(&mut self, query: &T) -> Result<CopyInWriter<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let sink = self.runtime.block_on(self.transaction.copy_in(query))?;
        Ok(CopyInWriter::new(self.rt(), sink))
    }

    /// Like `Client::copy_out`.
    pub fn copy_out<T>(&mut self, query: &T) -> Result<CopyOutReader<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let stream = self.runtime.block_on(self.transaction.copy_out(query))?;
        Ok(CopyOutReader::new(self.rt(), stream))
    }

    /// Like `Client::simple_query`.
    pub fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.runtime.block_on(self.transaction.simple_query(query))
    }

    /// Like `Client::batch_execute`.
    pub fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        self.runtime.block_on(self.transaction.batch_execute(query))
    }

    /// Like `Client::transaction`.
    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        let transaction = self.runtime.block_on(self.transaction.transaction())?;
        Ok(Transaction {
            runtime: self.runtime,
            transaction,
        })
    }
}

impl<'a> GenericConnection for Transaction<'a> {
    fn execute(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error> {
        self.execute(query, params)
    }
    fn query(&mut self, query: &str, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error> {
        self.query(query, params)
    }
    fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.prepare(query)
    }
    fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.transaction()
    }
}

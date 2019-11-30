use crate::iter::Iter;
use crate::{CopyInWriter, CopyOutReader, Portal, Statement, ToStatement};
use fallible_iterator::FallibleIterator;
use futures::executor;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row, SimpleQueryMessage};

/// A representation of a PostgreSQL database transaction.
///
/// Transactions will implicitly roll back by default when dropped. Use the `commit` method to commit the changes made
/// in the transaction. Transactions can be nested, with inner transactions implemented via safepoints.
pub struct Transaction<'a>(tokio_postgres::Transaction<'a>);

impl<'a> Transaction<'a> {
    pub(crate) fn new(transaction: tokio_postgres::Transaction<'a>) -> Transaction<'a> {
        Transaction(transaction)
    }

    /// Consumes the transaction, committing all changes made within it.
    pub fn commit(self) -> Result<(), Error> {
        executor::block_on(self.0.commit())
    }

    /// Rolls the transaction back, discarding all changes made within it.
    ///
    /// This is equivalent to `Transaction`'s `Drop` implementation, but provides any error encountered to the caller.
    pub fn rollback(self) -> Result<(), Error> {
        executor::block_on(self.0.rollback())
    }

    /// Like `Client::prepare`.
    pub fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        executor::block_on(self.0.prepare(query))
    }

    /// Like `Client::prepare_typed`.
    pub fn prepare_typed(&mut self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        executor::block_on(self.0.prepare_typed(query, types))
    }

    /// Like `Client::execute`.
    pub fn execute<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        executor::block_on(self.0.execute(query, params))
    }

    /// Like `Client::query`.
    pub fn query<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        executor::block_on(self.0.query(query, params))
    }

    /// Like `Client::query_one`.
    pub fn query_one<T>(&mut self, query: &T, params: &[&(dyn ToSql + Sync)]) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement,
    {
        executor::block_on(self.0.query_one(query, params))
    }

    /// Like `Client::query_raw`.
    pub fn query_raw<'b, T, I>(
        &mut self,
        query: &T,
        params: I,
    ) -> Result<impl FallibleIterator<Item = Row, Error = Error>, Error>
    where
        T: ?Sized + ToStatement,
        I: IntoIterator<Item = &'b dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        let stream = executor::block_on(self.0.query_raw(query, params))?;
        Ok(Iter::new(stream))
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
        executor::block_on(self.0.bind(query, params))
    }

    /// Continues execution of a portal, returning the next set of rows.
    ///
    /// Unlike `query`, portals can be incrementally evaluated by limiting the number of rows returned in each call to
    /// `query_portal`. If the requested number is negative or 0, all remaining rows will be returned.
    pub fn query_portal(&mut self, portal: &Portal, max_rows: i32) -> Result<Vec<Row>, Error> {
        executor::block_on(self.0.query_portal(portal, max_rows))
    }

    /// The maximally flexible version of `query_portal`.
    pub fn query_portal_raw(
        &mut self,
        portal: &Portal,
        max_rows: i32,
    ) -> Result<impl FallibleIterator<Item = Row, Error = Error>, Error> {
        let stream = executor::block_on(self.0.query_portal_raw(portal, max_rows))?;
        Ok(Iter::new(stream))
    }

    /// Like `Client::copy_in`.
    pub fn copy_in<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<CopyInWriter<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let sink = executor::block_on(self.0.copy_in(query, params))?;
        Ok(CopyInWriter::new(sink))
    }

    /// Like `Client::copy_out`.
    pub fn copy_out<T>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<CopyOutReader<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let stream = executor::block_on(self.0.copy_out(query, params))?;
        CopyOutReader::new(stream)
    }

    /// Like `Client::simple_query`.
    pub fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        executor::block_on(self.0.simple_query(query))
    }

    /// Like `Client::batch_execute`.
    pub fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        executor::block_on(self.0.batch_execute(query))
    }

    /// Like `Client::transaction`.
    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        let transaction = executor::block_on(self.0.transaction())?;
        Ok(Transaction(transaction))
    }
}

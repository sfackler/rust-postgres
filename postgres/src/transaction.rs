use fallible_iterator::FallibleIterator;
use futures::executor;
use std::io::{BufRead, Read};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row, SimpleQueryMessage};

use crate::copy_in_stream::CopyInStream;
use crate::copy_out_reader::CopyOutReader;
use crate::iter::Iter;
use crate::{Portal, Statement, ToStatement};

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
        self.query_iter(query, params)?.collect()
    }

    /// Like `Client::query_iter`.
    pub fn query_iter<'b, T>(
        &'b mut self,
        query: &'b T,
        params: &'b [&(dyn ToSql + Sync)],
    ) -> Result<impl FallibleIterator<Item = Row, Error = Error> + 'b, Error>
    where
        T: ?Sized + ToStatement,
    {
        Ok(Iter::new(self.0.query(query, params)))
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
        self.query_portal_iter(portal, max_rows)?.collect()
    }

    /// Like `query_portal`, except that it returns a fallible iterator over the resulting rows rather than buffering
    /// the entire response in memory.
    pub fn query_portal_iter<'b>(
        &'b mut self,
        portal: &'b Portal,
        max_rows: i32,
    ) -> Result<impl FallibleIterator<Item = Row, Error = Error> + 'b, Error> {
        Ok(Iter::new(self.0.query_portal(&portal, max_rows)))
    }

    /// Like `Client::copy_in`.
    pub fn copy_in<T, R>(
        &mut self,
        query: &T,
        params: &[&(dyn ToSql + Sync)],
        reader: R,
    ) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
        R: Read + Unpin,
    {
        executor::block_on(self.0.copy_in(query, params, CopyInStream(reader)))
    }

    /// Like `Client::copy_out`.
    pub fn copy_out<'b, T>(
        &'b mut self,
        query: &'b T,
        params: &'b [&(dyn ToSql + Sync)],
    ) -> Result<impl BufRead + 'b, Error>
    where
        T: ?Sized + ToStatement,
    {
        let stream = self.0.copy_out(query, params);
        CopyOutReader::new(stream)
    }

    /// Like `Client::simple_query`.
    pub fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query_iter(query)?.collect()
    }

    /// Like `Client::simple_query_iter`.
    pub fn simple_query_iter<'b>(
        &'b mut self,
        query: &'b str,
    ) -> Result<impl FallibleIterator<Item = SimpleQueryMessage, Error = Error> + 'b, Error> {
        Ok(Iter::new(self.0.simple_query(query)))
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

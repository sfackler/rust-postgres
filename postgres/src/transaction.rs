use fallible_iterator::FallibleIterator;
use futures::Future;
use std::io::Read;
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row, SimpleQueryMessage};

use crate::{
    Client, CopyOutReader, Portal, QueryIter, QueryPortalIter, SimpleQueryIter, Statement,
    ToStatement,
};

/// A representation of a PostgreSQL database transaction.
///
/// Transactions will implicitly roll back by default when dropped. Use the `commit` method to commit the changes made
/// in the transaction. Transactions can be nested, with inner transactions implemented via safepoints.
pub struct Transaction<'a> {
    client: &'a mut Client,
    depth: u32,
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
            depth: 0,
            done: false,
        }
    }

    /// Consumes the transaction, committing all changes made within it.
    pub fn commit(mut self) -> Result<(), Error> {
        self.done = true;
        if self.depth == 0 {
            self.client.simple_query("COMMIT")?;
        } else {
            self.client
                .simple_query(&format!("RELEASE sp{}", self.depth))?;
        }
        Ok(())
    }

    /// Rolls the transaction back, discarding all changes made within it.
    ///
    /// This is equivalent to `Transaction`'s `Drop` implementation, but provides any error encountered to the caller.
    pub fn rollback(mut self) -> Result<(), Error> {
        self.done = true;
        self.rollback_inner()
    }

    fn rollback_inner(&mut self) -> Result<(), Error> {
        if self.depth == 0 {
            self.client.simple_query("ROLLBACK")?;
        } else {
            self.client
                .simple_query(&format!("ROLLBACK TO sp{}", self.depth))?;
        }
        Ok(())
    }

    //// Like `Client::prepare`.
    pub fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.client.prepare(query)
    }

    //// Like `Client::prepare_typed`.
    pub fn prepare_typed(&mut self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        self.client.prepare_typed(query, types)
    }

    //// Like `Client::execute`.
    pub fn execute<T>(&mut self, query: &T, params: &[&dyn ToSql]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.execute(query, params)
    }

    //// Like `Client::query`.
    pub fn query<T>(&mut self, query: &T, params: &[&dyn ToSql]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.query(query, params)
    }

    //// Like `Client::query_iter`.
    pub fn query_iter<T>(
        &mut self,
        query: &T,
        params: &[&dyn ToSql],
    ) -> Result<QueryIter<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.query_iter(query, params)
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
    pub fn bind<T>(&mut self, query: &T, params: &[&dyn ToSql]) -> Result<Portal, Error>
    where
        T: ?Sized + ToStatement,
    {
        let statement = query.__statement(&mut self.client)?;
        self.client.get_mut().bind(&statement, params).wait()
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
    pub fn query_portal_iter(
        &mut self,
        portal: &Portal,
        max_rows: i32,
    ) -> Result<QueryPortalIter<'_>, Error> {
        Ok(QueryPortalIter::new(
            self.client.get_mut().query_portal(&portal, max_rows),
        ))
    }

    /// Like `Client::copy_in`.
    pub fn copy_in<T, R>(
        &mut self,
        query: &T,
        params: &[&dyn ToSql],
        reader: R,
    ) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
        R: Read,
    {
        self.client.copy_in(query, params, reader)
    }

    /// Like `Client::copy_out`.
    pub fn copy_out<T>(
        &mut self,
        query: &T,
        params: &[&dyn ToSql],
    ) -> Result<CopyOutReader<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.client.copy_out(query, params)
    }

    /// Like `Client::simple_query`.
    pub fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.client.simple_query(query)
    }

    /// Like `Client::simple_query_iter`.
    pub fn simple_query_iter(&mut self, query: &str) -> Result<SimpleQueryIter<'_>, Error> {
        self.client.simple_query_iter(query)
    }

    /// Like `Client::transaction`.
    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        let depth = self.depth + 1;
        self.client
            .simple_query(&format!("SAVEPOINT sp{}", depth))?;
        Ok(Transaction {
            client: self.client,
            depth,
            done: false,
        })
    }
}

use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::copy_out::CopyOutStream;
use crate::query::RowStream;
#[cfg(feature = "runtime")]
use crate::tls::MakeTlsConnect;
use crate::tls::TlsConnect;
use crate::types::{BorrowToSql, ToSql, Type};
#[cfg(feature = "runtime")]
use crate::Socket;
use crate::{
    bind, query, slice_iter, CancelToken, Client, CopyInSink, Error, FromRow, Portal, Row,
    SimpleQueryMessage, Statement, ToStatement,
};
use bytes::Buf;
use futures_util::{stream::BoxStream, TryStreamExt};
use postgres_protocol::message::frontend;
use postgres_types::FromSqlOwned;
use std::fmt;
use std::ops::Deref;
use tokio::io::{AsyncRead, AsyncWrite};

/// A representation of a PostgreSQL database transaction.
///
/// Transactions will implicitly roll back when dropped. Use the `commit` method to commit the changes made in the
/// transaction. Transactions can be nested, with inner transactions implemented via safepoints.
pub struct Transaction<'a> {
    client: &'a mut Client,
    savepoint: Option<Savepoint>,
    done: bool,
}

impl<'a> fmt::Debug for Transaction<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Transaction")
            .field("savepoint", &self.savepoint)
            .field("done", &self.done)
            .finish()
    }
}

/// A representation of a PostgreSQL database savepoint.
#[derive(Debug)]
struct Savepoint {
    name: String,
    depth: u32,
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if self.done {
            return;
        }

        let query = if let Some(sp) = self.savepoint.as_ref() {
            format!("ROLLBACK TO {}", sp.name)
        } else {
            "ROLLBACK".to_string()
        };
        let buf = self.client.inner().with_buf(|buf| {
            frontend::query(&query, buf).unwrap();
            buf.split().freeze()
        });
        let _ = self
            .client
            .inner()
            .send(RequestMessages::Single(FrontendMessage::Raw(buf)));
    }
}

impl<'a> Transaction<'a> {
    pub(crate) fn new(client: &'a mut Client) -> Transaction<'a> {
        Transaction {
            client,
            savepoint: None,
            done: false,
        }
    }

    /// Consumes the transaction, committing all changes made within it.
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn commit(mut self) -> Result<(), Error> {
        self.done = true;
        let query = if let Some(sp) = self.savepoint.as_ref() {
            format!("RELEASE {}", sp.name)
        } else {
            "COMMIT".to_string()
        };
        self.client.batch_execute(&query).await
    }

    /// Rolls the transaction back, discarding all changes made within it.
    ///
    /// This is equivalent to `Transaction`'s `Drop` implementation, but provides any error encountered to the caller.
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn rollback(mut self) -> Result<(), Error> {
        self.done = true;
        let query = if let Some(sp) = self.savepoint.as_ref() {
            format!("ROLLBACK TO {}", sp.name)
        } else {
            "ROLLBACK".to_string()
        };
        self.client.batch_execute(&query).await
    }

    /// Like [`Client::prepare`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn prepare(&self, query: &str) -> Result<Statement, Error> {
        self.client.prepare(query).await
    }

    /// Like [`Client::prepare_typed`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn prepare_typed(
        &self,
        query: &str,
        parameter_types: &[Type],
    ) -> Result<Statement, Error> {
        self.client.prepare_typed(query, parameter_types).await
    }

    /// Like [`Client::query`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn query<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.query(statement, params).await
    }

    /// Like [`Client::query_as`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn query_as<T, R: FromRow>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<R>, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.query_as(statement, params).await
    }

    /// Like [`Client::query_scalar`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn query_scalar<T, R: FromSqlOwned>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Vec<R>, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.query_scalar(statement, params).await
    }

    /// Like [`Client::query_one`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn query_one<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Row, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.query_one(statement, params).await
    }

    /// Like [`Client::query_one_as`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn query_one_as<T, R: FromRow>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<R, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.query_one_as(statement, params).await
    }

    /// Like [`Client::query_one_scalar`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn query_one_scalar<T, R: FromSqlOwned>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<R, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.query_one_scalar(statement, params).await
    }

    /// Like [`Client::query_opt`].
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn query_opt<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<Row>, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.query_opt(statement, params).await
    }

    /// Like [`Client::query_opt_as`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn query_opt_as<T, R: FromRow>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<R>, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.query_opt_as(statement, params).await
    }

    /// Like [`Client::query_opt_scalar`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn query_opt_scalar<T, R: FromSqlOwned>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Option<R>, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.query_opt_scalar(statement, params).await
    }

    /// Like [`Client::query_raw`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn query_raw<T, P, I>(&self, statement: &T, params: I) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        self.client.query_raw(statement, params).await
    }

    /// Like [`Client::stream`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn stream<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<RowStream, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.stream(statement, params).await
    }

    /// Like [`Client::stream_as`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn stream_as<T, R: FromRow>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<BoxStream<'static, Result<R, Error>>, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.stream_as(statement, params).await
    }

    /// Like [`Client::execute`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn execute<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.execute(statement, params).await
    }

    /// Like [`Client::execute_raw`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn execute_raw<P, I, T>(&self, statement: &T, params: I) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        self.client.execute_raw(statement, params).await
    }

    /// Binds a statement to a set of parameters, creating a `Portal` which can be incrementally queried.
    ///
    /// Portals only last for the duration of the transaction in which they are created, and can only be used on the
    /// connection that created them.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn bind<T>(
        &self,
        statement: &T,
        params: &[&(dyn ToSql + Sync)],
    ) -> Result<Portal, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.bind_raw(statement, slice_iter(params)).await
    }

    /// A maximally flexible version of [`bind`].
    ///
    /// [`bind`]: #method.bind
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn bind_raw<P, T, I>(&self, statement: &T, params: I) -> Result<Portal, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
        P: BorrowToSql,
        I: IntoIterator<Item = P>,
        I::IntoIter: ExactSizeIterator,
    {
        let statement = statement.__convert().into_statement(self.client).await?;
        bind::bind(self.client.inner(), statement, params).await
    }

    /// Continues execution of a portal, returning a stream of the resulting rows.
    ///
    /// Unlike `query`, portals can be incrementally evaluated by limiting the number of rows returned in each call to
    /// `query_portal`. If the requested number is negative or 0, all rows will be returned.
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn query_portal(&self, portal: &Portal, max_rows: i32) -> Result<Vec<Row>, Error> {
        self.query_portal_raw(portal, max_rows)
            .await?
            .try_collect()
            .await
    }

    /// The maximally flexible version of [`query_portal`].
    ///
    /// [`query_portal`]: #method.query_portal
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn query_portal_raw(
        &self,
        portal: &Portal,
        max_rows: i32,
    ) -> Result<RowStream, Error> {
        query::query_portal(self.client.inner(), portal, max_rows).await
    }

    /// Like [`Client::copy_in`]
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn copy_in<T, U>(&self, statement: &T) -> Result<CopyInSink<U>, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
        U: Buf + 'static + Send,
    {
        self.client.copy_in(statement).await
    }

    /// Like `Client::copy_out`.
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn copy_out<T>(&self, statement: &T) -> Result<CopyOutStream, Error>
    where
        T: ?Sized + ToStatement + fmt::Debug,
    {
        self.client.copy_out(statement).await
    }

    /// Like `Client::simple_query`.
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn simple_query(&self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.client.simple_query(query).await
    }

    /// Like `Client::batch_execute`.
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn batch_execute(&self, query: &str) -> Result<(), Error> {
        self.client.batch_execute(query).await
    }

    /// Like `Client::cancel_token`.
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub fn cancel_token(&self) -> CancelToken {
        self.client.cancel_token()
    }

    /// Like `Client::cancel_query`.
    #[cfg(feature = "runtime")]
    #[deprecated(since = "0.6.0", note = "use Transaction::cancel_token() instead")]
    pub async fn cancel_query<T>(&self, tls: T) -> Result<(), Error>
    where
        T: MakeTlsConnect<Socket> + fmt::Debug,
    {
        #[allow(deprecated)]
        self.client.cancel_query(tls).await
    }

    /// Like `Client::cancel_query_raw`.
    #[deprecated(since = "0.6.0", note = "use Transaction::cancel_token() instead")]
    pub async fn cancel_query_raw<S, T>(&self, stream: S, tls: T) -> Result<(), Error>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        T: TlsConnect<S> + fmt::Debug,
    {
        #[allow(deprecated)]
        self.client.cancel_query_raw(stream, tls).await
    }

    /// Like `Client::transaction`, but creates a nested transaction via a savepoint.
    pub async fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self._savepoint(None).await
    }

    /// Like `Client::transaction`, but creates a nested transaction via a savepoint with the specified name.
    #[cfg_attr(feature = "tracing", tracing::instrument)]
    pub async fn savepoint<I>(&mut self, name: I) -> Result<Transaction<'_>, Error>
    where
        I: Into<String> + fmt::Debug,
    {
        self._savepoint(Some(name.into())).await
    }

    async fn _savepoint(&mut self, name: Option<String>) -> Result<Transaction<'_>, Error> {
        let depth = self.savepoint.as_ref().map_or(0, |sp| sp.depth) + 1;
        let name = name.unwrap_or_else(|| format!("sp_{}", depth));
        let query = format!("SAVEPOINT {}", name);
        self.batch_execute(&query).await?;

        Ok(Transaction {
            client: self.client,
            savepoint: Some(Savepoint { name, depth }),
            done: false,
        })
    }

    /// Returns a reference to the underlying `Client`.
    pub fn client(&self) -> &Client {
        self.client
    }
}

impl<'a> Deref for Transaction<'a> {
    type Target = Client;

    fn deref(&self) -> &Self::Target {
        self.client
    }
}

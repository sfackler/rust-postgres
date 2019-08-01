use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
#[cfg(feature = "runtime")]
use crate::tls::MakeTlsConnect;
use crate::tls::TlsConnect;
use crate::types::{ToSql, Type};
#[cfg(feature = "runtime")]
use crate::Socket;
use crate::{query, Client, Error, Row, SimpleQueryMessage, Statement};
use bytes::{Bytes, IntoBuf};
use futures::{Stream, TryStream};
use postgres_protocol::message::frontend;
use std::error;
use std::future::Future;
use tokio::io::{AsyncRead, AsyncWrite};

/// A representation of a PostgreSQL database transaction.
///
/// Transactions will implicitly roll back when dropped. Use the `commit` method to commit the changes made in the
/// transaction. Transactions can be nested, with inner transactions implemented via safepoints.
pub struct Transaction<'a> {
    client: &'a mut Client,
    done: bool,
}

impl<'a> Drop for Transaction<'a> {
    fn drop(&mut self) {
        if self.done {
            return;
        }

        let mut buf = vec![];
        frontend::query("ROLLBACK", &mut buf).unwrap();
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
            done: false,
        }
    }

    /// Consumes the transaction, committing all changes made within it.
    pub async fn commit(mut self) -> Result<(), Error> {
        self.done = true;
        self.client.batch_execute("COMMIT").await
    }

    /// Rolls the transaction back, discarding all changes made within it.
    ///
    /// This is equivalent to `Transaction`'s `Drop` implementation, but provides any error encountered to the caller.
    pub async fn rollback(mut self) -> Result<(), Error> {
        self.done = true;
        self.client.batch_execute("ROLLBACK").await
    }

    /// Like `Client::prepare`.
    pub fn prepare(&mut self, query: &str) -> impl Future<Output = Result<Statement, Error>> {
        self.client.prepare(query)
    }

    /// Like `Client::prepare_typed`.
    pub fn prepare_typed(
        &mut self,
        query: &str,
        parameter_types: &[Type],
    ) -> impl Future<Output = Result<Statement, Error>> {
        self.client.prepare_typed(query, parameter_types)
    }

    /// Like `Client::query`.
    pub fn query(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Stream<Item = Result<Row, Error>> {
        self.client.query(statement, params)
    }

    /// Like `Client::query_iter`.
    pub fn query_iter<'b, I>(
        &mut self,
        statement: &Statement,
        params: I,
    ) -> impl Stream<Item = Result<Row, Error>> + 'static
    where
        I: IntoIterator<Item = &'b dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        // https://github.com/rust-lang/rust/issues/63032
        let buf = query::encode(statement, params);
        query::query(self.client.inner(), statement.clone(), buf)
    }

    /// Like `Client::execute`.
    pub fn execute(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Future<Output = Result<u64, Error>> {
        self.client.execute(statement, params)
    }

    /// Like `Client::execute_iter`.
    pub fn execute_iter<'b, I>(
        &mut self,
        statement: &Statement,
        params: I,
    ) -> impl Future<Output = Result<u64, Error>>
    where
        I: IntoIterator<Item = &'b dyn ToSql>,
        I::IntoIter: ExactSizeIterator,
    {
        // https://github.com/rust-lang/rust/issues/63032
        let buf = query::encode(statement, params);
        query::execute(self.client.inner(), buf)
    }

    /// Like `Client::copy_in`.
    pub fn copy_in<S>(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
        stream: S,
    ) -> impl Future<Output = Result<u64, Error>>
    where
        S: TryStream,
        S::Ok: IntoBuf,
        <S::Ok as IntoBuf>::Buf: 'static + Send,
        S::Error: Into<Box<dyn error::Error + Sync + Send>>,
    {
        self.client.copy_in(statement, params, stream)
    }

    /// Like `Client::copy_out`.
    pub fn copy_out(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> impl Stream<Item = Result<Bytes, Error>> {
        self.client.copy_out(statement, params)
    }

    /// Like `Client::simple_query`.
    pub fn simple_query(
        &mut self,
        query: &str,
    ) -> impl Stream<Item = Result<SimpleQueryMessage, Error>> {
        self.client.simple_query(query)
    }

    /// Like `Client::batch_execute`.
    pub fn batch_execute(&mut self, query: &str) -> impl Future<Output = Result<(), Error>> {
        self.client.batch_execute(query)
    }

    /// Like `Client::cancel_query`.
    #[cfg(feature = "runtime")]
    pub fn cancel_query<T>(&mut self, tls: T) -> impl Future<Output = Result<(), Error>>
    where
        T: MakeTlsConnect<Socket>,
    {
        self.client.cancel_query(tls)
    }

    /// Like `Client::cancel_query_raw`.
    pub fn cancel_query_raw<S, T>(
        &mut self,
        stream: S,
        tls: T,
    ) -> impl Future<Output = Result<(), Error>>
    where
        S: AsyncRead + AsyncWrite + Unpin,
        T: TlsConnect<S>,
    {
        self.client.cancel_query_raw(stream, tls)
    }
}

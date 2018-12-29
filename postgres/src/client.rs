use futures::{Async, Future, Poll, Stream};
use std::io::{self, Read};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::Error;
#[cfg(feature = "runtime")]
use tokio_postgres::{MakeTlsMode, Socket, TlsMode};

#[cfg(feature = "runtime")]
use crate::Builder;
use crate::{CopyOutReader, Query, Statement, ToStatement, Transaction};

pub struct Client(tokio_postgres::Client);

impl Client {
    #[cfg(feature = "runtime")]
    pub fn connect<T>(params: &str, tls_mode: T) -> Result<Client, Error>
    where
        T: MakeTlsMode<Socket> + 'static + Send,
        T::TlsMode: Send,
        T::Stream: Send,
        T::Future: Send,
        <T::TlsMode as TlsMode<Socket>>::Future: Send,
    {
        params.parse::<Builder>()?.connect(tls_mode)
    }

    #[cfg(feature = "runtime")]
    pub fn builder() -> Builder {
        Builder::new()
    }

    pub fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.0.prepare(query).wait().map(Statement)
    }

    pub fn prepare_typed(&mut self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        self.0.prepare_typed(query, types).wait().map(Statement)
    }

    pub fn execute<T>(&mut self, query: &T, params: &[&dyn ToSql]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        let statement = query.__statement(self)?;
        self.0.execute(&statement.0, params).wait()
    }

    pub fn query<T>(&mut self, query: &T, params: &[&dyn ToSql]) -> Result<Query<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let statement = query.__statement(self)?;
        Ok(Query::new(self.0.query(&statement.0, params)))
    }

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
        let statement = query.__statement(self)?;
        self.0
            .copy_in(&statement.0, params, CopyInStream(reader))
            .wait()
    }

    pub fn copy_out<T>(
        &mut self,
        query: &T,
        params: &[&dyn ToSql],
    ) -> Result<CopyOutReader<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let statement = query.__statement(self)?;
        let stream = self.0.copy_out(&statement.0, params);
        CopyOutReader::new(stream)
    }

    pub fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        self.0.batch_execute(query).wait()
    }

    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.batch_execute("BEGIN")?;
        Ok(Transaction::new(self))
    }

    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    pub fn get_ref(&self) -> &tokio_postgres::Client {
        &self.0
    }

    pub fn get_mut(&mut self) -> &mut tokio_postgres::Client {
        &mut self.0
    }

    pub fn into_inner(self) -> tokio_postgres::Client {
        self.0
    }
}

impl From<tokio_postgres::Client> for Client {
    fn from(c: tokio_postgres::Client) -> Client {
        Client(c)
    }
}

struct CopyInStream<R>(R);

impl<R> Stream for CopyInStream<R>
where
    R: Read,
{
    type Item = Vec<u8>;
    type Error = io::Error;

    fn poll(&mut self) -> Poll<Option<Vec<u8>>, io::Error> {
        let mut buf = vec![];
        match self.0.by_ref().take(4096).read_to_end(&mut buf)? {
            0 => Ok(Async::Ready(None)),
            _ => Ok(Async::Ready(Some(buf))),
        }
    }
}

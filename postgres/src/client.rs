use futures::{Future, Stream};
use tokio_postgres::types::{ToSql, Type};
use tokio_postgres::{Error, Row};
#[cfg(feature = "runtime")]
use tokio_postgres::{MakeTlsMode, Socket, TlsMode};

#[cfg(feature = "runtime")]
use crate::Builder;
use crate::{Statement, Transaction};

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

    pub fn execute(&mut self, statement: &Statement, params: &[&dyn ToSql]) -> Result<u64, Error> {
        self.0.execute(&statement.0, params).wait()
    }

    pub fn query(
        &mut self,
        statement: &Statement,
        params: &[&dyn ToSql],
    ) -> Result<Vec<Row>, Error> {
        self.0.query(&statement.0, params).collect().wait()
    }

    pub fn batch_execute(&mut self, query: &str) -> Result<(), Error> {
        self.0.batch_execute(query).wait()
    }

    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.batch_execute("BEGIN")?;
        Ok(Transaction::new(self))
    }
}

impl From<tokio_postgres::Client> for Client {
    fn from(c: tokio_postgres::Client) -> Client {
        Client(c)
    }
}

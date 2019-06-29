use fallible_iterator::FallibleIterator;
use futures::{Async, Future, Poll, Stream};
use std::io::{self, Read};
use tokio_postgres::tls::{MakeTlsConnect, TlsConnect};
use tokio_postgres::types::{ToSql, Type};
#[cfg(feature = "runtime")]
use tokio_postgres::Socket;
use tokio_postgres::{Error, Row, SimpleQueryMessage};

#[cfg(feature = "runtime")]
use crate::Config;
use crate::{CopyOutReader, QueryIter, SimpleQueryIter, Statement, ToStatement, Transaction};

/// A synchronous PostgreSQL client.
///
/// This is a lightweight wrapper over the asynchronous tokio_postgres `Client`.
pub struct Client(tokio_postgres::Client);

impl Client {
    /// A convenience function which parses a configuration string into a `Config` and then connects to the database.
    ///
    /// See the documentation for [`Config`] for information about the connection syntax.
    ///
    /// Requires the `runtime` Cargo feature (enabled by default).
    ///
    /// [`Config`]: config/struct.Config.html
    #[cfg(feature = "runtime")]
    pub fn connect<T>(params: &str, tls_mode: T) -> Result<Client, Error>
    where
        T: MakeTlsConnect<Socket> + 'static + Send,
        T::TlsConnect: Send,
        T::Stream: Send,
        <T::TlsConnect as TlsConnect<Socket>>::Future: Send,
    {
        params.parse::<Config>()?.connect(tls_mode)
    }

    /// Returns a new `Config` object which can be used to configure and connect to a database.
    ///
    /// Requires the `runtime` Cargo feature (enabled by default).
    #[cfg(feature = "runtime")]
    pub fn configure() -> Config {
        Config::new()
    }

    /// Executes a statement, returning the number of rows modified.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// If the statement does not modify any rows (e.g. `SELECT`), 0 is returned.
    ///
    /// The `query` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let bar = 1i32;
    /// let baz = true;
    /// let rows_updated = client.execute(
    ///     "UPDATE foo SET bar = $1 WHERE baz = $2",
    ///     &[&bar, &baz],
    /// )?;
    ///
    /// println!("{} rows updated", rows_updated);
    /// # Ok(())
    /// # }
    /// ```
    pub fn execute<T>(&mut self, query: &T, params: &[&dyn ToSql]) -> Result<u64, Error>
    where
        T: ?Sized + ToStatement,
    {
        let statement = query.__statement(self)?;
        self.0.execute(&statement, params).wait()
    }

    /// Executes a statement, returning the resulting rows.
    ///
    /// A statement may contain parameters, specified by `$n`, where `n` is the index of the parameter of the list
    /// provided, 1-indexed.
    ///
    /// The `query` argument can either be a `Statement`, or a raw query string. If the same statement will be
    /// repeatedly executed (perhaps with different query parameters), consider preparing the statement up front
    /// with the `prepare` method.
    ///
    /// The `query_iter` method can be used to avoid buffering all rows in memory at once.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let baz = true;
    /// for row in client.query("SELECT foo FROM bar WHERE baz = $1", &[&baz])? {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query<T>(&mut self, query: &T, params: &[&dyn ToSql]) -> Result<Vec<Row>, Error>
    where
        T: ?Sized + ToStatement,
    {
        self.query_iter(query, params)?.collect()
    }

    /// Like `query`, except that it returns a fallible iterator over the resulting rows rather than buffering the
    /// response in memory.
    ///
    /// # Panics
    ///
    /// Panics if the number of parameters provided does not match the number expected.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    /// use fallible_iterator::FallibleIterator;
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let baz = true;
    /// let mut it = client.query_iter("SELECT foo FROM bar WHERE baz = $1", &[&baz])?;
    ///
    /// while let Some(row) = it.next()? {
    ///     let foo: i32 = row.get("foo");
    ///     println!("foo: {}", foo);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn query_iter<T>(
        &mut self,
        query: &T,
        params: &[&dyn ToSql],
    ) -> Result<QueryIter<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let statement = query.__statement(self)?;
        Ok(QueryIter::new(self.0.query(&statement, params)))
    }

    /// Creates a new prepared statement.
    ///
    /// Prepared statements can be executed repeatedly, and may contain query parameters (indicated by `$1`, `$2`, etc),
    /// which are set when executed. Prepared statements can only be used with the connection that created them.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let statement = client.prepare("SELECT name FROM people WHERE id = $1")?;
    ///
    /// for id in 0..10 {
    ///     let rows = client.query(&statement, &[&id])?;
    ///     let name: &str = rows[0].get(0);
    ///     println!("name: {}", name);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn prepare(&mut self, query: &str) -> Result<Statement, Error> {
        self.0.prepare(query).wait()
    }

    /// Like `prepare`, but allows the types of query parameters to be explicitly specified.
    ///
    /// The list of types may be smaller than the number of parameters - the types of the remaining parameters will be
    /// inferred. For example, `client.prepare_typed(query, &[])` is equivalent to `client.prepare(query)`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    /// use postgres::types::Type;
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let statement = client.prepare_typed(
    ///     "SELECT name FROM people WHERE id = $1",
    ///     &[Type::INT8],
    /// )?;
    ///
    /// for id in 0..10 {
    ///     let rows = client.query(&statement, &[&id])?;
    ///     let name: &str = rows[0].get(0);
    ///     println!("name: {}", name);
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub fn prepare_typed(&mut self, query: &str, types: &[Type]) -> Result<Statement, Error> {
        self.0.prepare_typed(query, types).wait()
    }

    /// Executes a `COPY FROM STDIN` statement, returning the number of rows created.
    ///
    /// The `query` argument can either be a `Statement`, or a raw query string. The data in the provided reader is
    /// passed along to the server verbatim; it is the caller's responsibility to ensure it uses the proper format.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// client.copy_in("COPY people FROM stdin", &[], &mut "1\tjohn\n2\tjane\n".as_bytes())?;
    /// # Ok(())
    /// # }
    /// ```
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
            .copy_in(&statement, params, CopyInStream(reader))
            .wait()
    }

    /// Executes a `COPY TO STDOUT` statement, returning a reader of the resulting data.
    ///
    /// The `query` argument can either be a `Statement`, or a raw query string.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    /// use std::io::Read;
    ///
    /// # fn main() -> Result<(), Box<std::error::Error>> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let mut reader = client.copy_out("COPY people TO stdout", &[])?;
    /// let mut buf = vec![];
    /// reader.read_to_end(&mut buf)?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn copy_out<T>(
        &mut self,
        query: &T,
        params: &[&dyn ToSql],
    ) -> Result<CopyOutReader<'_>, Error>
    where
        T: ?Sized + ToStatement,
    {
        let statement = query.__statement(self)?;
        let stream = self.0.copy_out(&statement, params);
        CopyOutReader::new(stream)
    }

    /// Executes a sequence of SQL statements using the simple query protocol.
    ///
    /// Statements should be separated by semicolons. If an error occurs, execution of the sequence will stop at that
    /// point. The simple query protocol returns the values in rows as strings rather than in their binary encodings,
    /// so the associated row type doesn't work with the `FromSql` trait. Rather than simply returning the rows, this
    /// method returns a sequence of an enum which indicates either the completion of one of the commands, or a row of
    /// data. This preserves the framing between the separate statements in the request.
    ///
    /// This is a simple convenience method over `simple_query_iter`.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely imbed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub fn simple_query(&mut self, query: &str) -> Result<Vec<SimpleQueryMessage>, Error> {
        self.simple_query_iter(query)?.collect()
    }

    /// Like `simple_query`, except that it returns a fallible iterator over the resulting values rather than buffering
    /// the response in memory.
    ///
    /// # Warning
    ///
    /// Prepared statements should be use for any query which contains user-specified data, as they provided the
    /// functionality to safely imbed that data in the request. Do not form statements via string concatenation and pass
    /// them to this method!
    pub fn simple_query_iter(&mut self, query: &str) -> Result<SimpleQueryIter<'_>, Error> {
        Ok(SimpleQueryIter::new(self.0.simple_query(query)))
    }

    /// Begins a new database transaction.
    ///
    /// The transaction will roll back by default - use the `commit` method to commit it.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// use postgres::{Client, NoTls};
    ///
    /// # fn main() -> Result<(), postgres::Error> {
    /// let mut client = Client::connect("host=localhost user=postgres", NoTls)?;
    ///
    /// let mut transaction = client.transaction()?;
    /// transaction.execute("UPDATE foo SET bar = 10", &[])?;
    /// // ...
    ///
    /// transaction.commit()?;
    /// # Ok(())
    /// # }
    /// ```
    pub fn transaction(&mut self) -> Result<Transaction<'_>, Error> {
        self.simple_query("BEGIN")?;
        Ok(Transaction::new(self))
    }

    /// Determines if the client's connection has already closed.
    ///
    /// If this returns `true`, the client is no longer usable.
    pub fn is_closed(&self) -> bool {
        self.0.is_closed()
    }

    /// Returns a shared reference to the inner nonblocking client.
    pub fn get_ref(&self) -> &tokio_postgres::Client {
        &self.0
    }

    /// Returns a mutable reference to the inner nonblocking client.
    pub fn get_mut(&mut self) -> &mut tokio_postgres::Client {
        &mut self.0
    }

    /// Consumes the client, returning the inner nonblocking client.
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

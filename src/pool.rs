//! A simple connection pool

use std::cast;
use sync::{Arc, Mutex};

use {PostgresNotifications,
     PostgresCancelData,
     PostgresConnection,
     NormalPostgresStatement,
     PostgresTransaction,
     SslMode};
use error::{PostgresConnectError, PostgresError};
use types::ToSql;

struct InnerConnectionPool {
    url: ~str,
    ssl: SslMode,
    // Actually Vec<~PostgresConnection>
    pool: Vec<*()>,
}

impl Drop for InnerConnectionPool {
    fn drop(&mut self) {
        loop {
            match self.pool.pop() {
                Some(conn) => unsafe {
                    drop(cast::transmute::<*(), ~PostgresConnection>(conn));
                },
                None => break
            }
        }
    }
}

impl InnerConnectionPool {
    fn new_connection(&mut self) -> Option<PostgresConnectError> {
        match PostgresConnection::try_connect(self.url, &self.ssl) {
            Ok(conn) => {
                unsafe { self.pool.push(cast::transmute(~conn)) };
                None
            }
            Err(err) => Some(err)
        }
    }
}

/// A simple fixed-size Postgres connection pool.
///
/// It can be shared across tasks.
///
/// # Example
///
/// ```rust,no_run
/// # use postgres::NoSsl;
/// # use postgres::pool::PostgresConnectionPool;
/// let pool = PostgresConnectionPool::new("postgres://postgres@localhost",
///                                        NoSsl, 5);
/// for _ in range(0, 10) {
///     let pool = pool.clone();
///     spawn(proc() {
///         let conn = pool.get_connection();
///         conn.execute("UPDATE foo SET bar = 1", []);
///     });
/// }
/// ```
#[deriving(Clone)]
pub struct PostgresConnectionPool {
    priv pool: Arc<Mutex<InnerConnectionPool>>
}

impl PostgresConnectionPool {
    /// Attempts to create a new pool with the specified number of connections.
    ///
    /// Returns an error if the specified number of connections cannot be
    /// created.
    pub fn try_new(url: &str, ssl: SslMode, pool_size: uint)
            -> Result<PostgresConnectionPool, PostgresConnectError> {
        let mut pool = InnerConnectionPool {
            url: url.to_owned(),
            ssl: ssl,
            pool: Vec::new(),
        };

        for _ in range(0, pool_size) {
            match pool.new_connection() {
                None => (),
                Some(err) => return Err(err)
            }
        }

        Ok(PostgresConnectionPool {
            pool: Arc::new(Mutex::new(pool))
        })
    }

    /// A convenience function wrapping `try_new`.
    ///
    /// # Failure
    ///
    /// Fails if the pool cannot be created.
    pub fn new(url: &str, ssl: SslMode, pool_size: uint)
            -> PostgresConnectionPool {
        match PostgresConnectionPool::try_new(url, ssl, pool_size) {
            Ok(pool) => pool,
            Err(err) => fail!("Unable to initialize pool: {}", err)
        }
    }

    /// Retrieves a connection from the pool.
    ///
    /// If all connections are in use, blocks until one becomes available.
    pub fn get_connection(&self) -> PooledPostgresConnection {
        let mut pool = self.pool.lock();

        while pool.pool.is_empty() {
            pool.cond.wait();
        }

        PooledPostgresConnection {
            pool: self.clone(),
            conn: Some(unsafe { cast::transmute(pool.pool.pop().unwrap()) })
        }
    }
}

/// A Postgres connection pulled from a connection pool.
///
/// It will be returned to the pool when it falls out of scope, even due to
/// task failure.
pub struct PooledPostgresConnection {
    priv pool: PostgresConnectionPool,
    // TODO remove the Option wrapper when drop takes self by value
    priv conn: Option<~PostgresConnection>
}

impl Drop for PooledPostgresConnection {
    fn drop(&mut self) {
        let conn = unsafe { cast::transmute(self.conn.take_unwrap()) };
        self.pool.pool.lock().pool.push(conn);
    }
}

impl PooledPostgresConnection {
    /// Like `PostgresConnection::try_prepare`.
    pub fn try_prepare<'a>(&'a self, query: &str)
            -> Result<NormalPostgresStatement<'a>, PostgresError> {
        self.conn.get_ref().try_prepare(query)
    }

    /// Like `PostgresConnection::prepare`.
    pub fn prepare<'a>(&'a self, query: &str) -> NormalPostgresStatement<'a> {
        self.conn.get_ref().prepare(query)
    }

    /// Like `PostgresConnection::try_execute`.
    pub fn try_execute(&self, query: &str, params: &[&ToSql])
            -> Result<uint, PostgresError> {
        self.conn.get_ref().try_execute(query, params)
    }

    /// Like `PostgresConnection::execute`.
    pub fn execute(&self, query: &str, params: &[&ToSql]) -> uint {
        self.conn.get_ref().execute(query, params)
    }

    /// Like `PostgresConnection::try_transaction`.
    pub fn try_transaction<'a>(&'a self)
            -> Result<PostgresTransaction<'a>, PostgresError> {
        self.conn.get_ref().try_transaction()
    }

    /// Like `PostgresConnection::transaction`.
    pub fn transaction<'a>(&'a self) -> PostgresTransaction<'a> {
        self.conn.get_ref().transaction()
    }

    /// Like `PostgresConnection::notifications`.
    pub fn notifications<'a>(&'a self) -> PostgresNotifications<'a> {
        self.conn.get_ref().notifications()
    }

    /// Like `PostgresConnection::cancel_data`.
    pub fn cancel_data(&self) -> PostgresCancelData {
        self.conn.get_ref().cancel_data()
    }

    /// Like `PostgresConnection::is_desynchronized`.
    pub fn is_desynchronized(&self) -> bool {
        self.conn.get_ref().is_desynchronized()
    }
}

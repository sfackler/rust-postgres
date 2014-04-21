//! A simple connection pool

use std::cast;
use sync::{Arc, Mutex};

use {PostgresNotifications,
     PostgresConnectParams,
     IntoConnectParams,
     PostgresResult,
     PostgresCancelData,
     PostgresConnection,
     PostgresStatement,
     PostgresTransaction,
     SslMode};
use error::PostgresConnectError;
use types::ToSql;

struct InnerConnectionPool {
    params: PostgresConnectParams,
    ssl: SslMode,
    // Actually Vec<~PostgresConnection>
    pool: Vec<*()>,
}

impl Drop for InnerConnectionPool {
    fn drop(&mut self) {
        loop {
            match self.pool.pop() {
                Some(conn) => unsafe {
                    let c: ~PostgresConnection = cast::transmute(conn);
                    drop(c);
                },
                None => break
            }
        }
    }
}

impl InnerConnectionPool {
    fn add_connection(&mut self) -> Result<(), PostgresConnectError> {
        PostgresConnection::connect(self.params.clone(), &self.ssl)
            .map(|c| unsafe { self.pool.push(cast::transmute(~c)); })
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
///                                        NoSsl, 5).unwrap();
/// for _ in range(0, 10) {
///     let pool = pool.clone();
///     spawn(proc() {
///         let conn = pool.get_connection();
///         conn.execute("UPDATE foo SET bar = 1", []).unwrap();
///     });
/// }
/// ```
#[deriving(Clone)]
pub struct PostgresConnectionPool {
    pool: Arc<Mutex<InnerConnectionPool>>
}

impl PostgresConnectionPool {
    /// Creates a new pool with the specified number of connections.
    ///
    /// Returns an error if the specified number of connections cannot be
    /// created.
    pub fn new<T: IntoConnectParams>(params: T, ssl: SslMode, pool_size: uint)
            -> Result<PostgresConnectionPool, PostgresConnectError> {
        let mut pool = InnerConnectionPool {
            params: try!(params.into_connect_params()),
            ssl: ssl,
            pool: Vec::new(),
        };

        for _ in range(0, pool_size) {
            try!(pool.add_connection());
        }

        Ok(PostgresConnectionPool {
            pool: Arc::new(Mutex::new(pool))
        })
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
    pool: PostgresConnectionPool,
    // TODO remove the Option wrapper when drop takes self by value
    conn: Option<~PostgresConnection>
}

impl Drop for PooledPostgresConnection {
    fn drop(&mut self) {
        let conn = unsafe { cast::transmute(self.conn.take_unwrap()) };
        let mut pool = self.pool.pool.lock();
        pool.pool.push(conn);
        pool.cond.signal();
    }
}

impl PooledPostgresConnection {
    /// Like `PostgresConnection::prepare`.
    pub fn prepare<'a>(&'a self, query: &str)
            -> PostgresResult<PostgresStatement<'a>> {
        self.conn.get_ref().prepare(query)
    }

    /// Like `PostgresConnection::execute`.
    pub fn execute(&self, query: &str, params: &[&ToSql])
            -> PostgresResult<uint> {
        self.conn.get_ref().execute(query, params)
    }

    /// Like `PostgresConnection::transaction`.
    pub fn transaction<'a>(&'a self)
            -> PostgresResult<PostgresTransaction<'a>> {
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

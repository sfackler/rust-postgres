extern mod extra;

use extra::arc::MutexArc;

use super::{PostgresConnection,
            NormalPostgresStatement,
            PostgresDbError,
            PostgresConnectError,
            PostgresTransaction};
use super::types::ToSql;

pub struct PostgresConnectionPoolConfig {
    initial_size: uint,
    min_size: uint,
    max_size: uint
}

impl PostgresConnectionPoolConfig {
    fn validate(&self) {
        assert!(self.initial_size >= self.min_size);
        assert!(self.initial_size <= self.max_size);
    }
}

pub static DEFAULT_CONFIG: PostgresConnectionPoolConfig =
    PostgresConnectionPoolConfig {
        initial_size: 3,
        min_size: 3,
        max_size: 15
    };

struct InnerConnectionPool {
    url: ~str,
    config: PostgresConnectionPoolConfig,
    pool: ~[PostgresConnection],
    size: uint,
}

impl InnerConnectionPool {
    fn new_connection(&mut self) -> Option<PostgresConnectError> {
        match PostgresConnection::try_connect(self.url) {
            Ok(conn) => {
                self.pool.push(conn);
                self.size += 1;
                None
            }
            Err(err) => Some(err)
        }
    }
}

// Should be a newtype, but blocked by mozilla/rust#9155
#[deriving(Clone)]
pub struct PostgresConnectionPool {
    pool: MutexArc<InnerConnectionPool>
}

impl PostgresConnectionPool {
    pub fn new(url: &str, config: PostgresConnectionPoolConfig)
            -> Result<PostgresConnectionPool, PostgresConnectError> {
        config.validate();

        let mut pool = InnerConnectionPool {
            url: url.to_owned(),
            config: config,
            pool: ~[],
            size: 0,
        };

        while pool.size < pool.config.initial_size {
            match pool.new_connection() {
                None => (),
                Some(err) => return Err(err)
            }
        }

        Ok(PostgresConnectionPool {
            pool: MutexArc::new(pool)
        })
    }

    pub fn try_get_connection(&self) -> Result<PooledPostgresConnection,
                                               PostgresConnectError> {
        let conn = unsafe {
            do self.pool.unsafe_access_cond |pool, cvar| {
                while pool.pool.is_empty() {
                    cvar.wait();
                }

                pool.pool.pop()
            }
        };

        Ok(PooledPostgresConnection {
            pool: self.clone(),
            conn: Some(conn)
        })
    }

    pub fn get_connection(&self) -> PooledPostgresConnection {
        match self.try_get_connection() {
            Ok(conn) => conn,
            Err(err) => fail!("Unable to get connection: %s", err.to_str())
        }
    }
}

// Should be a newtype
pub struct PooledPostgresConnection {
    priv pool: PostgresConnectionPool,
    // Todo remove the Option wrapper when drop takes self by value
    priv conn: Option<PostgresConnection>
}

impl Drop for PooledPostgresConnection {
    fn drop(&mut self) {
        unsafe {
            do self.pool.pool.unsafe_access |pool| {
                pool.pool.push(self.conn.take_unwrap());
            }
        }
    }
}

impl PooledPostgresConnection {
    pub fn try_prepare<'a>(&'a self, query: &str)
            -> Result<NormalPostgresStatement<'a>, PostgresDbError> {
        self.conn.get_ref().try_prepare(query)
    }

    pub fn prepare<'a>(&'a self, query: &str) -> NormalPostgresStatement<'a> {
        self.conn.get_ref().prepare(query)
    }

    pub fn try_update(&self, query: &str, params: &[&ToSql])
            -> Result<uint, PostgresDbError> {
        self.conn.get_ref().try_update(query, params)
    }

    pub fn update(&self, query: &str, params: &[&ToSql]) -> uint {
        self.conn.get_ref().update(query, params)
    }

    pub fn in_transaction<T>(&self, blk: &fn(&PostgresTransaction) -> T) -> T {
        self.conn.get_ref().in_transaction(blk)
    }
}

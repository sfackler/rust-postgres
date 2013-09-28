extern mod extra;

use extra::arc::MutexArc;

use super::{PostgresConnection,
            NormalPostgresStatement,
            PostgresDbError,
            PostgresConnectError,
            PostgresTransaction};
use super::types::ToSql;

struct InnerConnectionPool {
    url: ~str,
    pool: ~[PostgresConnection],
}

impl InnerConnectionPool {
    fn new_connection(&mut self) -> Option<PostgresConnectError> {
        match PostgresConnection::try_connect(self.url) {
            Ok(conn) => {
                self.pool.push(conn);
                None
            }
            Err(err) => Some(err)
        }
    }
}

// Should be a newtype, but blocked by mozilla/rust#9155
#[deriving(Clone)]
pub struct PostgresConnectionPool {
    priv pool: MutexArc<InnerConnectionPool>
}

impl PostgresConnectionPool {
    pub fn try_new(url: &str, pool_size: uint)
            -> Result<PostgresConnectionPool, PostgresConnectError> {
        let mut pool = InnerConnectionPool {
            url: url.to_owned(),
            pool: ~[],
        };

        while pool.pool.len() < pool_size {
            match pool.new_connection() {
                None => (),
                Some(err) => return Err(err)
            }
        }

        Ok(PostgresConnectionPool {
            pool: MutexArc::new(pool)
        })
    }

    pub fn new(url: &str, pool_size: uint) -> PostgresConnectionPool {
        match PostgresConnectionPool::try_new(url, pool_size) {
            Ok(pool) => pool,
            Err(err) => fail!("Unable to initialize pool: %s", err.to_str())
        }
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

pub struct PooledPostgresConnection {
    priv pool: PostgresConnectionPool,
    // TODO remove the Option wrapper when drop takes self by value
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

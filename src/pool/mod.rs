extern mod extra;

use extra::arc::MutexArc;
use std::cell::Cell;

use super::{PostgresConnection, PostgresConnectError};

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

    pub fn try_with_connection<T>(&mut self,
                                  blk: &fn(&PostgresConnection) -> T)
            -> Result<T, PostgresConnectError> {
        let conn = unsafe {
            do self.pool.unsafe_access_cond |pool, cond| {
                while pool.pool.is_empty() {
                    cond.wait();
                }

                pool.pool.pop()
            }
        };

        let ret = blk(&conn);

        let conn = Cell::new(conn);
        unsafe {
            do self.pool.unsafe_access |pool| {
                pool.pool.push(conn.take());
            }
        }

        Ok(ret)
    }

    pub fn with_connection<T>(&mut self, blk: &fn(&PostgresConnection) -> T)
            -> T {
        match self.try_with_connection(blk) {
            Ok(ret) => ret,
            Err(err) => fail!("Error getting connection: %s", err.to_str())
        }
    }
}

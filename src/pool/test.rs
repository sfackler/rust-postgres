extern mod postgres;

use postgres::pool;
use postgres::pool::{PostgresConnectionPool};

#[test]
fn test_pool() {
    let mut pool = PostgresConnectionPool::new("postgres://postgres@localhost",
                                               pool::DEFAULT_CONFIG).unwrap();

    do pool.with_connection |_conn| {

    }
}

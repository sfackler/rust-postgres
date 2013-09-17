extern mod extra;
extern mod postgres;

use std::cell::Cell;
use extra::comm::DuplexStream;
use extra::future;

use postgres::pool::{PostgresConnectionPool, PostgresConnectionPoolConfig};

#[test]
// Make sure we can take both connections at once and can still get one after
fn test_pool() {
    let config = PostgresConnectionPoolConfig {
        initial_size: 2,
        min_size: 2,
        max_size: 2
    };
    let mut pool = PostgresConnectionPool::new("postgres://postgres@localhost",
                                               config).unwrap();

    let (stream1, stream2) = DuplexStream::<(), ()>();

    let pool1 = Cell::new(pool.clone());
    let mut fut1 = do future::spawn {
        let mut pool = pool1.take();

        do pool.with_connection |_conn| {
            stream1.send(());
            stream1.recv();
        }
    };

    let pool2 = Cell::new(pool.clone());
    let mut fut2 = do future::spawn {
        let mut pool = pool2.take();

        do pool.with_connection |_conn| {
            stream2.send(());
            stream2.recv();
        }
    };

    fut1.get();
    fut2.get();

    do pool.with_connection |_conn| { }
}

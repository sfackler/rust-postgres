extern mod extra;
extern mod postgres;

use extra::comm::DuplexStream;
use extra::future::Future;

use postgres::pool::PostgresConnectionPool;

#[test]
// Make sure we can take both connections at once and can still get one after
fn test_pool() {
    let pool = PostgresConnectionPool::new("postgres://postgres@localhost", 2);

    let (stream1, stream2) = DuplexStream::<(), ()>();

    let mut fut1 = do Future::spawn_with(pool.clone()) |pool| {
        let _conn = pool.get_connection();
        stream1.send(());
        stream1.recv();
    };

    let mut fut2 = do Future::spawn_with(pool.clone()) |pool| {
        let _conn = pool.get_connection();
        stream2.send(());
        stream2.recv();
    };

    fut1.get();
    fut2.get();

    pool.get_connection();
}

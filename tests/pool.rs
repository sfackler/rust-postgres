#![allow(deprecated)]

use std::comm;
use std::sync::Future;

use postgres::NoSsl;
use postgres::pool::PostgresConnectionPool;

#[test]
// Make sure we can take both connections at once and can still get one after
fn test_pool() {
    let pool = or_fail!(PostgresConnectionPool::new("postgres://postgres@localhost",
                                                    NoSsl, 2));

    let (s1, r1) = comm::channel();
    let (s2, r2) = comm::channel();

    let pool1 = pool.clone();
    let mut fut1 = Future::spawn(proc() {
        let _conn = pool1.get_connection();
        s1.send(());
        r2.recv();
    });

    let pool2 = pool.clone();
    let mut fut2 = Future::spawn(proc() {
        let _conn = pool2.get_connection();
        s2.send(());
        r1.recv();
    });

    fut1.get();
    fut2.get();

    pool.get_connection();
}

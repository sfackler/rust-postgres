extern mod sqlite3;

use sqlite3::SqlType;

macro_rules! chk (
    ($result:expr) => (
        match $result {
            Err(err) => fail!(err),
            Ok(val) => val
        }
    )
)

#[test]
fn test_basic() {
    let conn = chk!(sqlite3::open(":memory:"));
    chk!(conn.update("CREATE TABLE foo (
                        id  BIGINT PRIMARY KEY
                      )"));
    chk!(conn.update("INSERT INTO foo (id) VALUES (101), (102)"));

    assert_eq!(2, chk!(conn.query("SELECT COUNT(*) FROM foo", |it| {
        it.next().unwrap()[0]
    })));
}

#[test]
fn test_trans() {
    let conn = chk!(sqlite3::open(":memory:"));
    chk!(conn.update("CREATE TABLE bar (
                        id BIGINT PRIMARY KEY
                      )"));
    do conn.in_transaction |conn| {
        chk!(conn.update("INSERT INTO bar (id) VALUES (100)"));
        Err::<(), ~str>(~"")
    };
    assert_eq!(0, chk!(conn.query("SELECT COUNT(*) FROM bar", |it| {
        it.next().unwrap()[0]
    })));

    do conn.in_transaction |conn| {
        chk!(conn.update("INSERT INTO bar (id) VALUES (100)"));
        Ok(())
    };

    assert_eq!(1, chk!(conn.query("SELECT COUNT(*) FROM bar", |it| {
        it.next().unwrap()[0]
    })));
}

#[test]
fn test_params() {
    let conn = chk!(sqlite3::open(":memory:"));
    chk!(conn.update("CREATE TABLE foo (
                        id  BIGINT PRIMARY KEY
                      )"));
    chk!(conn.update_params("INSERT INTO foo (id) VALUES (?), (?)",
                          &[@100 as @SqlType, @101 as @SqlType]));

    assert_eq!(2, chk!(conn.query("SELECT COUNT(*) FROM foo", |it| {
        it.next().unwrap()[0]
    })));
}

#[test]
fn test_null() {
    let conn = chk!(sqlite3::open(":memory:"));
    chk!(conn.update("CREATE TABLE foo (
                        id BIGINT PRIMARY KEY,
                        n BIGINT
                      )"));
    chk!(conn.update_params("INSERT INTO foo (id, n) VALUES (?, ?), (?, ?)",
                            &[@100 as @SqlType, @None::<int> as @SqlType,
                              @101 as @SqlType, @Some(1) as @SqlType]));

    do conn.query("SELECT n FROM foo WHERE id = 100") |it| {
        assert!(it.next().unwrap().get::<Option<int>>(0).is_none());
    };

    do conn.query("SELECT n FROM foo WHERE id = 101") |it| {
        assert_eq!(Some(1), it.next().unwrap()[0])
    };
}

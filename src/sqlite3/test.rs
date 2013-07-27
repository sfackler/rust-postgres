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

    do conn.query("SELECT id FROM foo") |it| {
        for it.advance |row| {
            printfln!("%u %d", row.len(), row.get(0).get());
        }
    };
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
        it.next().get().get(0).get()
    })));

    do conn.in_transaction |conn| {
        chk!(conn.update("INSERT INTO bar (id) VALUES (100)"));
        Ok(())
    };

    assert_eq!(1, chk!(conn.query("SELECT COUNT(*) FROM bar", |it| {
        it.next().get().get(0).get()
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
        it.next().get().get(0).get()
    })));
}

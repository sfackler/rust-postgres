use std::io::Read;
use tokio_postgres::types::Type;
use tokio_postgres::NoTls;

use super::*;

#[test]
fn prepare() {
    let mut client = Client::connect("host=localhost port=5433 user=postgres", NoTls).unwrap();

    let stmt = client.prepare("SELECT 1::INT, $1::TEXT").unwrap();
    assert_eq!(stmt.params(), &[Type::TEXT]);
    assert_eq!(stmt.columns().len(), 2);
    assert_eq!(stmt.columns()[0].type_(), &Type::INT4);
    assert_eq!(stmt.columns()[1].type_(), &Type::TEXT);
}

#[test]
fn query_prepared() {
    let mut client = Client::connect("host=localhost port=5433 user=postgres", NoTls).unwrap();

    let stmt = client.prepare("SELECT $1::TEXT").unwrap();
    let rows = client.query(&stmt, &[&"hello"]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "hello");
}

#[test]
fn query_unprepared() {
    let mut client = Client::connect("host=localhost port=5433 user=postgres", NoTls).unwrap();

    let rows = client.query("SELECT $1::TEXT", &[&"hello"]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "hello");
}

#[test]
fn transaction_commit() {
    let mut client = Client::connect("host=localhost port=5433 user=postgres", NoTls).unwrap();

    client
        .simple_query("CREATE TEMPORARY TABLE foo (id SERIAL PRIMARY KEY)")
        .unwrap();

    let mut transaction = client.transaction().unwrap();

    transaction
        .execute("INSERT INTO foo DEFAULT VALUES", &[])
        .unwrap();

    transaction.commit().unwrap();

    let rows = client.query("SELECT * FROM foo", &[]).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
}

#[test]
fn transaction_rollback() {
    let mut client = Client::connect("host=localhost port=5433 user=postgres", NoTls).unwrap();

    client
        .simple_query("CREATE TEMPORARY TABLE foo (id SERIAL PRIMARY KEY)")
        .unwrap();

    let mut transaction = client.transaction().unwrap();

    transaction
        .execute("INSERT INTO foo DEFAULT VALUES", &[])
        .unwrap();

    transaction.rollback().unwrap();

    let rows = client.query("SELECT * FROM foo", &[]).unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn transaction_drop() {
    let mut client = Client::connect("host=localhost port=5433 user=postgres", NoTls).unwrap();

    client
        .simple_query("CREATE TEMPORARY TABLE foo (id SERIAL PRIMARY KEY)")
        .unwrap();

    let mut transaction = client.transaction().unwrap();

    transaction
        .execute("INSERT INTO foo DEFAULT VALUES", &[])
        .unwrap();

    drop(transaction);

    let rows = client.query("SELECT * FROM foo", &[]).unwrap();
    assert_eq!(rows.len(), 0);
}

#[test]
fn nested_transactions() {
    let mut client = Client::connect("host=localhost port=5433 user=postgres", NoTls).unwrap();

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)")
        .unwrap();

    let mut transaction = client.transaction().unwrap();

    transaction
        .execute("INSERT INTO foo (id) VALUES (1)", &[])
        .unwrap();

    let mut transaction2 = transaction.transaction().unwrap();

    transaction2
        .execute("INSERT INTO foo (id) VALUES (2)", &[])
        .unwrap();

    transaction2.rollback().unwrap();

    let rows = transaction
        .query("SELECT id FROM foo ORDER BY id", &[])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);

    let mut transaction3 = transaction.transaction().unwrap();

    transaction3
        .execute("INSERT INTO foo (id) VALUES(3)", &[])
        .unwrap();

    let mut transaction4 = transaction3.transaction().unwrap();

    transaction4
        .execute("INSERT INTO foo (id) VALUES(4)", &[])
        .unwrap();

    transaction4.commit().unwrap();
    transaction3.commit().unwrap();
    transaction.commit().unwrap();

    let rows = client.query("SELECT id FROM foo ORDER BY id", &[]).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(0), 3);
    assert_eq!(rows[2].get::<_, i32>(0), 4);
}

#[test]
fn copy_in() {
    let mut client = Client::connect("host=localhost port=5433 user=postgres", NoTls).unwrap();

    client
        .simple_query("CREATE TEMPORARY TABLE foo (id INT, name TEXT)")
        .unwrap();

    client
        .copy_in(
            "COPY foo FROM stdin",
            &[],
            &mut &b"1\tsteven\n2\ttimothy"[..],
        )
        .unwrap();

    let rows = client
        .query("SELECT id, name FROM foo ORDER BY id", &[])
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "steven");
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, &str>(1), "timothy");
}

#[test]
fn copy_out() {
    let mut client = Client::connect("host=localhost port=5433 user=postgres", NoTls).unwrap();

    client
        .simple_query(
            "CREATE TEMPORARY TABLE foo (id INT, name TEXT);
             INSERT INTO foo (id, name) VALUES (1, 'steven'), (2, 'timothy');",
        )
        .unwrap();

    let mut reader = client
        .copy_out("COPY foo (id, name) TO STDOUT", &[])
        .unwrap();
    let mut s = String::new();
    reader.read_to_string(&mut s).unwrap();
    drop(reader);

    assert_eq!(s, "1\tsteven\n2\ttimothy\n");

    client.simple_query("SELECT 1").unwrap();
}

#[test]
fn portal() {
    let mut client = Client::connect("host=localhost port=5433 user=postgres", NoTls).unwrap();

    client
        .simple_query(
            "CREATE TEMPORARY TABLE foo (id INT);
             INSERT INTO foo (id) VALUES (1), (2), (3);",
        )
        .unwrap();

    let mut transaction = client.transaction().unwrap();

    let portal = transaction
        .bind("SELECT * FROM foo ORDER BY id", &[])
        .unwrap();

    let rows = transaction.query_portal(&portal, 2).unwrap();
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[1].get::<_, i32>(0), 2);

    let rows = transaction.query_portal(&portal, 2).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 3);
}

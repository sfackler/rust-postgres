extern mod postgres;

use postgres::{PostgresConnection, ToSql};

#[test]
fn test_basic() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    do conn.in_transaction |conn| {
        conn.prepare("CREATE TABLE foo (id BIGINT PRIMARY KEY)").update([]);

        Err::<(), ()>(())
    };
}

#[test]
fn test_prepare_err() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");
    assert!(conn.try_prepare("invalid sql statment").is_err());
}

#[test]
fn test_query() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    do conn.in_transaction |conn| {
        conn.prepare("CREATE TABLE foo (id BIGINT PRIMARY KEY)").update([]);
        conn.prepare("INSERT INTO foo (id) VALUES ($1), ($2)")
                .update([&1 as &ToSql, &2 as &ToSql]);
        let stmt = conn.prepare("SELECT * from foo ORDER BY id");
        let result = stmt.query([]);

        assert_eq!(~[1, 2], result.iter().map(|row| { row[0] }).collect());

        Err::<(), ()>(())
    };
}

#[test]
fn test_nulls() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    do conn.in_transaction |conn| {
        conn.prepare("CREATE TABLE foo (
                        id BIGINT PRIMARY KEY,
                        val VARCHAR
                      )").update([]);
        conn.prepare("INSERT INTO foo (id, val) VALUES ($1, $2), ($3, $4)")
                .update([&1 as &ToSql, & &"foobar" as &ToSql,
                         &2 as &ToSql, &None::<~str> as &ToSql]);
        let stmt = conn.prepare("SELECT id, val FROM foo ORDER BY id");
        let result = stmt.query([]);

        assert_eq!(~[Some(~"foobar"), None],
                   result.iter().map(|row| { row[1] }).collect());

        Err::<(), ()>(())
    };
}

#[test]
fn test_plaintext_pass() {
    PostgresConnection::connect("postgres://pass_user:password@127.0.0.1:5432");
}

#[test]
#[should_fail]
fn test_plaintext_pass_no_pass() {
    PostgresConnection::connect("postgres://pass_user@127.0.0.1:5432");
}

#[test]
#[should_fail]
fn test_plaintext_pass_wrong_pass() {
    PostgresConnection::connect("postgres://pass_user:asdf@127.0.0.1:5432");
}

#[test]
fn test_md5_pass() {
    PostgresConnection::connect("postgres://md5_user:password@127.0.0.1:5432");
}

#[test]
#[should_fail]
fn test_md5_pass_no_pass() {
    PostgresConnection::connect("postgres://md5_user@127.0.0.1:5432");
}

#[test]
#[should_fail]
fn test_md5_pass_wrong_pass() {
    PostgresConnection::connect("postgres://md5_user:asdf@127.0.0.1:5432");
}

extern mod postgres;

use postgres::*;
use postgres::types::ToSql;

#[test]
fn test_basic() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    do conn.in_transaction |trans| {
        trans.prepare("CREATE TABLE foo (id BIGINT PRIMARY KEY)").update([]);

        trans.set_rollback();
    };
}

#[test]
fn test_prepare_err() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");
    match conn.try_prepare("invalid sql statment") {
        Err(PostgresDbError { position, code, _ }) => {
            assert_eq!(code, ~"42601");
            match position {
                Some(Position(1)) => (),
                position => fail!("Unexpected position %?", position)
            }
        }
        resp => fail!("Unexpected result %?", resp)
    }
}

#[test]
fn test_query() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    do conn.in_transaction |trans| {
        trans.prepare("CREATE TABLE foo (id BIGINT PRIMARY KEY)").update([]);
        trans.prepare("INSERT INTO foo (id) VALUES ($1), ($2)")
                .update([&1 as &ToSql, &2 as &ToSql]);
        let stmt = trans.prepare("SELECT * from foo ORDER BY id");
        let result = stmt.query([]);

        assert_eq!(~[1, 2], result.iter().map(|row| { row[0] }).collect());

        trans.set_rollback();
    };
}

#[test]
fn test_nulls() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    do conn.in_transaction |trans| {
        trans.prepare("CREATE TABLE foo (
                        id BIGINT PRIMARY KEY,
                        val VARCHAR
                      )").update([]);
        trans.prepare("INSERT INTO foo (id, val) VALUES ($1, $2), ($3, $4)")
                .update([&1 as &ToSql, & &"foobar" as &ToSql,
                         &2 as &ToSql, &None::<~str> as &ToSql]);
        let stmt = trans.prepare("SELECT id, val FROM foo ORDER BY id");
        let result = stmt.query([]);

        assert_eq!(~[Some(~"foobar"), None],
                   result.iter().map(|row| { row[1] }).collect());

        trans.set_rollback();
    };
}

#[test]
fn test_binary_bool_params() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    do conn.in_transaction |trans| {
        trans.prepare("CREATE TABLE foo (
                        id BIGINT PRIMARY KEY,
                        b BOOL
                       )").update([]);
        trans.prepare("INSERT INTO foo (id, b) VALUES
                        ($1, $2), ($3, $4), ($5, $6)")
                .update([&1 as &ToSql, &true as &ToSql,
                         &2 as &ToSql, &false as &ToSql,
                         &3 as &ToSql, &None::<bool> as &ToSql]);
        let stmt = trans.prepare("SELECT b FROM foo ORDER BY id");
        let result = stmt.query([]);

        assert_eq!(~[Some(true), Some(false), None],
                   result.iter().map(|row| { row[0] }).collect());

        trans.set_rollback();
    }
}

#[test]
fn test_wrong_num_params() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    do conn.in_transaction |trans| {
        trans.prepare("CREATE TABLE foo (
                        id BIGINT PRIMARY KEY,
                        val VARCHAR
                      )").update([]);
        let res = trans.prepare("INSERT INTO foo (id, val) VALUES ($1, $2), ($3, $4)")
                .try_update([&1 as &ToSql, & &"foobar" as &ToSql]);
        match res {
            Err(PostgresDbError { code: ~"08P01", _ }) => (),
            resp => fail!("Unexpected response: %?", resp)
        }

        trans.set_rollback();
    }
}

#[test]
fn test_plaintext_pass() {
    PostgresConnection::connect("postgres://pass_user:password@127.0.0.1:5432");
}

#[test]
fn test_plaintext_pass_no_pass() {
    let ret = PostgresConnection::try_connect("postgres://pass_user@127.0.0.1:5432");
    match ret {
        Err(MissingPassword) => (),
        ret => fail!("Unexpected result %?", ret)
    }
}

#[test]
fn test_plaintext_pass_wrong_pass() {
    let ret = PostgresConnection::try_connect("postgres://pass_user:asdf@127.0.0.1:5432");
    match ret {
        Err(DbError(PostgresDbError { code: ~"28P01", _ })) => (),
        ret => fail!("Unexpected result %?", ret)
    }
}

#[test]
fn test_md5_pass() {
    PostgresConnection::connect("postgres://md5_user:password@127.0.0.1:5432");
}

#[test]
fn test_md5_pass_no_pass() {
    let ret = PostgresConnection::try_connect("postgres://md5_user@127.0.0.1:5432");
    match ret {
        Err(MissingPassword) => (),
        ret => fail!("Unexpected result %?", ret)
    }
}

#[test]
fn test_md5_pass_wrong_pass() {
    let ret = PostgresConnection::try_connect("postgres://md5_user:asdf@127.0.0.1:5432");
    match ret {
        Err(DbError(PostgresDbError { code: ~"28P01", _ })) => (),
        ret => fail!("Unexpected result %?", ret)
    }
}

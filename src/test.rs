extern mod extra;
extern mod postgres;

use extra::json;
use extra::uuid::Uuid;
use std::f32;
use std::f64;

use postgres::*;
use postgres::types::{ToSql, FromSql};

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
fn test_transaction_commit() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");
    conn.update("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []);

    do conn.in_transaction |trans| {
        trans.update("INSERT INTO foo (id) VALUES ($1)", [&1i32 as &ToSql]);
    }

    let stmt = conn.prepare("SELECT * FROM foo");
    let result = stmt.query([]);

    assert_eq!(~[1i32], result.map(|row| { row[0] }).collect());
}

#[test]
fn test_transaction_rollback() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");
    conn.update("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []);

    conn.update("INSERT INTO foo (id) VALUES ($1)", [&1i32 as &ToSql]);
    do conn.in_transaction |trans| {
        trans.update("INSERT INTO foo (id) VALUES ($1)", [&2i32 as &ToSql]);
        trans.set_rollback();
    }

    let stmt = conn.prepare("SELECT * FROM foo");
    let result = stmt.query([]);

    assert_eq!(~[1i32], result.map(|row| { row[0] }).collect());
}

#[test]
fn test_query() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");
    conn.update("CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY)", []);
    conn.update("INSERT INTO foo (id) VALUES ($1), ($2)",
                 [&1i64 as &ToSql, &2i64 as &ToSql]);
    let stmt = conn.prepare("SELECT * from foo ORDER BY id");
    let result = stmt.query([]);

    assert_eq!(~[1i64, 2], result.map(|row| { row[0] }).collect());
}

#[test]
fn test_lazy_query() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    do conn.in_transaction |trans| {
        trans.update("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []);
        let stmt = trans.prepare("INSERT INTO foo (id) VALUES ($1)");
        let values = ~[0i32, 1, 2, 3, 4, 5];
        for value in values.iter() {
            stmt.update([value as &ToSql]);
        }

        let stmt = trans.prepare("SELECT id FROM foo ORDER BY id");
        let result = stmt.lazy_query(2, []);
        assert_eq!(values, result.map(|row| { row[0] }).collect());

        trans.set_rollback();
    }
}

fn test_type<T: Eq+ToSql+FromSql>(sql_type: &str, values: &[T]) {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");
    conn.update("CREATE TEMPORARY TABLE foo (
                    id SERIAL PRIMARY KEY,
                    b " + sql_type +
                ")", []);
    let stmt = conn.prepare("INSERT INTO foo (b) VALUES ($1)");
    for value in values.iter() {
        stmt.update([value as &ToSql]);
    }

    let stmt = conn.prepare("SELECT b FROM foo ORDER BY id");
    let result = stmt.query([]);

    let actual_values: ~[T] = result.map(|row| { row[0] }).collect();
    assert_eq!(actual_values.as_slice(), values);
}

#[test]
fn test_bool_params() {
    test_type("BOOL", [Some(true), Some(false), None]);
}

#[test]
fn test_i8_params() {
    test_type("\"char\"", [Some(-100i8), Some(127i8), None]);
}

#[test]
fn test_i16_params() {
    test_type("SMALLINT", [Some(0x0011i16), Some(-0x0011i16), None]);
}

#[test]
fn test_i32_params() {
    test_type("INT", [Some(0x00112233i32), Some(-0x00112233i32), None]);
}

#[test]
fn test_i64_params() {
    test_type("BIGINT", [Some(0x0011223344556677i64),
                         Some(-0x0011223344556677i64), None]);
}

#[test]
fn test_f32_params() {
    test_type("REAL", [Some(f32::infinity), Some(f32::neg_infinity),
                       Some(1000.55), None]);
}

#[test]
fn test_f64_params() {
    test_type("DOUBLE PRECISION", [Some(f64::infinity),
                                   Some(f64::neg_infinity),
                                   Some(10000.55), None]);
}

#[test]
fn test_varchar_params() {
    test_type("VARCHAR", [Some(~"hello world"),
                          Some(~"イロハニホヘト チリヌルヲ"), None]);
}

#[test]
fn test_text_params() {
    test_type("TEXT", [Some(~"hello world"),
                       Some(~"イロハニホヘト チリヌルヲ"), None]);

}

#[test]
fn test_bpchar_params() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");
    conn.update("CREATE TEMPORARY TABLE foo (
                    id SERIAL PRIMARY KEY,
                    b CHAR(5)
                 )", []);
    conn.update("INSERT INTO foo (b) VALUES ($1), ($2), ($3)",
                [&Some("12345") as &ToSql, &Some("123") as &ToSql,
                 &None::<~str> as &ToSql]);
    let stmt = conn.prepare("SELECT b FROM foo ORDER BY id");
    let res = stmt.query([]);

    assert_eq!(~[Some(~"12345"), Some(~"123  "), None],
               res.map(|row| { row[0] }).collect());
}

#[test]
fn test_bytea_params() {
    test_type("BYTEA", [Some(~[0u8, 1, 2, 3, 254, 255]), None]);
}

#[test]
fn test_json_params() {
    test_type("JSON", [Some(json::from_str("[10, 11, 12]").unwrap()),
                       Some(json::from_str("{\"f\": \"asd\"}").unwrap()),
                       None])
}

#[test]
fn test_uuid_params() {
    test_type("UUID", [Some(Uuid::parse_string("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap()),
                       None])
}

fn test_nan_param<T: Float+ToSql+FromSql>(sql_type: &str) {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");
    let nan: T = Float::NaN();
    let stmt = conn.prepare("SELECT $1::" + sql_type);
    let mut result = stmt.query([&nan as &ToSql]);
    let val: T = result.next().unwrap()[0];
    assert!(val.is_NaN())
}

#[test]
fn test_f32_nan_param() {
    test_nan_param::<f32>("REAL");
}

#[test]
fn test_f64_nan_param() {
    test_nan_param::<f64>("DOUBLE PRECISION");
}

#[test]
#[should_fail]
fn test_wrong_param_type() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");
    conn.try_update("SELECT $1::VARCHAR", [&1i32 as &ToSql]);
}

#[test]
fn test_find_col_named() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");
    let stmt = conn.prepare("SELECT 1 as my_id, 'hi' as val");
    assert_eq!(Some(0), stmt.find_col_named("my_id"));
    assert_eq!(Some(1), stmt.find_col_named("val"));
    assert_eq!(None, stmt.find_col_named("asdf"));
}

#[test]
fn test_get_named() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");
    let stmt = conn.prepare("SELECT 10::INT as val");
    let result = stmt.query([]);

    assert_eq!(~[10i32], result.map(|row| { row["val"] }).collect());
}

#[test]
#[should_fail]
fn test_get_named_fail() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");
    let stmt = conn.prepare("SELECT 10::INT as id");
    let mut result = stmt.query([]);

    let _: i32 = result.next().unwrap()["asdf"];
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

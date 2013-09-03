extern mod extra;
extern mod postgres;

use extra::json;
use extra::uuid::Uuid;
use std::f32;
use std::f64;

use postgres::*;
use postgres::types::{ToSql, FromSql};

#[test]
fn test_basic() {
    do test_in_transaction |trans| {
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

fn test_in_transaction(blk: &fn(&PostgresTransaction)) {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    do conn.in_transaction |trans| {
        blk(trans);
        trans.set_rollback();
    }
}

#[test]
fn test_query() {
    do test_in_transaction |trans| {
        trans.update("CREATE TABLE foo (id BIGINT PRIMARY KEY)", []);
        trans.update("INSERT INTO foo (id) VALUES ($1), ($2)",
                     [&1i64 as &ToSql, &2i64 as &ToSql]);
        let stmt = trans.prepare("SELECT * from foo ORDER BY id");
        let result = stmt.query([]);

        assert_eq!(~[1i64, 2], result.map(|row| { row[0] }).collect());
    }
}

#[test]
fn test_nulls() {
    do test_in_transaction |trans| {
        trans.update("CREATE TABLE foo (
                        id SERIAL PRIMARY KEY,
                        val VARCHAR
                      )", []);
        trans.update("INSERT INTO foo (val) VALUES ($1), ($2)",
                     [& &"foobar" as &ToSql, &None::<~str> as &ToSql]);
        let stmt = trans.prepare("SELECT val FROM foo ORDER BY id");
        let result = stmt.query([]);

        assert_eq!(~[Some(~"foobar"), None],
                   result.map(|row| { row[0] }).collect());

        trans.set_rollback();
    }
}

#[test]
fn test_lazy_query() {
    do test_in_transaction |trans| {
        trans.update("CREATE TABLE foo (
                        id SERIAL PRIMARY KEY,
                        val BIGINT
                      )", []);
        let stmt = trans.prepare("INSERT INTO foo (val) VALUES ($1)");
        let data = ~[1i64, 2, 3, 4, 5, 6];
        for datum in data.iter() {
            stmt.update([datum as &ToSql]);
        }

        let stmt = trans.prepare("SELECT val FROM foo ORDER BY id");
        let result = stmt.lazy_query(2, []);

        assert_eq!(data,
                   result.map(|row| { row[0] }).collect());
    }
}

fn test_type<T: Eq+ToSql+FromSql>(sql_type: &str, values: &[T]) {
    do test_in_transaction |trans| {
        trans.update("CREATE TABLE foo (
                        id SERIAL PRIMARY KEY,
                        b " + sql_type +
                     ")", []);
        let stmt = trans.prepare("INSERT INTO foo (b) VALUES ($1)");
        for value in values.iter() {
            stmt.update([value as &ToSql]);
        }

        let stmt = trans.prepare("SELECT b FROM foo ORDER BY id");
        let result = stmt.query([]);

        let actual_values: ~[T] = result.map(|row| { row[0] }).collect();
        assert_eq!(values, actual_values.as_slice());
    }
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
    do test_in_transaction |trans| {
        trans.update("CREATE TABLE foo (
                        id SERIAL PRIMARY KEY,
                        b CHAR(5)
                      )", []);
        trans.update("INSERT INTO foo (b) VALUES ($1), ($2), ($3)",
                     [&Some("12345") as &ToSql, &Some("123") as &ToSql,
                      &None::<~str> as &ToSql]);
        let stmt = trans.prepare("SELECT b FROM foo ORDER BY id");
        let res = stmt.query([]);

        assert_eq!(~[Some(~"12345"), Some(~"123  "), None],
                   res.map(|row| { row[0] }).collect());
    }
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
    do test_in_transaction |trans| {
        trans.update("CREATE TABLE foo (
                        id SERIAL PRIMARY KEY,
                        b " + sql_type +
                     ")", []);
        let nan: T = Float::NaN();
        trans.update("INSERT INTO foo (b) VALUES ($1)", [&nan as &ToSql]);

        let stmt = trans.prepare("SELECT b FROM foo");
        let mut result = stmt.query([]);
        let val: T = result.next().unwrap()[0];
        assert!(val.is_NaN());
    }
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
fn test_wrong_num_params() {
    do test_in_transaction |trans| {
        trans.update("CREATE TABLE foo (
                        id SERIAL PRIMARY KEY,
                        val VARCHAR
                     )", []);
        let res = trans.try_update("INSERT INTO foo (val) VALUES ($1), ($2)",
                                   [& &"foobar" as &ToSql]);
        match res {
            Err(PostgresDbError { code: ~"08P01", _ }) => (),
            resp => fail!("Unexpected response: %?", resp)
        }
    }
}

#[test]
#[should_fail]
fn test_wrong_param_type() {
    do test_in_transaction |trans| {
        trans.update("CREATE TABLE foo (
                        id SERIAL PRIMARY KEY,
                        val BOOL
                      )", []);
        trans.try_update("INSERT INTO foo (val) VALUES ($1)",
                         [&1i32 as &ToSql]);
    }
}

#[test]
fn test_find_col_named() {
    do test_in_transaction |trans| {
        trans.update("CREATE TABLE foo (
                        id SERIAL PRIMARY KEY,
                        val BOOL
                      )", []);
        let stmt = trans.prepare("SELECT id as my_id, val FROM foo");
        assert_eq!(Some(0), stmt.find_col_named("my_id"));
        assert_eq!(Some(1), stmt.find_col_named("val"));
        assert_eq!(None, stmt.find_col_named("asdf"));
    }
}

#[test]
fn test_find_get_named() {
    do test_in_transaction |trans| {
        trans.update("CREATE TABLE foo (
                        id SERIAL PRIMARY KEY,
                        val INT
                      )", []);
        trans.update("INSERT INTO foo (val) VALUES (10)", []);
        let stmt = trans.prepare("SELECT id, val FROM foo");
        let result = stmt.query([]);

        assert_eq!(~[10i32],
                   result.map(|row| { row.get_named("val") }).collect());
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

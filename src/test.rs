use collections::HashMap;
use serialize::json;
use sync;
use sync::Future;
use time;
use time::Timespec;
use uuid::Uuid;
use openssl::ssl::{SslContext, Sslv3};
use std::f32;
use std::f64;
use std::io::timer;
use url;

use {PostgresNoticeHandler,
     PostgresNotification,
     PostgresConnection,
     ResultDescription,
     RequireSsl,
     PreferSsl,
     NoSsl};
use error::{PgConnectDbError,
            PgDbError,
            PgWrongConnection,
            PgWrongParamCount,
            PgWrongType,
            PgInvalidColumn,
            PgWasNull,
            MissingPassword,
            Position,
            PostgresDbError,
            SyntaxError,
            InvalidPassword,
            QueryCanceled,
            InvalidCatalogName,
            PgWrongTransaction};
use types::{ToSql, FromSql, PgInt4, PgVarchar};
use types::array::{ArrayBase};
use types::range::{Range, Inclusive, Exclusive, RangeBound};
use pool::PostgresConnectionPool;

macro_rules! or_fail(
    ($e:expr) => (
        match $e {
            Ok(ok) => ok,
            Err(err) => fail!("{}", err)
        }
    )
)

#[test]
// Make sure we can take both connections at once and can still get one after
fn test_pool() {
    let pool = or_fail!(PostgresConnectionPool::new("postgres://postgres@localhost",
                                                    NoSsl, 2));

    let (stream1, stream2) = sync::duplex();

    let pool1 = pool.clone();
    let mut fut1 = Future::spawn(proc() {
        let _conn = pool1.get_connection();
        stream1.send(());
        stream1.recv();
    });

    let pool2 = pool.clone();
    let mut fut2 = Future::spawn(proc() {
        let _conn = pool2.get_connection();
        stream2.send(());
        stream2.recv();
    });

    fut1.get();
    fut2.get();

    pool.get_connection();
}

#[test]
fn test_non_default_database() {
    or_fail!(PostgresConnection::connect("postgres://postgres@localhost/postgres", &NoSsl));
}

#[test]
fn test_url_terminating_slash() {
    or_fail!(PostgresConnection::connect("postgres://postgres@localhost/", &NoSsl));
}

#[test]
fn test_prepare_err() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    match conn.prepare("invalid sql statment") {
        Err(PgDbError(PostgresDbError { code: SyntaxError, position: Some(Position(1)), .. })) => (),
        resp => fail!("Unexpected result {:?}", resp)
    }
}

#[test]
fn test_unknown_database() {
    match PostgresConnection::connect("postgres://postgres@localhost/asdf", &NoSsl) {
        Err(PgConnectDbError(PostgresDbError { code: InvalidCatalogName, .. })) => {}
        resp => fail!("Unexpected result {:?}", resp)
    }
}

#[test]
fn test_connection_finish() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    assert!(conn.finish().is_ok());
}

#[test]
fn test_unix_connection() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SHOW unix_socket_directories"));
    let result = or_fail!(stmt.query([]));
    let unix_socket_directories: StrBuf = result.map(|row| row[1]).next().unwrap();

    if unix_socket_directories.is_empty() {
        fail!("can't test connect_unix; unix_socket_directories is empty");
    }

    let unix_socket_directory = unix_socket_directories.as_slice()
            .splitn(',', 1).next().unwrap();

    let url = format!("postgres://postgres@{}", url::encode_component(unix_socket_directory));
    let conn = or_fail!(PostgresConnection::connect(url.as_slice(), &NoSsl));
    assert!(conn.finish().is_ok());
}

#[test]
fn test_transaction_commit() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    let trans = or_fail!(conn.transaction());
    or_fail!(trans.execute("INSERT INTO foo (id) VALUES ($1)", [&1i32 as &ToSql]));
    drop(trans);

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row[1]).collect());
}

#[test]
fn test_transaction_commit_finish() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    let trans = or_fail!(conn.transaction());
    or_fail!(trans.execute("INSERT INTO foo (id) VALUES ($1)", [&1i32 as &ToSql]));
    assert!(trans.finish().is_ok());

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row[1]).collect());
}

#[test]
fn test_transaction_rollback() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    or_fail!(conn.execute("INSERT INTO foo (id) VALUES ($1)", [&1i32 as &ToSql]));

    let trans = or_fail!(conn.transaction());
    or_fail!(trans.execute("INSERT INTO foo (id) VALUES ($1)", [&2i32 as &ToSql]));
    trans.set_rollback();
    drop(trans);

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row[1]).collect());
}

#[test]
fn test_transaction_rollback_finish() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    or_fail!(conn.execute("INSERT INTO foo (id) VALUES ($1)", [&1i32 as &ToSql]));

    let trans = or_fail!(conn.transaction());
    or_fail!(trans.execute("INSERT INTO foo (id) VALUES ($1)", [&2i32 as &ToSql]));
    trans.set_rollback();
    assert!(trans.finish().is_ok());

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row[1]).collect());
}

#[test]
fn test_nested_transactions() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    or_fail!(conn.execute("INSERT INTO foo (id) VALUES (1)", []));

    {
        let trans1 = or_fail!(conn.transaction());
        or_fail!(trans1.execute("INSERT INTO foo (id) VALUES (2)", []));

        {
            let trans2 = or_fail!(trans1.transaction());
            or_fail!(trans2.execute("INSERT INTO foo (id) VALUES (3)", []));
            trans2.set_rollback();
        }

        {
            let trans2 = or_fail!(trans1.transaction());
            or_fail!(trans2.execute("INSERT INTO foo (id) VALUES (4)", []));

            {
                let trans3 = or_fail!(trans2.transaction());
                or_fail!(trans3.execute("INSERT INTO foo (id) VALUES (5)", []));
                trans3.set_rollback();
            }

            {
                let trans3 = or_fail!(trans2.transaction());
                or_fail!(trans3.execute("INSERT INTO foo (id) VALUES (6)", []));
            }
        }

        let stmt = or_fail!(trans1.prepare("SELECT * FROM foo ORDER BY id"));
        let result = or_fail!(stmt.query([]));

        assert_eq!(vec![1i32, 2, 4, 6], result.map(|row| row[1]).collect());

        trans1.set_rollback();
    }

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo ORDER BY id"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row[1]).collect());
}

#[test]
fn test_nested_transactions_finish() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    or_fail!(conn.execute("INSERT INTO foo (id) VALUES (1)", []));

    {
        let trans1 = or_fail!(conn.transaction());
        or_fail!(trans1.execute("INSERT INTO foo (id) VALUES (2)", []));

        {
            let trans2 = or_fail!(trans1.transaction());
            or_fail!(trans2.execute("INSERT INTO foo (id) VALUES (3)", []));
            trans2.set_rollback();
            assert!(trans2.finish().is_ok());
        }

        {
            let trans2 = or_fail!(trans1.transaction());
            or_fail!(trans2.execute("INSERT INTO foo (id) VALUES (4)", []));

            {
                let trans3 = or_fail!(trans2.transaction());
                or_fail!(trans3.execute("INSERT INTO foo (id) VALUES (5)", []));
                trans3.set_rollback();
                assert!(trans3.finish().is_ok());
            }

            {
                let trans3 = or_fail!(trans2.transaction());
                or_fail!(trans3.execute("INSERT INTO foo (id) VALUES (6)", []));
                assert!(trans3.finish().is_ok());
            }

            assert!(trans2.finish().is_ok());
        }

        // in a block to unborrow trans1 for the finish call
        {
            let stmt = or_fail!(trans1.prepare("SELECT * FROM foo ORDER BY id"));
            let result = or_fail!(stmt.query([]));

            assert_eq!(vec![1i32, 2, 4, 6], result.map(|row| row[1]).collect());
        }

        trans1.set_rollback();
        assert!(trans1.finish().is_ok());
    }

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo ORDER BY id"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row[1]).collect());
}

#[test]
fn test_conn_prepare_with_trans() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let _trans = or_fail!(conn.transaction());
    match conn.prepare("") {
        Err(PgWrongTransaction) => {}
        Err(r) => fail!("Unexpected error {}", r),
        Ok(_) => fail!("Unexpected success"),
    }
    match conn.transaction() {
        Err(PgWrongTransaction) => {}
        Err(r) => fail!("Unexpected error {}", r),
        Ok(_) => fail!("Unexpected success"),
    }
}

#[test]
fn test_trans_prepare_with_nested_trans() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let trans = or_fail!(conn.transaction());
    let _trans2 = or_fail!(trans.transaction());
    match trans.prepare("") {
        Err(PgWrongTransaction) => {}
        Err(r) => fail!("Unexpected error {}", r),
        Ok(_) => fail!("Unexpected success"),
    }
    match trans.transaction() {
        Err(PgWrongTransaction) => {}
        Err(r) => fail!("Unexpected error {}", r),
        Ok(_) => fail!("Unexpected success"),
    }
}

#[test]
fn test_stmt_finish() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY)", []));
    let stmt = or_fail!(conn.prepare("SELECT * FROM foo"));
    assert!(stmt.finish().is_ok());
}

#[test]
fn test_query() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY)", []));
    or_fail!(conn.execute("INSERT INTO foo (id) VALUES ($1), ($2)",
                          [&1i64 as &ToSql, &2i64 as &ToSql]));
    let stmt = or_fail!(conn.prepare("SELECT * from foo ORDER BY id"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i64, 2], result.map(|row| row[1]).collect());
}

#[test]
fn test_result_finish() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY)", []));
    let stmt = or_fail!(conn.prepare("SELECT * FROM foo"));
    let result = or_fail!(stmt.query([]));
    assert!(result.finish().is_ok());
}

#[test]
fn test_lazy_query() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));

    let trans = or_fail!(conn.transaction());
    or_fail!(trans.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));
    let stmt = or_fail!(trans.prepare("INSERT INTO foo (id) VALUES ($1)"));
    let values = vec!(0i32, 1, 2, 3, 4, 5);
    for value in values.iter() {
        or_fail!(stmt.execute([value as &ToSql]));
    }
    let stmt = or_fail!(trans.prepare("SELECT id FROM foo ORDER BY id"));
    let result = or_fail!(trans.lazy_query(&stmt, [], 2));
    assert_eq!(values, result.map(|row| row.unwrap()[1]).collect());

    trans.set_rollback();
}

#[test]
fn test_lazy_query_wrong_conn() {
    let conn1 = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let conn2 = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));

    let trans = or_fail!(conn1.transaction());
    let stmt = or_fail!(conn2.prepare("SELECT 1::INT"));
    match trans.lazy_query(&stmt, [], 1) {
        Err(PgWrongConnection) => {}
        Err(err) => fail!("Unexpected error {}", err),
        Ok(_) => fail!("Expected failure")
    }
}

#[test]
fn test_param_types() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SELECT $1::INT, $2::VARCHAR"));
    assert_eq!(stmt.param_types(), &[PgInt4, PgVarchar]);
}

#[test]
fn test_result_descriptions() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SELECT 1::INT as a, 'hi'::VARCHAR as b"));
    assert!(stmt.result_descriptions() ==
            [ResultDescription { name: "a".to_strbuf(), ty: PgInt4},
             ResultDescription { name: "b".to_strbuf(), ty: PgVarchar}]);
}

#[test]
fn test_execute_counts() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    assert_eq!(0, or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (
                                            id SERIAL PRIMARY KEY,
                                            b INT
                                         )", [])));
    assert_eq!(3, or_fail!(conn.execute("INSERT INTO foo (b) VALUES ($1), ($2), ($2)",
                                        [&1i32 as &ToSql, &2i32 as &ToSql])));
    assert_eq!(2, or_fail!(conn.execute("UPDATE foo SET b = 0 WHERE b = 2", [])));
    assert_eq!(3, or_fail!(conn.execute("SELECT * FROM foo", [])));
}

fn test_type<T: Eq+FromSql+ToSql, S: Str>(sql_type: &str, checks: &[(T, S)]) {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    for &(ref val, ref repr) in checks.iter() {
        let stmt = or_fail!(conn.prepare(format!("SELECT {:s}::{}", *repr, sql_type).as_slice()));
        let result = or_fail!(stmt.query([])).next().unwrap()[1];
        assert!(val == &result);

        let stmt = or_fail!(conn.prepare(format!("SELECT $1::{}", sql_type).as_slice()));
        let result = or_fail!(stmt.query([val as &ToSql])).next().unwrap()[1];
        assert!(val == &result);
    }
}

#[test]
fn test_bool_params() {
    test_type("BOOL", [(Some(true), "'t'"), (Some(false), "'f'"),
                       (None, "NULL")]);
}

#[test]
fn test_i8_params() {
    test_type("\"char\"", [(Some('a' as i8), "'a'"), (None, "NULL")]);
}

#[test]
fn test_name_params() {
    test_type("NAME", [(Some("hello world".to_strbuf()), "'hello world'"),
                       (Some("イロハニホヘト チリヌルヲ".to_strbuf()), "'イロハニホヘト チリヌルヲ'"),
                       (None, "NULL")]);
}

#[test]
fn test_i16_params() {
    test_type("SMALLINT", [(Some(15001i16), "15001"),
                           (Some(-15001i16), "-15001"), (None, "NULL")]);
}

#[test]
fn test_i32_params() {
    test_type("INT", [(Some(2147483548i32), "2147483548"),
                      (Some(-2147483548i32), "-2147483548"), (None, "NULL")]);
}

#[test]
fn test_i64_params() {
    test_type("BIGINT", [(Some(9223372036854775708i64), "9223372036854775708"),
                         (Some(-9223372036854775708i64), "-9223372036854775708"),
                         (None, "NULL")]);
}

#[test]
fn test_f32_params() {
    test_type("REAL", [(Some(f32::INFINITY), "'infinity'"),
                       (Some(f32::NEG_INFINITY), "'-infinity'"),
                       (Some(1000.55), "1000.55"), (None, "NULL")]);
}

#[test]
fn test_f64_params() {
    test_type("DOUBLE PRECISION", [(Some(f64::INFINITY), "'infinity'"),
                                   (Some(f64::NEG_INFINITY), "'-infinity'"),
                                   (Some(10000.55), "10000.55"),
                                   (None, "NULL")]);
}

#[test]
fn test_varchar_params() {
    test_type("VARCHAR", [(Some("hello world".to_strbuf()), "'hello world'"),
                          (Some("イロハニホヘト チリヌルヲ".to_strbuf()), "'イロハニホヘト チリヌルヲ'"),
                          (None, "NULL")]);
}

#[test]
fn test_text_params() {
    test_type("TEXT", [(Some("hello world".to_strbuf()), "'hello world'"),
                       (Some("イロハニホヘト チリヌルヲ".to_strbuf()), "'イロハニホヘト チリヌルヲ'"),
                       (None, "NULL")]);
}

#[test]
fn test_bpchar_params() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (
                            id SERIAL PRIMARY KEY,
                            b CHAR(5)
                           )", []));
    or_fail!(conn.execute("INSERT INTO foo (b) VALUES ($1), ($2), ($3)",
                          [&Some("12345") as &ToSql, &Some("123") as &ToSql,
                           &None::<&'static str> as &ToSql]));
    let stmt = or_fail!(conn.prepare("SELECT b FROM foo ORDER BY id"));
    let res = or_fail!(stmt.query([]));

    assert_eq!(vec!(Some("12345".to_strbuf()), Some("123  ".to_strbuf()), None),
               res.map(|row| row[1]).collect());
}

#[test]
fn test_bytea_params() {
    test_type("BYTEA", [(Some(vec!(0u8, 1, 2, 3, 254, 255)), "'\\x00010203feff'"),
                        (None, "NULL")]);
}

#[test]
fn test_json_params() {
    test_type("JSON", [(Some(json::from_str("[10, 11, 12]").unwrap()),
                        "'[10, 11, 12]'"),
                       (Some(json::from_str("{\"f\": \"asd\"}").unwrap()),
                        "'{\"f\": \"asd\"}'"),
                       (None, "NULL")])
}

#[test]
fn test_uuid_params() {
    test_type("UUID", [(Some(Uuid::parse_string("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11").unwrap()),
                        "'a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11'"),
                       (None, "NULL")])
}

#[test]
fn test_tm_params() {
    fn make_check<'a>(time: &'a str) -> (Option<Timespec>, &'a str) {
        (Some(time::strptime(time, "'%Y-%m-%d %H:%M:%S.%f'").unwrap().to_timespec()), time)
    }
    test_type("TIMESTAMP",
              [make_check("'1970-01-01 00:00:00.01'"),
               make_check("'1965-09-25 11:19:33.100314'"),
               make_check("'2010-02-09 23:11:45.1202'"),
               (None, "NULL")]);
    test_type("TIMESTAMP WITH TIME ZONE",
              [make_check("'1970-01-01 00:00:00.01'"),
               make_check("'1965-09-25 11:19:33.100314'"),
               make_check("'2010-02-09 23:11:45.1202'"),
               (None, "NULL")]);
}

macro_rules! test_range(
    ($name:expr, $t:ty, $low:expr, $low_str:expr, $high:expr, $high_str:expr) => ({
        let tests = [(Some(range!('(', ')')), "'(,)'".to_strbuf()),
                     (Some(range!('[' $low, ')')), format!("'[{},)'", $low_str)),
                     (Some(range!('(' $low, ')')), format!("'({},)'", $low_str)),
                     (Some(range!('(', $high ']')), format!("'(,{}]'", $high_str)),
                     (Some(range!('(', $high ')')), format!("'(,{})'", $high_str)),
                     (Some(range!('[' $low, $high ']')),
                      format!("'[{},{}]'", $low_str, $high_str)),
                     (Some(range!('[' $low, $high ')')),
                      format!("'[{},{})'", $low_str, $high_str)),
                     (Some(range!('(' $low, $high ']')),
                      format!("'({},{}]'", $low_str, $high_str)),
                     (Some(range!('(' $low, $high ')')),
                      format!("'({},{})'", $low_str, $high_str)),
                     (Some(range!(empty)), "'empty'".to_strbuf()),
                     (None, "NULL".to_strbuf())];
        test_type($name, tests);
    })
)

#[test]
fn test_int4range_params() {
    test_range!("INT4RANGE", i32, 100i32, "100", 200i32, "200")
}

#[test]
fn test_int8range_params() {
    test_range!("INT8RANGE", i64, 100i64, "100", 200i64, "200")
}

fn test_timespec_range_params(sql_type: &str) {
    fn t(time: &str) -> Timespec {
        time::strptime(time, "%Y-%m-%d").unwrap().to_timespec()
    }
    let low = "1970-01-01";
    let high = "1980-01-01";
    test_range!(sql_type, Timespec, t(low), low, t(high), high);
}

#[test]
fn test_tsrange_params() {
    test_timespec_range_params("TSRANGE");
}

#[test]
fn test_tstzrange_params() {
    test_timespec_range_params("TSTZRANGE");
}

macro_rules! test_array_params(
    ($name:expr, $v1:expr, $s1:expr, $v2:expr, $s2:expr, $v3:expr, $s3:expr) => ({
        let tests = [(Some(ArrayBase::from_vec(vec!(Some($v1), Some($v2), None), 1)),
                      format!(r"'\{{},{},NULL\}'", $s1, $s2).into_strbuf()),
                     (None, "NULL".to_strbuf())];
        test_type(format!("{}[]", $name).as_slice(), tests);
        let mut a = ArrayBase::from_vec(vec!(Some($v1), Some($v2)), 0);
        a.wrap(-1);
        a.push_move(ArrayBase::from_vec(vec!(None, Some($v3)), 0));
        let tests = [(Some(a), format!(r"'[-1:0][0:1]=\{\{{},{}\},\{NULL,{}\}\}'",
                                       $s1, $s2, $s3).into_strbuf())];
        test_type(format!("{}[][]", $name).as_slice(), tests);
    })
)

#[test]
fn test_boolarray_params() {
    test_array_params!("BOOL", false, "f", true, "t", true, "t");
}

#[test]
fn test_byteaarray_params() {
    test_array_params!("BYTEA", vec!(0u8, 1), r#""\\x0001""#, vec!(254u8, 255u8),
                       r#""\\xfeff""#, vec!(10u8, 11u8), r#""\\x0a0b""#);
}

#[test]
fn test_chararray_params() {
    test_array_params!("\"char\"", 'a' as i8, "a", 'z' as i8, "z",
                       '0' as i8, "0");
}

#[test]
fn test_namearray_params() {
    test_array_params!("NAME", "hello".to_strbuf(), "hello", "world".to_strbuf(),
                       "world", "!".to_strbuf(), "!");
}

#[test]
fn test_int2array_params() {
    test_array_params!("INT2", 0i16, "0", 1i16, "1", 2i16, "2");
}

#[test]
fn test_int4array_params() {
    test_array_params!("INT4", 0i32, "0", 1i32, "1", 2i32, "2");
}

#[test]
fn test_textarray_params() {
    test_array_params!("TEXT", "hello".to_strbuf(), "hello", "world".to_strbuf(),
                       "world", "!".to_strbuf(), "!");
}

#[test]
fn test_charnarray_params() {
    test_array_params!("CHAR(5)", "hello".to_strbuf(), "hello",
                       "world".to_strbuf(), "world", "!    ".to_strbuf(), "!");
}

#[test]
fn test_varchararray_params() {
    test_array_params!("VARCHAR", "hello".to_strbuf(), "hello",
                       "world".to_strbuf(), "world", "!".to_strbuf(), "!");
}

#[test]
fn test_int8array_params() {
    test_array_params!("INT8", 0i64, "0", 1i64, "1", 2i64, "2");
}

#[test]
fn test_timestamparray_params() {
    fn make_check<'a>(time: &'a str) -> (Timespec, &'a str) {
        (time::strptime(time, "%Y-%m-%d %H:%M:%S.%f").unwrap().to_timespec(), time)
    }
    let (v1, s1) = make_check("1970-01-01 00:00:00.01");
    let (v2, s2) = make_check("1965-09-25 11:19:33.100314");
    let (v3, s3) = make_check("2010-02-09 23:11:45.1202");
    test_array_params!("TIMESTAMP", v1, s1, v2, s2, v3, s3);
    test_array_params!("TIMESTAMPTZ", v1, s1, v2, s2, v3, s3);
}

#[test]
fn test_float4array_params() {
    test_array_params!("FLOAT4", 0f32, "0", 1.5f32, "1.5", 0.009f32, ".009");
}

#[test]
fn test_float8array_params() {
    test_array_params!("FLOAT8", 0f64, "0", 1.5f64, "1.5", 0.009f64, ".009");
}

#[test]
fn test_uuidarray_params() {
    fn make_check<'a>(uuid: &'a str) -> (Uuid, &'a str) {
        (Uuid::parse_string(uuid).unwrap(), uuid)
    }
    let (v1, s1) = make_check("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    let (v2, s2) = make_check("00000000-0000-0000-0000-000000000000");
    let (v3, s3) = make_check("a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11");
    test_array_params!("UUID", v1, s1, v2, s2, v3, s3);
}

#[test]
fn test_int4rangearray_params() {
    test_array_params!("INT4RANGE",
                       Range::new(None, None), "\"(,)\"",
                       Range::new(Some(RangeBound::new(10i32, Inclusive)), None), "\"[10,)\"",
                       Range::new(None, Some(RangeBound::new(10i32, Exclusive))), "\"(,10)\"");
}

#[test]
fn test_tsrangearray_params() {
    fn make_check<'a>(time: &'a str) -> (Timespec, &'a str) {
        (time::strptime(time, "%Y-%m-%d").unwrap().to_timespec(), time)
    }
    let (v1, s1) = make_check("1970-10-11");
    let (v2, s2) = make_check("1990-01-01");
    let r1 = Range::new(None, None);
    let rs1 = "\"(,)\"";
    let r2 = Range::new(Some(RangeBound::new(v1, Inclusive)), None);
    let rs2 = format!("\"[{},)\"", s1);
    let r3 = Range::new(None, Some(RangeBound::new(v2, Exclusive)));
    let rs3 = format!("\"(,{})\"", s2);
    test_array_params!("TSRANGE", r1, rs1, r2, rs2, r3, rs3);
    test_array_params!("TSTZRANGE", r1, rs1, r2, rs2, r3, rs3);
}

#[test]
fn test_int8rangearray_params() {
    test_array_params!("INT8RANGE",
                       Range::new(None, None), "\"(,)\"",
                       Range::new(Some(RangeBound::new(10i64, Inclusive)), None), "\"[10,)\"",
                       Range::new(None, Some(RangeBound::new(10i64, Exclusive))), "\"(,10)\"");
}

#[test]
fn test_hstore_params() {
    macro_rules! make_map(
        ($($k:expr => $v:expr),+) => ({
            let mut map = HashMap::new();
            $(map.insert($k, $v);)+
            map
        })
    )
    test_type("hstore",
              [(Some(make_map!("a".to_strbuf() => Some("1".to_strbuf()))), "'a=>1'"),
               (Some(make_map!("hello".to_strbuf() => Some("world!".to_strbuf()),
                               "hola".to_strbuf() => Some("mundo!".to_strbuf()),
                               "what".to_strbuf() => None)),
                "'hello=>world!,hola=>mundo!,what=>NULL'"),
                (None, "NULL")]);
}

fn test_nan_param<T: Float+ToSql+FromSql>(sql_type: &str) {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare(format!("SELECT 'NaN'::{}", sql_type).as_slice()));
    let mut result = or_fail!(stmt.query([]));
    let val: T = result.next().unwrap()[1];
    assert!(val.is_nan());

    let nan: T = Float::nan();
    let stmt = or_fail!(conn.prepare(format!("SELECT $1::{}", sql_type).as_slice()));
    let mut result = or_fail!(stmt.query([&nan as &ToSql]));
    let val: T = result.next().unwrap()[1];
    assert!(val.is_nan())
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
fn test_wrong_param_type() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    match conn.execute("SELECT $1::VARCHAR", [&1i32 as &ToSql]) {
        Err(PgWrongType(_)) => {}
        res => fail!("unexpected result {}", res)
    }
}

#[test]
fn test_too_few_params() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    match conn.execute("SELECT $1::INT, $2::INT", [&1i32 as &ToSql]) {
        Err(PgWrongParamCount { expected: 2, actual: 1 }) => {},
        res => fail!("unexpected result {}", res)
    }
}

#[test]
fn test_too_many_params() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    match conn.execute("SELECT $1::INT, $2::INT", [&1i32 as &ToSql,
                                                   &2i32 as &ToSql,
                                                   &3i32 as &ToSql]) {
        Err(PgWrongParamCount { expected: 2, actual: 3 }) => {},
        res => fail!("unexpected result {}", res)
    }
}

#[test]
fn test_index_named() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SELECT 10::INT as val"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![10i32], result.map(|row| row["val"]).collect());
}

#[test]
#[should_fail]
fn test_index_named_fail() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SELECT 10::INT as id"));
    let mut result = or_fail!(stmt.query([]));

    let _: i32 = result.next().unwrap()["asdf"];
}

#[test]
fn test_get_named_err() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SELECT 10::INT as id"));
    let mut result = or_fail!(stmt.query([]));

    match result.next().unwrap().get::<&str, i32>("asdf") {
        Err(PgInvalidColumn) => {}
        res => fail!("unexpected result {}", res),
    };
}

#[test]
fn test_get_was_null() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SELECT NULL::INT as id"));
    let mut result = or_fail!(stmt.query([]));

    match result.next().unwrap().get::<uint, i32>(1) {
        Err(PgWasNull) => {}
        res => fail!("unexpected result {}", res),
    };
}

#[test]
fn test_custom_notice_handler() {
    static mut count: uint = 0;
    struct Handler;

    impl PostgresNoticeHandler for Handler {
        fn handle(&mut self, notice: PostgresDbError) {
            assert_eq!("note", notice.message.as_slice());
            unsafe { count += 1; }
        }
    }

    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost?client_min_messages=NOTICE", &NoSsl));
    conn.set_notice_handler(box Handler);
    or_fail!(conn.execute("CREATE FUNCTION pg_temp.note() RETURNS INT AS $$
                           BEGIN
                            RAISE NOTICE 'note';
                            RETURN 1;
                           END; $$ LANGUAGE plpgsql", []));
    or_fail!(conn.execute("SELECT pg_temp.note()", []));

    assert_eq!(unsafe { count }, 1);
}

#[test]
fn test_notification_iterator_none() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    assert!(conn.notifications().next().is_none());
}

#[test]
fn test_notification_iterator_some() {
    fn check_notification(expected: PostgresNotification,
                          actual: Option<PostgresNotification>) {
        match actual {
            Some(PostgresNotification { channel, payload, .. }) => {
                assert_eq!(&expected.channel, &channel);
                assert_eq!(&expected.payload, &payload);
            }
            x => fail!("Expected {:?} but got {:?}", expected, x)
        }
    }

    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let mut it = conn.notifications();
    or_fail!(conn.execute("LISTEN test_notification_iterator_one_channel", []));
    or_fail!(conn.execute("LISTEN test_notification_iterator_one_channel2", []));
    or_fail!(conn.execute("NOTIFY test_notification_iterator_one_channel, 'hello'", []));
    or_fail!(conn.execute("NOTIFY test_notification_iterator_one_channel2, 'world'", []));

    check_notification(PostgresNotification {
        pid: 0,
        channel: "test_notification_iterator_one_channel".to_strbuf(),
        payload: "hello".to_strbuf()
    }, it.next());
    check_notification(PostgresNotification {
        pid: 0,
        channel: "test_notification_iterator_one_channel2".to_strbuf(),
        payload: "world".to_strbuf()
    }, it.next());
    assert!(it.next().is_none());

    or_fail!(conn.execute("NOTIFY test_notification_iterator_one_channel, '!'", []));
    check_notification(PostgresNotification {
        pid: 0,
        channel: "test_notification_iterator_one_channel".to_strbuf(),
        payload: "!".to_strbuf()
    }, it.next());
    assert!(it.next().is_none());
}

#[test]
// This test is pretty sad, but I don't think there's a better way :(
fn test_cancel_query() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let cancel_data = conn.cancel_data();

    spawn(proc() {
        timer::sleep(500);
        assert!(super::cancel_query("postgres://postgres@localhost", &NoSsl,
                                    cancel_data).is_ok());
    });

    match conn.execute("SELECT pg_sleep(10)", []) {
        Err(PgDbError(PostgresDbError { code: QueryCanceled, .. })) => {}
        res => fail!("Unexpected result {:?}", res)
    }
}

#[test]
fn test_require_ssl_conn() {
    let ctx = SslContext::new(Sslv3);
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost",
                                                    &RequireSsl(ctx)));
    or_fail!(conn.execute("SELECT 1::VARCHAR", []));
}

#[test]
fn test_prefer_ssl_conn() {
    let ctx = SslContext::new(Sslv3);
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost",
                                                    &PreferSsl(ctx)));
    or_fail!(conn.execute("SELECT 1::VARCHAR", []));
}

#[test]
fn test_plaintext_pass() {
    or_fail!(PostgresConnection::connect("postgres://pass_user:password@localhost/postgres", &NoSsl));
}

#[test]
fn test_plaintext_pass_no_pass() {
    let ret = PostgresConnection::connect("postgres://pass_user@localhost/postgres", &NoSsl);
    match ret {
        Err(MissingPassword) => (),
        Err(err) => fail!("Unexpected error {}", err),
        _ => fail!("Expected error")
    }
}

#[test]
fn test_plaintext_pass_wrong_pass() {
    let ret = PostgresConnection::connect("postgres://pass_user:asdf@localhost/postgres", &NoSsl);
    match ret {
        Err(PgConnectDbError(PostgresDbError { code: InvalidPassword, .. })) => (),
        Err(err) => fail!("Unexpected error {}", err),
        _ => fail!("Expected error")
    }
}

 #[test]
fn test_md5_pass() {
    or_fail!(PostgresConnection::connect("postgres://md5_user:password@localhost/postgres", &NoSsl));
}

#[test]
fn test_md5_pass_no_pass() {
    let ret = PostgresConnection::connect("postgres://md5_user@localhost/postgres", &NoSsl);
    match ret {
        Err(MissingPassword) => (),
        Err(err) => fail!("Unexpected error {}", err),
        _ => fail!("Expected error")
    }
}

#[test]
fn test_md5_pass_wrong_pass() {
    let ret = PostgresConnection::connect("postgres://md5_user:asdf@localhost/postgres", &NoSsl);
    match ret {
        Err(PgConnectDbError(PostgresDbError { code: InvalidPassword, .. })) => (),
        Err(err) => fail!("Unexpected error {}", err),
        _ => fail!("Expected error")
    }
}

#[test]
fn test_jsonarray_params() {
    test_array_params!("JSON",
                       json::from_str("[10, 11, 12]").unwrap(),
                       "\"[10,11,12]\"",
                       json::from_str(r#"{"a": 10, "b": null}"#).unwrap(),
                       r#""{\"a\": 10, \"b\": null}""#,
                       json::from_str(r#"{"a": [10], "b": true}"#).unwrap(),
                       r#""{\"a\": [10], \"b\": true}""#);
}

#[test]
fn test_pg_database_datname() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SELECT datname FROM pg_database"));
    let mut result = or_fail!(stmt.query([]));

    let next = result.next().unwrap();
    or_fail!(next.get::<uint, StrBuf>(1));
    or_fail!(next.get::<&str, StrBuf>("datname"));
}

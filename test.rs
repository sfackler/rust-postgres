#[feature(struct_variant, macro_rules, globs)];

extern mod extra;
extern mod ssl = "github.com/sfackler/rust-ssl";

use extra::comm::DuplexStream;
use extra::future::Future;
use extra::time;
use extra::time::Timespec;
use extra::json;
use extra::uuid::Uuid;
use ssl::{SslContext, Sslv3};
use std::f32;
use std::f64;
use std::io::timer;

use lib::{PostgresNoticeHandler,
          PostgresNotification,
          PostgresConnection,
          PostgresStatement,
          ResultDescription,
          RequireSsl,
          PreferSsl,
          NoSsl};
use lib::error::{DbError,
                 DnsError,
                 MissingPassword,
                 Position,
                 PostgresDbError,
                 SyntaxError,
                 InvalidPassword,
                 QueryCanceled,
                 InvalidCatalogName};
use lib::types::{ToSql, FromSql, PgInt4, PgVarchar};
use lib::types::array::{ArrayBase};
use lib::types::range::{Range, Inclusive, Exclusive, RangeBound};
use lib::pool::PostgresConnectionPool;

mod lib;

#[test]
// Make sure we can take both connections at once and can still get one after
fn test_pool() {
    let pool = PostgresConnectionPool::new("postgres://postgres@localhost",
                                           NoSsl, 2);

    let (stream1, stream2) = DuplexStream::<(), ()>();

    let pool1 = pool.clone();
    let mut fut1 = do Future::spawn {
        let _conn = pool1.get_connection();
        stream1.send(());
        stream1.recv();
    };

    let pool2 = pool.clone();
    let mut fut2 = do Future::spawn {
        let _conn = pool2.get_connection();
        stream2.send(());
        stream2.recv();
    };

    fut1.get();
    fut2.get();

    pool.get_connection();
}

#[test]
fn test_non_default_database() {
    PostgresConnection::connect("postgres://postgres@localhost/postgres", &NoSsl);
}

#[test]
fn test_url_terminating_slash() {
    PostgresConnection::connect("postgres://postgres@localhost/", &NoSsl);
}

#[test]
fn test_prepare_err() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    match conn.try_prepare("invalid sql statment") {
        Err(PostgresDbError { code: SyntaxError, position: Some(Position(1)), .. }) => (),
        resp => fail!("Unexpected result {:?}", resp)
    }
}

#[test]
fn test_unknown_database() {
    match PostgresConnection::try_connect("postgres://postgres@localhost/asdf", &NoSsl) {
        Err(DbError(PostgresDbError { code: InvalidCatalogName, .. })) => {}
        resp => fail!("Unexpected result {:?}", resp)
    }
}

#[test]
fn test_transaction_commit() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    conn.update("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []);

    {
        let trans = conn.transaction();
        trans.update("INSERT INTO foo (id) VALUES ($1)", [&1i32 as &ToSql]);
    }

    let stmt = conn.prepare("SELECT * FROM foo");
    let result = stmt.query([]);

    assert_eq!(~[1i32], result.map(|row| { row[0] }).collect());
}

#[test]
fn test_transaction_rollback() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    conn.update("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []);

    conn.update("INSERT INTO foo (id) VALUES ($1)", [&1i32 as &ToSql]);
    {
        let trans = conn.transaction();
        trans.update("INSERT INTO foo (id) VALUES ($1)", [&2i32 as &ToSql]);
        trans.set_rollback();
    }

    let stmt = conn.prepare("SELECT * FROM foo");
    let result = stmt.query([]);

    assert_eq!(~[1i32], result.map(|row| row[0]).collect());
}

#[test]
fn test_nested_transactions() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    conn.update("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []);

    conn.update("INSERT INTO foo (id) VALUES (1)", []);

    {
        let trans1 = conn.transaction();
        trans1.update("INSERT INTO foo (id) VALUES (2)", []);

        {
            let trans2 = trans1.transaction();
            trans2.update("INSERT INTO foo (id) VALUES (3)", []);
            trans2.set_rollback();
        }

        {
            let trans2 = trans1.transaction();
            trans2.update("INSERT INTO foo (id) VALUES (4)", []);

            {
                let trans3 = trans2.transaction();
                trans3.update("INSERT INTO foo (id) VALUES (5)", []);
                trans3.set_rollback();
            }

            {
                let trans3 = trans2.transaction();
                trans3.update("INSERT INTO foo (id) VALUES (6)", []);
            }
        }

        let stmt = conn.prepare("SELECT * FROM foo ORDER BY id");
        let result = stmt.query([]);

        assert_eq!(~[1i32, 2, 4, 6], result.map(|row| row[0]).collect());

        trans1.set_rollback();
    }

    let stmt = conn.prepare("SELECT * FROM foo ORDER BY id");
    let result = stmt.query([]);

    assert_eq!(~[1i32], result.map(|row| row[0]).collect());
}

#[test]
fn test_query() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    conn.update("CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY)", []);
    conn.update("INSERT INTO foo (id) VALUES ($1), ($2)",
                 [&1i64 as &ToSql, &2i64 as &ToSql]);
    let stmt = conn.prepare("SELECT * from foo ORDER BY id");
    let result = stmt.query([]);

    assert_eq!(~[1i64, 2], result.map(|row| row[0]).collect());
}

#[test]
fn test_lazy_query() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);

    {
        let trans = conn.transaction();
        trans.update("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []);
        let stmt = trans.prepare("INSERT INTO foo (id) VALUES ($1)");
        let values = ~[0i32, 1, 2, 3, 4, 5];
        for value in values.iter() {
            stmt.update([value as &ToSql]);
        }

        let stmt = trans.prepare("SELECT id FROM foo ORDER BY id");
        let result = stmt.lazy_query(2, []);
        assert_eq!(values, result.map(|row| row[0]).collect());

        trans.set_rollback();
    }
}

#[test]
fn test_param_types() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    let stmt = conn.prepare("SELECT $1::INT, $2::VARCHAR");
    assert_eq!(stmt.param_types(), [PgInt4, PgVarchar]);
}

#[test]
fn test_result_descriptions() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    let stmt = conn.prepare("SELECT 1::INT as a, 'hi'::VARCHAR as b");
    assert_eq!(stmt.result_descriptions(),
               [ResultDescription { name: ~"a", ty: PgInt4},
                ResultDescription { name: ~"b", ty: PgVarchar}]);
}

fn test_type<T: Eq+FromSql+ToSql>(sql_type: &str, checks: &[(T, &str)]) {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    for &(ref val, ref repr) in checks.iter() {
        let stmt = conn.prepare("SELECT " + *repr + "::" + sql_type);
        let result = stmt.query([]).next().unwrap()[0];
        assert_eq!(val, &result);

        let stmt = conn.prepare("SELECT $1::" + sql_type);
        let result = stmt.query([val as &ToSql]).next().unwrap()[0];
        assert_eq!(val, &result);
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
    test_type("VARCHAR", [(Some(~"hello world"), "'hello world'"),
                          (Some(~"イロハニホヘト チリヌルヲ"), "'イロハニホヘト チリヌルヲ'"),
                          (None, "NULL")]);
}

#[test]
fn test_text_params() {
    test_type("TEXT", [(Some(~"hello world"), "'hello world'"),
                       (Some(~"イロハニホヘト チリヌルヲ"), "'イロハニホヘト チリヌルヲ'"),
                       (None, "NULL")]);
}

#[test]
fn test_bpchar_params() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
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
               res.map(|row| row[0]).collect());
}

#[test]
fn test_bytea_params() {
    test_type("BYTEA", [(Some(~[0u8, 1, 2, 3, 254, 255]), "'\\x00010203feff'"),
                        (None, "NULL")]);
}

#[test]
#[ignore(cfg(travis))]
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
        let tests = [(Some(Range::new(None, None)), ~"'(,)'"),
                     (Some(Range::new(Some(RangeBound::new($low, Inclusive)),
                                      None)),
                      "'[" + $low_str + ",)'"),
                     (Some(Range::new(Some(RangeBound::new($low, Exclusive)),
                                      None)),
                      "'(" + $low_str + ",)'"),
                     (Some(Range::new(None,
                                      Some(RangeBound::new($high, Inclusive)))),
                      "'(," + $high_str + "]'"),
                     (Some(Range::new(None,
                                      Some(RangeBound::new($high, Exclusive)))),
                      "'(," + $high_str + ")'"),
                     (Some(Range::new(Some(RangeBound::new($low, Inclusive)),
                                      Some(RangeBound::new($high, Inclusive)))),
                      "'[" + $low_str + "," + $high_str + "]'"),
                     (Some(Range::new(Some(RangeBound::new($low, Inclusive)),
                                      Some(RangeBound::new($high, Exclusive)))),
                      "'[" + $low_str + "," + $high_str + ")'"),
                     (Some(Range::new(Some(RangeBound::new($low, Exclusive)),
                                      Some(RangeBound::new($high, Inclusive)))),
                      "'(" + $low_str + "," + $high_str + "]'"),
                     (Some(Range::new(Some(RangeBound::new($low, Exclusive)),
                                      Some(RangeBound::new($high, Exclusive)))),
                      "'(" + $low_str + "," + $high_str + ")'"),
                     (Some(Range::empty()), ~"'empty'"),
                     (None, ~"NULL")];
        let test_refs: ~[(Option<Range<$t>>, &str)] = tests.iter()
                .map(|&(ref val, ref s)| { (val.clone(), s.as_slice()) })
                .collect();
        test_type($name, test_refs);
    })
)

#[test]
#[ignore(cfg(travis))]
fn test_int4range_params() {
    test_range!("INT4RANGE", i32, 100i32, "100", 200i32, "200")
}

#[test]
#[ignore(cfg(travis))]
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
#[ignore(cfg(travis))]
fn test_tsrange_params() {
    test_timespec_range_params("TSRANGE");
}

#[test]
#[ignore(cfg(travis))]
fn test_tstzrange_params() {
    test_timespec_range_params("TSTZRANGE");
}

#[test]
fn test_int4array_params() {
    test_type("INT4[]",
              [(Some(ArrayBase::from_vec(~[Some(0i32), Some(1), None], 1)),
                "'{0,1,NULL}'"),
               (None, "NULL")]);
    let mut a = ArrayBase::from_vec(~[Some(0i32), Some(1)], 0);
    a.wrap(-1);
    a.push_move(ArrayBase::from_vec(~[None, Some(3)], 0));
    test_type("INT4[][]",
              [(Some(a), "'[-1:0][0:1]={{0,1},{NULL,3}}'")]);
}

fn test_nan_param<T: Float+ToSql+FromSql>(sql_type: &str) {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    let stmt = conn.prepare("SELECT 'NaN'::" + sql_type);
    let mut result = stmt.query([]);
    let val: T = result.next().unwrap()[0];
    assert!(val.is_nan());

    let nan: T = Float::nan();
    let stmt = conn.prepare("SELECT $1::" + sql_type);
    let mut result = stmt.query([&nan as &ToSql]);
    let val: T = result.next().unwrap()[0];
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
#[should_fail]
fn test_wrong_param_type() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    conn.try_update("SELECT $1::VARCHAR", [&1i32 as &ToSql]);
}

#[test]
#[should_fail]
fn test_too_few_params() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    conn.try_update("SELECT $1::INT, $2::INT", [&1i32 as &ToSql]);
}

#[test]
#[should_fail]
fn test_too_many_params() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    conn.try_update("SELECT $1::INT, $2::INT", [&1i32 as &ToSql,
                                                &2i32 as &ToSql,
                                                &3i32 as &ToSql]);
}

#[test]
fn test_get_named() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    let stmt = conn.prepare("SELECT 10::INT as val");
    let result = stmt.query([]);

    assert_eq!(~[10i32], result.map(|row| { row["val"] }).collect());
}

#[test]
#[should_fail]
fn test_get_named_fail() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    let stmt = conn.prepare("SELECT 10::INT as id");
    let mut result = stmt.query([]);

    let _: i32 = result.next().unwrap()["asdf"];
}

#[test]
fn test_custom_notice_handler() {
    static mut count: uint = 0;
    struct Handler;

    impl PostgresNoticeHandler for Handler {
        fn handle(&mut self, _notice: PostgresDbError) {
            unsafe { count += 1; }
        }
    }

    let conn = PostgresConnection::connect("postgres://postgres@localhost?client_min_messages=NOTICE", &NoSsl);
    conn.set_notice_handler(~Handler as ~PostgresNoticeHandler);
    conn.update("CREATE FUNCTION pg_temp.note() RETURNS INT AS $$
                BEGIN
                    RAISE NOTICE 'note';
                    RETURN 1;
                END; $$ LANGUAGE plpgsql", []);
    conn.update("SELECT pg_temp.note()", []);

    assert_eq!(unsafe { count }, 1);
}

#[test]
fn test_notification_iterator_none() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
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

    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    let mut it = conn.notifications();
    conn.update("LISTEN test_notification_iterator_one_channel", []);
    conn.update("LISTEN test_notification_iterator_one_channel2", []);
    conn.update("NOTIFY test_notification_iterator_one_channel, 'hello'", []);
    conn.update("NOTIFY test_notification_iterator_one_channel2, 'world'", []);

    check_notification(PostgresNotification {
        pid: 0,
        channel: ~"test_notification_iterator_one_channel",
        payload: ~"hello"
    }, it.next());
    check_notification(PostgresNotification {
        pid: 0,
        channel: ~"test_notification_iterator_one_channel2",
        payload: ~"world"
    }, it.next());
    assert!(it.next().is_none());

    conn.update("NOTIFY test_notification_iterator_one_channel, '!'", []);
    check_notification(PostgresNotification {
        pid: 0,
        channel: ~"test_notification_iterator_one_channel",
        payload: ~"!"
    }, it.next());
    assert!(it.next().is_none());
}

#[test]
// This test is pretty sad, but I don't think there's a better way :(
fn test_cancel_query() {
    let conn = PostgresConnection::connect("postgres://postgres@localhost", &NoSsl);
    let cancel_data = conn.cancel_data();

    do spawn {
        timer::sleep(500);
        assert!(lib::cancel_query("postgres://postgres@localhost", &NoSsl,
                                    cancel_data).is_none());
    }

    match conn.try_update("SELECT pg_sleep(10)", []) {
        Err(PostgresDbError { code: QueryCanceled, .. }) => {}
        res => fail!("Unexpected result {:?}", res)
    }
}

#[test]
fn test_require_ssl_conn() {
    let ctx = SslContext::new(Sslv3);
    let conn = PostgresConnection::connect("postgres://postgres@localhost",
                                           &RequireSsl(ctx));
    conn.update("SELECT 1::VARCHAR", []);
}

#[test]
fn test_prefer_ssl_conn() {
    let ctx = SslContext::new(Sslv3);
    let conn = PostgresConnection::connect("postgres://postgres@localhost",
                                           &PreferSsl(ctx));
    conn.update("SELECT 1::VARCHAR", []);
}

#[test]
fn test_plaintext_pass() {
    PostgresConnection::connect("postgres://pass_user:password@localhost/postgres", &NoSsl);
}

#[test]
fn test_plaintext_pass_no_pass() {
    let ret = PostgresConnection::try_connect("postgres://pass_user@localhost/postgres", &NoSsl);
    match ret {
        Err(MissingPassword) => (),
        Err(err) => fail!("Unexpected error {}", err.to_str()),
        _ => fail!("Expected error")
    }
}

#[test]
fn test_plaintext_pass_wrong_pass() {
    let ret = PostgresConnection::try_connect("postgres://pass_user:asdf@localhost/postgres", &NoSsl);
    match ret {
        Err(DbError(PostgresDbError { code: InvalidPassword, .. })) => (),
        Err(err) => fail!("Unexpected error {}", err.to_str()),
        _ => fail!("Expected error")
    }
}

 #[test]
fn test_md5_pass() {
    PostgresConnection::connect("postgres://md5_user:password@localhost/postgres", &NoSsl);
}

#[test]
fn test_md5_pass_no_pass() {
    let ret = PostgresConnection::try_connect("postgres://md5_user@localhost/postgres", &NoSsl);
    match ret {
        Err(MissingPassword) => (),
        Err(err) => fail!("Unexpected error {}", err.to_str()),
        _ => fail!("Expected error")
    }
}

#[test]
fn test_md5_pass_wrong_pass() {
    let ret = PostgresConnection::try_connect("postgres://md5_user:asdf@localhost/postgres", &NoSsl);
    match ret {
        Err(DbError(PostgresDbError { code: InvalidPassword, .. })) => (),
        Err(err) => fail!("Unexpected error {}", err.to_str()),
        _ => fail!("Expected error")
    }
}

#[test]
fn test_dns_failure() {
    let ret = PostgresConnection::try_connect("postgres://postgres@asdfasdfasdf", &NoSsl);
    match ret {
        Err(DnsError) => (),
        Err(err) => fail!("Unexpected error {}", err.to_str()),
        _ => fail!("Expected error")
    }
}

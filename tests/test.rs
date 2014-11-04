#![feature(macro_rules, phase, slicing_syntax)]

#[phase(plugin, link)]
extern crate postgres;
extern crate serialize;
extern crate time;
extern crate url;
extern crate openssl;

use openssl::ssl::{SslContext, Sslv3};
use std::io::timer;
use std::time::Duration;

use postgres::{NoticeHandler,
               Notification,
               Connection,
               GenericConnection,
               ResultDescription,
               RequireSsl,
               PreferSsl,
               NoSsl,
               Type,
               ToSql};
use postgres::error::{PgConnectDbError,
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
                      UndefinedTable,
                      InvalidCatalogName,
                      PgWrongTransaction,
                      CardinalityViolation};

macro_rules! or_panic(
    ($e:expr) => (
        match $e {
            Ok(ok) => ok,
            Err(err) => panic!("{}", err)
        }
    )
)

mod types;

#[test]
fn test_non_default_database() {
    or_panic!(Connection::connect("postgres://postgres@localhost/postgres", &NoSsl));
}

#[test]
fn test_url_terminating_slash() {
    or_panic!(Connection::connect("postgres://postgres@localhost/", &NoSsl));
}

#[test]
fn test_prepare_err() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    match conn.prepare("invalid sql statment") {
        Err(PgDbError(PostgresDbError { code: SyntaxError, position: Some(Position(1)), .. })) => (),
        Err(e) => panic!("Unexpected result {}", e),
        _ => panic!("Unexpected result"),
    }
}

#[test]
fn test_unknown_database() {
    match Connection::connect("postgres://postgres@localhost/asdf", &NoSsl) {
        Err(PgConnectDbError(PostgresDbError { code: InvalidCatalogName, .. })) => {}
        Err(resp) => panic!("Unexpected result {}", resp),
        _ => panic!("Unexpected result"),
    }
}

#[test]
fn test_connection_finish() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    assert!(conn.finish().is_ok());
}

#[test]
fn test_unix_connection() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_panic!(conn.prepare("SHOW unix_socket_directories"));
    let result = or_panic!(stmt.query([]));
    let unix_socket_directories: String = result.map(|row| row.get(0)).next().unwrap();

    if unix_socket_directories.is_empty() {
        panic!("can't test connect_unix; unix_socket_directories is empty");
    }

    let unix_socket_directory = unix_socket_directories[].split(',').next().unwrap();

    let path = url::utf8_percent_encode(unix_socket_directory, url::USERNAME_ENCODE_SET);
    let url = format!("postgres://postgres@{}", path);
    let conn = or_panic!(Connection::connect(url[], &NoSsl));
    assert!(conn.finish().is_ok());
}

#[test]
fn test_transaction_commit() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("INSERT INTO foo (id) VALUES ($1)", &[&1i32]));
    trans.set_commit();
    drop(trans);

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    let result = or_panic!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect());
}

#[test]
fn test_transaction_commit_finish() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("INSERT INTO foo (id) VALUES ($1)", &[&1i32]));
    trans.set_commit();
    assert!(trans.finish().is_ok());

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    let result = or_panic!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect());
}

#[test]
fn test_transaction_commit_method() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("INSERT INTO foo (id) VALUES ($1)", &[&1i32]));
    assert!(trans.commit().is_ok());

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    let result = or_panic!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect());
}

#[test]
fn test_transaction_rollback() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    or_panic!(conn.execute("INSERT INTO foo (id) VALUES ($1)", &[&1i32]));

    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("INSERT INTO foo (id) VALUES ($1)", &[&2i32]));
    drop(trans);

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    let result = or_panic!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect());
}

#[test]
fn test_transaction_rollback_finish() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    or_panic!(conn.execute("INSERT INTO foo (id) VALUES ($1)", &[&1i32]));

    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("INSERT INTO foo (id) VALUES ($1)", &[&2i32]));
    assert!(trans.finish().is_ok());

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    let result = or_panic!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect());
}

#[test]
fn test_nested_transactions() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    or_panic!(conn.execute("INSERT INTO foo (id) VALUES (1)", []));

    {
        let trans1 = or_panic!(conn.transaction());
        or_panic!(trans1.execute("INSERT INTO foo (id) VALUES (2)", []));

        {
            let trans2 = or_panic!(trans1.transaction());
            or_panic!(trans2.execute("INSERT INTO foo (id) VALUES (3)", []));
        }

        {
            let trans2 = or_panic!(trans1.transaction());
            or_panic!(trans2.execute("INSERT INTO foo (id) VALUES (4)", []));

            {
                let trans3 = or_panic!(trans2.transaction());
                or_panic!(trans3.execute("INSERT INTO foo (id) VALUES (5)", []));
            }

            {
                let trans3 = or_panic!(trans2.transaction());
                or_panic!(trans3.execute("INSERT INTO foo (id) VALUES (6)", []));
                assert!(trans3.commit().is_ok());
            }

            assert!(trans2.commit().is_ok());
        }

        let stmt = or_panic!(trans1.prepare("SELECT * FROM foo ORDER BY id"));
        let result = or_panic!(stmt.query([]));

        assert_eq!(vec![1i32, 2, 4, 6], result.map(|row| row.get(0)).collect());
    }

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo ORDER BY id"));
    let result = or_panic!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect());
}

#[test]
fn test_nested_transactions_finish() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    or_panic!(conn.execute("INSERT INTO foo (id) VALUES (1)", []));

    {
        let trans1 = or_panic!(conn.transaction());
        or_panic!(trans1.execute("INSERT INTO foo (id) VALUES (2)", []));

        {
            let trans2 = or_panic!(trans1.transaction());
            or_panic!(trans2.execute("INSERT INTO foo (id) VALUES (3)", []));
            assert!(trans2.finish().is_ok());
        }

        {
            let trans2 = or_panic!(trans1.transaction());
            or_panic!(trans2.execute("INSERT INTO foo (id) VALUES (4)", []));

            {
                let trans3 = or_panic!(trans2.transaction());
                or_panic!(trans3.execute("INSERT INTO foo (id) VALUES (5)", []));
                assert!(trans3.finish().is_ok());
            }

            {
                let trans3 = or_panic!(trans2.transaction());
                or_panic!(trans3.execute("INSERT INTO foo (id) VALUES (6)", []));
                trans3.set_commit();
                assert!(trans3.finish().is_ok());
            }

            trans2.set_commit();
            assert!(trans2.finish().is_ok());
        }

        // in a block to unborrow trans1 for the finish call
        {
            let stmt = or_panic!(trans1.prepare("SELECT * FROM foo ORDER BY id"));
            let result = or_panic!(stmt.query([]));

            assert_eq!(vec![1i32, 2, 4, 6], result.map(|row| row.get(0)).collect());
        }

        assert!(trans1.finish().is_ok());
    }

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo ORDER BY id"));
    let result = or_panic!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect());
}

#[test]
fn test_conn_prepare_with_trans() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let _trans = or_panic!(conn.transaction());
    match conn.prepare("") {
        Err(PgWrongTransaction) => {}
        Err(r) => panic!("Unexpected error {}", r),
        Ok(_) => panic!("Unexpected success"),
    }
    match conn.transaction() {
        Err(PgWrongTransaction) => {}
        Err(r) => panic!("Unexpected error {}", r),
        Ok(_) => panic!("Unexpected success"),
    }
}

#[test]
fn test_trans_prepare_with_nested_trans() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let trans = or_panic!(conn.transaction());
    let _trans2 = or_panic!(trans.transaction());
    match trans.prepare("") {
        Err(PgWrongTransaction) => {}
        Err(r) => panic!("Unexpected error {}", r),
        Ok(_) => panic!("Unexpected success"),
    }
    match trans.transaction() {
        Err(PgWrongTransaction) => {}
        Err(r) => panic!("Unexpected error {}", r),
        Ok(_) => panic!("Unexpected success"),
    }
}

#[test]
fn test_stmt_finish() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY)", []));
    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    assert!(stmt.finish().is_ok());
}

#[test]
fn test_batch_execute() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let query = "CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY);
                 INSERT INTO foo (id) VALUES (10);";
    or_panic!(conn.batch_execute(query));

    let stmt = or_panic!(conn.prepare("SELECT * from foo ORDER BY id"));
    let result = or_panic!(stmt.query([]));

    assert_eq!(vec![10i64], result.map(|row| row.get(0)).collect());
}

#[test]
fn test_batch_execute_error() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let query = "CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY);
                 INSERT INTO foo (id) VALUES (10);
                 asdfa;
                 INSERT INTO foo (id) VALUES (11)";
    conn.batch_execute(query).unwrap_err();

    match conn.prepare("SELECT * from foo ORDER BY id") {
        Err(PgDbError(PostgresDbError { code: UndefinedTable, .. })) => {},
        Err(e) => panic!("unexpected error {}", e),
        _ => panic!("unexpected success"),
    }
}

#[test]
fn test_transaction_batch_execute() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let trans = or_panic!(conn.transaction());
    let query = "CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY);
                 INSERT INTO foo (id) VALUES (10);";
    or_panic!(trans.batch_execute(query));

    let stmt = or_panic!(trans.prepare("SELECT * from foo ORDER BY id"));
    let result = or_panic!(stmt.query([]));

    assert_eq!(vec![10i64], result.map(|row| row.get(0)).collect());
}

#[test]
fn test_query() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY)", []));
    or_panic!(conn.execute("INSERT INTO foo (id) VALUES ($1), ($2)",
                          &[&1i64, &2i64]));
    let stmt = or_panic!(conn.prepare("SELECT * from foo ORDER BY id"));
    let result = or_panic!(stmt.query([]));

    assert_eq!(vec![1i64, 2], result.map(|row| row.get(0)).collect());
}

#[test]
fn test_error_after_datarow() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_panic!(conn.prepare("
SELECT
    (SELECT generate_series(1, ss.i))
FROM (SELECT gs.i
      FROM generate_series(1, 2) gs(i)
      ORDER BY gs.i
      LIMIT 2) ss"));
    match stmt.query([]) {
        Err(PgDbError(PostgresDbError { code: CardinalityViolation, .. })) => {}
        Err(err) => panic!("Unexpected error {}", err),
        Ok(_) => panic!("Expected failure"),
    }
}

#[test]
fn test_result_finish() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY)", []));
    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    let result = or_panic!(stmt.query([]));
    assert!(result.finish().is_ok());
}

#[test]
fn test_lazy_query() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));

    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));
    let stmt = or_panic!(trans.prepare("INSERT INTO foo (id) VALUES ($1)"));
    let values = vec!(0i32, 1, 2, 3, 4, 5);
    for value in values.iter() {
        or_panic!(stmt.execute(&[value]));
    }
    let stmt = or_panic!(trans.prepare("SELECT id FROM foo ORDER BY id"));
    let result = or_panic!(trans.lazy_query(&stmt, [], 2));
    assert_eq!(values, result.map(|row| row.unwrap().get(0)).collect());
}

#[test]
fn test_lazy_query_wrong_conn() {
    let conn1 = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let conn2 = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));

    let trans = or_panic!(conn1.transaction());
    let stmt = or_panic!(conn2.prepare("SELECT 1::INT"));
    match trans.lazy_query(&stmt, [], 1) {
        Err(PgWrongConnection) => {}
        Err(err) => panic!("Unexpected error {}", err),
        Ok(_) => panic!("Expected failure")
    }
}

#[test]
fn test_param_types() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_panic!(conn.prepare("SELECT $1::INT, $2::VARCHAR"));
    assert_eq!(stmt.param_types(), [Type::Int4, Type::Varchar][]);
}

#[test]
fn test_result_descriptions() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_panic!(conn.prepare("SELECT 1::INT as a, 'hi'::VARCHAR as b"));
    assert!(stmt.result_descriptions() ==
            [ResultDescription { name: "a".to_string(), ty: Type::Int4},
             ResultDescription { name: "b".to_string(), ty: Type::Varchar}]);
}

#[test]
fn test_execute_counts() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    assert_eq!(0, or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (
                                            id SERIAL PRIMARY KEY,
                                            b INT
                                         )", [])));
    assert_eq!(3, or_panic!(conn.execute("INSERT INTO foo (b) VALUES ($1), ($2), ($2)",
                                        &[&1i32, &2i32])));
    assert_eq!(2, or_panic!(conn.execute("UPDATE foo SET b = 0 WHERE b = 2", [])));
    assert_eq!(3, or_panic!(conn.execute("SELECT * FROM foo", [])));
}

#[test]
fn test_wrong_param_type() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    match conn.execute("SELECT $1::VARCHAR", &[&1i32]) {
        Err(PgWrongType(_)) => {}
        res => panic!("unexpected result {}", res)
    }
}

#[test]
fn test_too_few_params() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    match conn.execute("SELECT $1::INT, $2::INT", &[&1i32]) {
        Err(PgWrongParamCount { expected: 2, actual: 1 }) => {},
        res => panic!("unexpected result {}", res)
    }
}

#[test]
fn test_too_many_params() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    match conn.execute("SELECT $1::INT, $2::INT", &[&1i32, &2i32, &3i32]) {
        Err(PgWrongParamCount { expected: 2, actual: 3 }) => {},
        res => panic!("unexpected result {}", res)
    }
}

#[test]
fn test_index_named() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_panic!(conn.prepare("SELECT 10::INT as val"));
    let result = or_panic!(stmt.query([]));

    assert_eq!(vec![10i32], result.map(|row| row.get("val")).collect());
}

#[test]
#[should_fail]
fn test_index_named_fail() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_panic!(conn.prepare("SELECT 10::INT as id"));
    let mut result = or_panic!(stmt.query([]));

    let _: i32 = result.next().unwrap().get("asdf");
}

#[test]
fn test_get_named_err() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_panic!(conn.prepare("SELECT 10::INT as id"));
    let mut result = or_panic!(stmt.query([]));

    match result.next().unwrap().get_opt::<&str, i32>("asdf") {
        Err(PgInvalidColumn) => {}
        res => panic!("unexpected result {}", res),
    };
}

#[test]
fn test_get_was_null() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_panic!(conn.prepare("SELECT NULL::INT as id"));
    let mut result = or_panic!(stmt.query([]));

    match result.next().unwrap().get_opt::<uint, i32>(0) {
        Err(PgWasNull) => {}
        res => panic!("unexpected result {}", res),
    };
}

#[test]
fn test_custom_notice_handler() {
    static mut count: uint = 0;
    struct Handler;

    impl NoticeHandler for Handler {
        fn handle(&mut self, notice: PostgresDbError) {
            assert_eq!("note", notice.message[]);
            unsafe { count += 1; }
        }
    }

    let conn = or_panic!(Connection::connect(
            "postgres://postgres@localhost?client_min_messages=NOTICE", &NoSsl));
    conn.set_notice_handler(box Handler);
    or_panic!(conn.execute("CREATE FUNCTION pg_temp.note() RETURNS INT AS $$
                           BEGIN
                            RAISE NOTICE 'note';
                            RETURN 1;
                           END; $$ LANGUAGE plpgsql", []));
    or_panic!(conn.execute("SELECT pg_temp.note()", []));

    assert_eq!(unsafe { count }, 1);
}

#[test]
fn test_notification_iterator_none() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    assert!(conn.notifications().next().is_none());
}

#[test]
fn test_notification_iterator_some() {
    fn check_notification(expected: Notification,
                          actual: Option<Notification>) {
        match actual {
            Some(Notification { channel, payload, .. }) => {
                assert_eq!(&expected.channel, &channel);
                assert_eq!(&expected.payload, &payload);
            }
            None => panic!("Unexpected result")
        }
    }

    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let mut it = conn.notifications();
    or_panic!(conn.execute("LISTEN test_notification_iterator_one_channel", []));
    or_panic!(conn.execute("LISTEN test_notification_iterator_one_channel2", []));
    or_panic!(conn.execute("NOTIFY test_notification_iterator_one_channel, 'hello'", []));
    or_panic!(conn.execute("NOTIFY test_notification_iterator_one_channel2, 'world'", []));

    check_notification(Notification {
        pid: 0,
        channel: "test_notification_iterator_one_channel".to_string(),
        payload: "hello".to_string()
    }, it.next());
    check_notification(Notification {
        pid: 0,
        channel: "test_notification_iterator_one_channel2".to_string(),
        payload: "world".to_string()
    }, it.next());
    assert!(it.next().is_none());

    or_panic!(conn.execute("NOTIFY test_notification_iterator_one_channel, '!'", []));
    check_notification(Notification {
        pid: 0,
        channel: "test_notification_iterator_one_channel".to_string(),
        payload: "!".to_string()
    }, it.next());
    assert!(it.next().is_none());
}

#[test]
// This test is pretty sad, but I don't think there's a better way :(
fn test_cancel_query() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    let cancel_data = conn.cancel_data();

    spawn(proc() {
        timer::sleep(Duration::milliseconds(500));
        assert!(postgres::cancel_query("postgres://postgres@localhost", &NoSsl,
                                       cancel_data).is_ok());
    });

    match conn.execute("SELECT pg_sleep(10)", []) {
        Err(PgDbError(PostgresDbError { code: QueryCanceled, .. })) => {}
        Err(res) => panic!("Unexpected result {}", res),
        _ => panic!("Unexpected result"),
    }
}

#[test]
fn test_require_ssl_conn() {
    let ctx = SslContext::new(Sslv3).unwrap();
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost",
                                                    &RequireSsl(ctx)));
    or_panic!(conn.execute("SELECT 1::VARCHAR", []));
}

#[test]
fn test_prefer_ssl_conn() {
    let ctx = SslContext::new(Sslv3).unwrap();
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost",
                                                    &PreferSsl(ctx)));
    or_panic!(conn.execute("SELECT 1::VARCHAR", []));
}

#[test]
fn test_plaintext_pass() {
    or_panic!(Connection::connect("postgres://pass_user:password@localhost/postgres", &NoSsl));
}

#[test]
fn test_plaintext_pass_no_pass() {
    let ret = Connection::connect("postgres://pass_user@localhost/postgres", &NoSsl);
    match ret {
        Err(MissingPassword) => (),
        Err(err) => panic!("Unexpected error {}", err),
        _ => panic!("Expected error")
    }
}

#[test]
fn test_plaintext_pass_wrong_pass() {
    let ret = Connection::connect("postgres://pass_user:asdf@localhost/postgres", &NoSsl);
    match ret {
        Err(PgConnectDbError(PostgresDbError { code: InvalidPassword, .. })) => (),
        Err(err) => panic!("Unexpected error {}", err),
        _ => panic!("Expected error")
    }
}

 #[test]
fn test_md5_pass() {
    or_panic!(Connection::connect("postgres://md5_user:password@localhost/postgres", &NoSsl));
}

#[test]
fn test_md5_pass_no_pass() {
    let ret = Connection::connect("postgres://md5_user@localhost/postgres", &NoSsl);
    match ret {
        Err(MissingPassword) => (),
        Err(err) => panic!("Unexpected error {}", err),
        _ => panic!("Expected error")
    }
}

#[test]
fn test_md5_pass_wrong_pass() {
    let ret = Connection::connect("postgres://md5_user:asdf@localhost/postgres", &NoSsl);
    match ret {
        Err(PgConnectDbError(PostgresDbError { code: InvalidPassword, .. })) => (),
        Err(err) => panic!("Unexpected error {}", err),
        _ => panic!("Expected error")
    }
}

#[test]
fn test_execute_copy_from_err() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT)", []));
    let stmt = or_panic!(conn.prepare("COPY foo (id) FROM STDIN"));
    match stmt.execute([]) {
        Err(PgDbError(ref err)) if err.message[].contains("COPY") => {}
        Err(err) => panic!("Unexptected error {}", err),
        _ => panic!("Expected error"),
    }
    match stmt.query([]) {
        Err(PgDbError(ref err)) if err.message[].contains("COPY") => {}
        Err(err) => panic!("Unexptected error {}", err),
        _ => panic!("Expected error"),
    }
}

#[test]
fn test_copy_in() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT, name VARCHAR)", []));

    let stmt = or_panic!(conn.prepare_copy_in("foo", ["id", "name"]));
    let data: &[&[&ToSql]] = &[&[&0i32, &"Steven".to_string()], &[&1i32, &None::<String>]];

    assert_eq!(Ok(2), stmt.execute(data.iter().map(|r| r.iter().map(|&e| e))));

    let stmt = or_panic!(conn.prepare("SELECT id, name FROM foo ORDER BY id"));
    assert_eq!(vec![(0i32, Some("Steven".to_string())), (1, None)],
               or_panic!(stmt.query([])).map(|r| (r.get(0), r.get(1))).collect());
}

#[test]
fn test_copy_in_bad_column_count() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT, name VARCHAR)", []));

    let stmt = or_panic!(conn.prepare_copy_in("foo", ["id", "name"]));
    let data: &[&[&ToSql]] = &[&[&0i32, &"Steven".to_string()], &[&1i32]];

    let res = stmt.execute(data.iter().map(|r| r.iter().map(|&e| e)));
    match res {
        Err(PgDbError(ref err)) if err.message[].contains("Invalid column count") => {}
        Err(err) => panic!("unexpected error {}", err),
        _ => panic!("Expected error"),
    }

    let data: &[&[&ToSql]] = &[&[&0i32, &"Steven".to_string()], &[&1i32, &"Steven".to_string(), &1i32]];

    let res = stmt.execute(data.iter().map(|r| r.iter().map(|&e| e)));
    match res {
        Err(PgDbError(ref err)) if err.message[].contains("Invalid column count") => {}
        Err(err) => panic!("unexpected error {}", err),
        _ => panic!("Expected error"),
    }

    or_panic!(conn.execute("SELECT 1", []));
}

#[test]
fn test_copy_in_bad_type() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT, name VARCHAR)", []));

    let stmt = or_panic!(conn.prepare_copy_in("foo", ["id", "name"]));
    let data: &[&[&ToSql]] = &[&[&0i32, &"Steven".to_string()], &[&1i32, &2i32]];

    let res = stmt.execute(data.iter().map(|r| r.iter().map(|&e| e)));
    match res {
        Err(PgDbError(ref err)) if err.message[].contains("Unexpected type Varchar") => {}
        Err(err) => panic!("unexpected error {}", err),
        _ => panic!("Expected error"),
    }

    or_panic!(conn.execute("SELECT 1", []));
}

#[test]
fn test_batch_execute_copy_from_err() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT)", []));
    match conn.batch_execute("COPY foo (id) FROM STDIN") {
        Err(PgDbError(ref err)) if err.message[].contains("COPY") => {}
        Err(err) => panic!("Unexptected error {}", err),
        _ => panic!("Expected error"),
    }
}

#[test]
// Just make sure the impls don't infinite loop
fn test_generic_connection() {
    fn f<T>(t: &T) where T: GenericConnection {
        or_panic!(t.execute("SELECT 1", []));
    }

    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &NoSsl));
    f(&conn);
    let trans = or_panic!(conn.transaction());
    f(&trans);
}

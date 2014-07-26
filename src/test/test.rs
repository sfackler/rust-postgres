#![feature(macro_rules, phase)]

#[phase(plugin, link)]
extern crate postgres;
extern crate serialize;
extern crate time;
extern crate url;
extern crate uuid;
extern crate openssl;

use openssl::ssl::{SslContext, Sslv3};
use std::io::timer;

use postgres::{PostgresNoticeHandler,
               PostgresNotification,
               PostgresConnection,
               ResultDescription,
               RequireSsl,
               PreferSsl,
               NoSsl};
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
use postgres::types::{PgInt4, PgVarchar};

macro_rules! or_fail(
    ($e:expr) => (
        match $e {
            Ok(ok) => ok,
            Err(err) => fail!("{}", err)
        }
    )
)

mod types;
mod pool;

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
        Err(e) => fail!("Unexpected result {}", e),
        _ => fail!("Unexpected result"),
    }
}

#[test]
fn test_unknown_database() {
    match PostgresConnection::connect("postgres://postgres@localhost/asdf", &NoSsl) {
        Err(PgConnectDbError(PostgresDbError { code: InvalidCatalogName, .. })) => {}
        Err(resp) => fail!("Unexpected result {}", resp),
        _ => fail!("Unexpected result"),
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
    let unix_socket_directories: String = result.map(|row| row.get(0u)).next().unwrap();

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
    or_fail!(trans.execute("INSERT INTO foo (id) VALUES ($1)", [&1i32]));
    trans.set_commit();
    drop(trans);

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0u)).collect());
}

#[test]
fn test_transaction_commit_finish() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    let trans = or_fail!(conn.transaction());
    or_fail!(trans.execute("INSERT INTO foo (id) VALUES ($1)", [&1i32]));
    trans.set_commit();
    assert!(trans.finish().is_ok());

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0u)).collect());
}

#[test]
fn test_transaction_commit_method() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    let trans = or_fail!(conn.transaction());
    or_fail!(trans.execute("INSERT INTO foo (id) VALUES ($1)", [&1i32]));
    assert!(trans.commit().is_ok());

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0u)).collect());
}

#[test]
fn test_transaction_rollback() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    or_fail!(conn.execute("INSERT INTO foo (id) VALUES ($1)", [&1i32]));

    let trans = or_fail!(conn.transaction());
    or_fail!(trans.execute("INSERT INTO foo (id) VALUES ($1)", [&2i32]));
    drop(trans);

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0u)).collect());
}

#[test]
fn test_transaction_rollback_finish() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", []));

    or_fail!(conn.execute("INSERT INTO foo (id) VALUES ($1)", [&1i32]));

    let trans = or_fail!(conn.transaction());
    or_fail!(trans.execute("INSERT INTO foo (id) VALUES ($1)", [&2i32]));
    assert!(trans.finish().is_ok());

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0u)).collect());
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
        }

        {
            let trans2 = or_fail!(trans1.transaction());
            or_fail!(trans2.execute("INSERT INTO foo (id) VALUES (4)", []));

            {
                let trans3 = or_fail!(trans2.transaction());
                or_fail!(trans3.execute("INSERT INTO foo (id) VALUES (5)", []));
            }

            {
                let trans3 = or_fail!(trans2.transaction());
                or_fail!(trans3.execute("INSERT INTO foo (id) VALUES (6)", []));
                assert!(trans3.commit().is_ok());
            }

            assert!(trans2.commit().is_ok());
        }

        let stmt = or_fail!(trans1.prepare("SELECT * FROM foo ORDER BY id"));
        let result = or_fail!(stmt.query([]));

        assert_eq!(vec![1i32, 2, 4, 6], result.map(|row| row.get(0u)).collect());
    }

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo ORDER BY id"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0u)).collect());
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
            assert!(trans2.finish().is_ok());
        }

        {
            let trans2 = or_fail!(trans1.transaction());
            or_fail!(trans2.execute("INSERT INTO foo (id) VALUES (4)", []));

            {
                let trans3 = or_fail!(trans2.transaction());
                or_fail!(trans3.execute("INSERT INTO foo (id) VALUES (5)", []));
                assert!(trans3.finish().is_ok());
            }

            {
                let trans3 = or_fail!(trans2.transaction());
                or_fail!(trans3.execute("INSERT INTO foo (id) VALUES (6)", []));
                trans3.set_commit();
                assert!(trans3.finish().is_ok());
            }

            trans2.set_commit();
            assert!(trans2.finish().is_ok());
        }

        // in a block to unborrow trans1 for the finish call
        {
            let stmt = or_fail!(trans1.prepare("SELECT * FROM foo ORDER BY id"));
            let result = or_fail!(stmt.query([]));

            assert_eq!(vec![1i32, 2, 4, 6], result.map(|row| row.get(0u)).collect());
        }

        assert!(trans1.finish().is_ok());
    }

    let stmt = or_fail!(conn.prepare("SELECT * FROM foo ORDER BY id"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0u)).collect());
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
fn test_batch_execute() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let query = "CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY);
                 INSERT INTO foo (id) VALUES (10);";
    or_fail!(conn.batch_execute(query));

    let stmt = or_fail!(conn.prepare("SELECT * from foo ORDER BY id"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![10i64], result.map(|row| row.get(0u)).collect());
}

#[test]
fn test_batch_execute_error() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let query = "CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY);
                 INSERT INTO foo (id) VALUES (10);
                 asdfa;
                 INSERT INTO foo (id) VALUES (11)";
    conn.batch_execute(query).unwrap_err();

    match conn.prepare("SELECT * from foo ORDER BY id") {
        Err(PgDbError(PostgresDbError { code: UndefinedTable, .. })) => {},
        Err(e) => fail!("unexpected error {}", e),
        _ => fail!("unexpected success"),
    }
}

#[test]
fn test_query() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY)", []));
    or_fail!(conn.execute("INSERT INTO foo (id) VALUES ($1), ($2)",
                          [&1i64, &2i64]));
    let stmt = or_fail!(conn.prepare("SELECT * from foo ORDER BY id"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![1i64, 2], result.map(|row| row.get(0u)).collect());
}

#[test]
fn test_error_after_datarow() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("
SELECT
    (SELECT generate_series(1, ss.i))
FROM (SELECT gs.i
      FROM generate_series(1, 2) gs(i)
      ORDER BY gs.i
      LIMIT 2) ss"));
    match stmt.query([]) {
        Err(PgDbError(PostgresDbError { code: CardinalityViolation, .. })) => {}
        Err(err) => fail!("Unexpected error {}", err),
        Ok(_) => fail!("Expected failure"),
    }
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
        or_fail!(stmt.execute([value]));
    }
    let stmt = or_fail!(trans.prepare("SELECT id FROM foo ORDER BY id"));
    let result = or_fail!(trans.lazy_query(&stmt, [], 2));
    assert_eq!(values, result.map(|row| row.unwrap().get(0u)).collect());

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
            [ResultDescription { name: "a".to_string(), ty: PgInt4},
             ResultDescription { name: "b".to_string(), ty: PgVarchar}]);
}

#[test]
fn test_execute_counts() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    assert_eq!(0, or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (
                                            id SERIAL PRIMARY KEY,
                                            b INT
                                         )", [])));
    assert_eq!(3, or_fail!(conn.execute("INSERT INTO foo (b) VALUES ($1), ($2), ($2)",
                                        [&1i32, &2i32])));
    assert_eq!(2, or_fail!(conn.execute("UPDATE foo SET b = 0 WHERE b = 2", [])));
    assert_eq!(3, or_fail!(conn.execute("SELECT * FROM foo", [])));
}

#[test]
fn test_wrong_param_type() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    match conn.execute("SELECT $1::VARCHAR", [&1i32]) {
        Err(PgWrongType(_)) => {}
        res => fail!("unexpected result {}", res)
    }
}

#[test]
fn test_too_few_params() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    match conn.execute("SELECT $1::INT, $2::INT", [&1i32]) {
        Err(PgWrongParamCount { expected: 2, actual: 1 }) => {},
        res => fail!("unexpected result {}", res)
    }
}

#[test]
fn test_too_many_params() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    match conn.execute("SELECT $1::INT, $2::INT", [&1i32,
                                                   &2i32,
                                                   &3i32]) {
        Err(PgWrongParamCount { expected: 2, actual: 3 }) => {},
        res => fail!("unexpected result {}", res)
    }
}

#[test]
fn test_index_named() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SELECT 10::INT as val"));
    let result = or_fail!(stmt.query([]));

    assert_eq!(vec![10i32], result.map(|row| row.get("val")).collect());
}

#[test]
#[should_fail]
fn test_index_named_fail() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SELECT 10::INT as id"));
    let mut result = or_fail!(stmt.query([]));

    let _: i32 = result.next().unwrap().get("asdf");
}

#[test]
fn test_get_named_err() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SELECT 10::INT as id"));
    let mut result = or_fail!(stmt.query([]));

    match result.next().unwrap().get_opt::<&str, i32>("asdf") {
        Err(PgInvalidColumn) => {}
        res => fail!("unexpected result {}", res),
    };
}

#[test]
fn test_get_was_null() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SELECT NULL::INT as id"));
    let mut result = or_fail!(stmt.query([]));

    match result.next().unwrap().get_opt::<uint, i32>(0) {
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
            None => fail!("Unexpected result")
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
        channel: "test_notification_iterator_one_channel".to_string(),
        payload: "hello".to_string()
    }, it.next());
    check_notification(PostgresNotification {
        pid: 0,
        channel: "test_notification_iterator_one_channel2".to_string(),
        payload: "world".to_string()
    }, it.next());
    assert!(it.next().is_none());

    or_fail!(conn.execute("NOTIFY test_notification_iterator_one_channel, '!'", []));
    check_notification(PostgresNotification {
        pid: 0,
        channel: "test_notification_iterator_one_channel".to_string(),
        payload: "!".to_string()
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
        assert!(postgres::cancel_query("postgres://postgres@localhost", &NoSsl,
                                       cancel_data).is_ok());
    });

    match conn.execute("SELECT pg_sleep(10)", []) {
        Err(PgDbError(PostgresDbError { code: QueryCanceled, .. })) => {}
        Err(res) => fail!("Unexpected result {}", res),
        _ => fail!("Unexpected result"),
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

#![feature(core, std_misc, old_io)]

extern crate postgres;
extern crate "rustc-serialize" as serialize;
extern crate url;
extern crate openssl;

use openssl::ssl::SslContext;
use openssl::ssl::SslMethod;
use std::old_io::timer;
use std::time::Duration;
use std::thread;

use postgres::{NoticeHandler,
               Notification,
               Connection,
               GenericConnection,
               SslMode,
               Type,
               Kind,
               ToSql,
               Error,
               ConnectError,
               DbError,
               VecStreamIterator};
use postgres::SqlState::{SyntaxError,
                         QueryCanceled,
                         UndefinedTable,
                         InvalidCatalogName,
                         InvalidPassword,
                         CardinalityViolation};
use postgres::ErrorPosition::Normal;

macro_rules! or_panic {
    ($e:expr) => (
        match $e {
            Ok(ok) => ok,
            Err(err) => panic!("{:?}", err)
        }
    )
}

mod types;

#[test]
fn test_non_default_database() {
    or_panic!(Connection::connect("postgres://postgres@localhost/postgres", &SslMode::None));
}

#[test]
fn test_url_terminating_slash() {
    or_panic!(Connection::connect("postgres://postgres@localhost/", &SslMode::None));
}

#[test]
fn test_prepare_err() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = conn.prepare("invalid sql database");
    match stmt {
        Err(Error::DbError(ref e)) if e.code() == &SyntaxError && e.position() == Some(&Normal(1)) => {}
        Err(e) => panic!("Unexpected result {:?}", e),
        _ => panic!("Unexpected result"),
    }
}

#[test]
fn test_unknown_database() {
    match Connection::connect("postgres://postgres@localhost/asdf", &SslMode::None) {
        Err(ConnectError::DbError(ref e)) if e.code() == &InvalidCatalogName => {}
        Err(resp) => panic!("Unexpected result {:?}", resp),
        _ => panic!("Unexpected result"),
    }
}

#[test]
fn test_connection_finish() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    assert!(conn.finish().is_ok());
}

#[test]
fn test_unix_connection() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = or_panic!(conn.prepare("SHOW unix_socket_directories"));
    let result = or_panic!(stmt.query(&[]));
    let unix_socket_directories: String = result.map(|row| row.get(0)).next().unwrap();

    if unix_socket_directories.is_empty() {
        panic!("can't test connect_unix; unix_socket_directories is empty");
    }

    let unix_socket_directory = unix_socket_directories.split(',').next().unwrap();

    let path = url::utf8_percent_encode(unix_socket_directory, url::USERNAME_ENCODE_SET);
    let url = format!("postgres://postgres@{}", path);
    let conn = or_panic!(Connection::connect(&url[..], &SslMode::None));
    assert!(conn.finish().is_ok());
}

#[test]
fn test_transaction_commit() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", &[]));

    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("INSERT INTO foo (id) VALUES ($1)", &[&1i32]));
    trans.set_commit();
    drop(trans);

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    let result = or_panic!(stmt.query(&[]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect::<Vec<_>>());
}

#[test]
fn test_transaction_commit_finish() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", &[]));

    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("INSERT INTO foo (id) VALUES ($1)", &[&1i32]));
    trans.set_commit();
    assert!(trans.finish().is_ok());

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    let result = or_panic!(stmt.query(&[]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect::<Vec<_>>());
}

#[test]
fn test_transaction_commit_method() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", &[]));

    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("INSERT INTO foo (id) VALUES ($1)", &[&1i32]));
    assert!(trans.commit().is_ok());

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    let result = or_panic!(stmt.query(&[]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect::<Vec<_>>());
}

#[test]
fn test_transaction_rollback() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", &[]));

    or_panic!(conn.execute("INSERT INTO foo (id) VALUES ($1)", &[&1i32]));

    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("INSERT INTO foo (id) VALUES ($1)", &[&2i32]));
    drop(trans);

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    let result = or_panic!(stmt.query(&[]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect::<Vec<_>>());
}

#[test]
fn test_transaction_rollback_finish() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", &[]));

    or_panic!(conn.execute("INSERT INTO foo (id) VALUES ($1)", &[&1i32]));

    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("INSERT INTO foo (id) VALUES ($1)", &[&2i32]));
    assert!(trans.finish().is_ok());

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    let result = or_panic!(stmt.query(&[]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect::<Vec<_>>());
}

#[test]
fn test_nested_transactions() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", &[]));

    or_panic!(conn.execute("INSERT INTO foo (id) VALUES (1)", &[]));

    {
        let trans1 = or_panic!(conn.transaction());
        or_panic!(trans1.execute("INSERT INTO foo (id) VALUES (2)", &[]));

        {
            let trans2 = or_panic!(trans1.transaction());
            or_panic!(trans2.execute("INSERT INTO foo (id) VALUES (3)", &[]));
        }

        {
            let trans2 = or_panic!(trans1.transaction());
            or_panic!(trans2.execute("INSERT INTO foo (id) VALUES (4)", &[]));

            {
                let trans3 = or_panic!(trans2.transaction());
                or_panic!(trans3.execute("INSERT INTO foo (id) VALUES (5)", &[]));
            }

            {
                let trans3 = or_panic!(trans2.transaction());
                or_panic!(trans3.execute("INSERT INTO foo (id) VALUES (6)", &[]));
                assert!(trans3.commit().is_ok());
            }

            assert!(trans2.commit().is_ok());
        }

        let stmt = or_panic!(trans1.prepare("SELECT * FROM foo ORDER BY id"));
        let result = or_panic!(stmt.query(&[]));

        assert_eq!(vec![1i32, 2, 4, 6], result.map(|row| row.get(0)).collect::<Vec<_>>());
    }

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo ORDER BY id"));
    let result = or_panic!(stmt.query(&[]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect::<Vec<_>>());
}

#[test]
fn test_nested_transactions_finish() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", &[]));

    or_panic!(conn.execute("INSERT INTO foo (id) VALUES (1)", &[]));

    {
        let trans1 = or_panic!(conn.transaction());
        or_panic!(trans1.execute("INSERT INTO foo (id) VALUES (2)", &[]));

        {
            let trans2 = or_panic!(trans1.transaction());
            or_panic!(trans2.execute("INSERT INTO foo (id) VALUES (3)", &[]));
            assert!(trans2.finish().is_ok());
        }

        {
            let trans2 = or_panic!(trans1.transaction());
            or_panic!(trans2.execute("INSERT INTO foo (id) VALUES (4)", &[]));

            {
                let trans3 = or_panic!(trans2.transaction());
                or_panic!(trans3.execute("INSERT INTO foo (id) VALUES (5)", &[]));
                assert!(trans3.finish().is_ok());
            }

            {
                let trans3 = or_panic!(trans2.transaction());
                or_panic!(trans3.execute("INSERT INTO foo (id) VALUES (6)", &[]));
                trans3.set_commit();
                assert!(trans3.finish().is_ok());
            }

            trans2.set_commit();
            assert!(trans2.finish().is_ok());
        }

        // in a block to unborrow trans1 for the finish call
        {
            let stmt = or_panic!(trans1.prepare("SELECT * FROM foo ORDER BY id"));
            let result = or_panic!(stmt.query(&[]));

            assert_eq!(vec![1i32, 2, 4, 6], result.map(|row| row.get(0)).collect::<Vec<_>>());
        }

        assert!(trans1.finish().is_ok());
    }

    let stmt = or_panic!(conn.prepare("SELECT * FROM foo ORDER BY id"));
    let result = or_panic!(stmt.query(&[]));

    assert_eq!(vec![1i32], result.map(|row| row.get(0)).collect::<Vec<_>>());
}

#[test]
#[should_fail(expected = "active transaction")]
fn test_conn_trans_when_nested() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let _trans = or_panic!(conn.transaction());
    conn.transaction().unwrap();
}

#[test]
#[should_fail(expected = "active transaction")]
fn test_trans_with_nested_trans() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let trans = or_panic!(conn.transaction());
    let _trans2 = or_panic!(trans.transaction());
    trans.transaction().unwrap();
}

#[test]
fn test_stmt_execute_after_transaction() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let trans = or_panic!(conn.transaction());
    let stmt = or_panic!(trans.prepare("SELECT 1"));
    or_panic!(trans.finish());
    let mut result = or_panic!(stmt.query(&[]));
    assert_eq!(1i32, result.next().unwrap().get(0));
}

#[test]
fn test_stmt_finish() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY)", &[]));
    let stmt = or_panic!(conn.prepare("SELECT * FROM foo"));
    assert!(stmt.finish().is_ok());
}

#[test]
fn test_batch_execute() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let query = "CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY);
                 INSERT INTO foo (id) VALUES (10);";
    or_panic!(conn.batch_execute(query));

    let stmt = or_panic!(conn.prepare("SELECT * from foo ORDER BY id"));
    let result = or_panic!(stmt.query(&[]));

    assert_eq!(vec![10i64], result.map(|row| row.get(0)).collect::<Vec<_>>());
}

#[test]
fn test_batch_execute_error() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let query = "CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY);
                 INSERT INTO foo (id) VALUES (10);
                 asdfa;
                 INSERT INTO foo (id) VALUES (11)";
    conn.batch_execute(query).err().unwrap();

    let stmt = conn.prepare("SELECT * FROM foo ORDER BY id");
    match stmt {
        Err(Error::DbError(ref e)) if e.code() == &UndefinedTable => {}
        Err(e) => panic!("unexpected error {:?}", e),
        _ => panic!("unexpected success"),
    }
}

#[test]
fn test_transaction_batch_execute() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let trans = or_panic!(conn.transaction());
    let query = "CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY);
                 INSERT INTO foo (id) VALUES (10);";
    or_panic!(trans.batch_execute(query));

    let stmt = or_panic!(trans.prepare("SELECT * from foo ORDER BY id"));
    let result = or_panic!(stmt.query(&[]));

    assert_eq!(vec![10i64], result.map(|row| row.get(0)).collect::<Vec<_>>());
}

#[test]
fn test_query() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id BIGINT PRIMARY KEY)", &[]));
    or_panic!(conn.execute("INSERT INTO foo (id) VALUES ($1), ($2)",
                          &[&1i64, &2i64]));
    let stmt = or_panic!(conn.prepare("SELECT * from foo ORDER BY id"));
    let result = or_panic!(stmt.query(&[]));

    assert_eq!(vec![1i64, 2], result.map(|row| row.get(0)).collect::<Vec<_>>());
}

#[test]
fn test_error_after_datarow() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = or_panic!(conn.prepare("
SELECT
    (SELECT generate_series(1, ss.i))
FROM (SELECT gs.i
      FROM generate_series(1, 2) gs(i)
      ORDER BY gs.i
      LIMIT 2) ss"));
    match stmt.query(&[]) {
        Err(Error::DbError(ref e)) if e.code() == &CardinalityViolation => {}
        Err(err) => panic!("Unexpected error {:?}", err),
        Ok(_) => panic!("Expected failure"),
    }
}

#[test]
fn test_lazy_query() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));

    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("CREATE TEMPORARY TABLE foo (id INT PRIMARY KEY)", &[]));
    let stmt = or_panic!(trans.prepare("INSERT INTO foo (id) VALUES ($1)"));
    let values = vec!(0i32, 1, 2, 3, 4, 5);
    for value in values.iter() {
        or_panic!(stmt.execute(&[value]));
    }
    let stmt = or_panic!(trans.prepare("SELECT id FROM foo ORDER BY id"));
    let result = or_panic!(stmt.lazy_query(&trans, &[], 2));
    assert_eq!(values, result.map(|row| row.unwrap().get(0)).collect::<Vec<_>>());
}

#[test]
#[should_fail(expected = "same `Connection` as")]
fn test_lazy_query_wrong_conn() {
    let conn1 = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let conn2 = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));

    let trans = or_panic!(conn1.transaction());
    let stmt = or_panic!(conn2.prepare("SELECT 1::INT"));
    stmt.lazy_query(&trans, &[], 1).unwrap();
}

#[test]
fn test_param_types() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = or_panic!(conn.prepare("SELECT $1::INT, $2::VARCHAR"));
    assert_eq!(stmt.param_types(), [Type::Int4, Type::Varchar].as_slice());
}

#[test]
fn test_columns() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = or_panic!(conn.prepare("SELECT 1::INT as a, 'hi'::VARCHAR as b"));
    let cols = stmt.columns();
    assert_eq!(2, cols.len());
    assert_eq!(cols[0].name(), "a");
    assert_eq!(cols[0].type_(), &Type::Int4);
    assert_eq!(cols[1].name(), "b");
    assert_eq!(cols[1].type_(), &Type::Varchar);
}

#[test]
fn test_execute_counts() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    assert_eq!(0, or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (
                                            id SERIAL PRIMARY KEY,
                                            b INT
                                         )", &[])));
    assert_eq!(3, or_panic!(conn.execute("INSERT INTO foo (b) VALUES ($1), ($2), ($2)",
                                        &[&1i32, &2i32])));
    assert_eq!(2, or_panic!(conn.execute("UPDATE foo SET b = 0 WHERE b = 2", &[])));
    assert_eq!(3, or_panic!(conn.execute("SELECT * FROM foo", &[])));
}

#[test]
fn test_wrong_param_type() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    match conn.execute("SELECT $1::VARCHAR", &[&1i32]) {
        Err(Error::WrongType(_)) => {}
        res => panic!("unexpected result {:?}", res)
    }
}

#[test]
#[should_fail(expected = "expected 2 parameters but got 1")]
fn test_too_few_params() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let _ = conn.execute("SELECT $1::INT, $2::INT", &[&1i32]);
}

#[test]
#[should_fail(expected = "expected 2 parameters but got 3")]
fn test_too_many_params() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let _ = conn.execute("SELECT $1::INT, $2::INT", &[&1i32, &2i32, &3i32]);
}

#[test]
fn test_index_named() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = or_panic!(conn.prepare("SELECT 10::INT as val"));
    let result = or_panic!(stmt.query(&[]));

    assert_eq!(vec![10i32], result.map(|row| row.get("val")).collect::<Vec<_>>());
}

#[test]
#[should_fail]
fn test_index_named_fail() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = or_panic!(conn.prepare("SELECT 10::INT as id"));
    let mut result = or_panic!(stmt.query(&[]));

    let _: i32 = result.next().unwrap().get("asdf");
}

#[test]
fn test_get_named_err() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = or_panic!(conn.prepare("SELECT 10::INT as id"));
    let mut result = or_panic!(stmt.query(&[]));

    match result.next().unwrap().get_opt::<&str, i32>("asdf") {
        Err(Error::InvalidColumn) => {}
        res => panic!("unexpected result {:?}", res),
    };
}

#[test]
fn test_get_was_null() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = or_panic!(conn.prepare("SELECT NULL::INT as id"));
    let mut result = or_panic!(stmt.query(&[]));

    match result.next().unwrap().get_opt::<usize, i32>(0) {
        Err(Error::WasNull) => {}
        res => panic!("unexpected result {:?}", res),
    };
}

#[test]
fn test_get_off_by_one() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = or_panic!(conn.prepare("SELECT 10::INT as id"));
    let mut result = or_panic!(stmt.query(&[]));

    match result.next().unwrap().get_opt::<usize, i32>(1) {
        Err(Error::InvalidColumn) => {}
        res => panic!("unexpected result {:?}", res),
    };
}

#[test]
fn test_custom_notice_handler() {
    static mut count: usize = 0;
    struct Handler;

    impl NoticeHandler for Handler {
        fn handle(&mut self, notice: DbError) {
            assert_eq!("note", notice.message());
            unsafe { count += 1; }
        }
    }

    let conn = or_panic!(Connection::connect(
            "postgres://postgres@localhost?client_min_messages=NOTICE", &SslMode::None));
    conn.set_notice_handler(Box::new(Handler));
    or_panic!(conn.execute("CREATE FUNCTION pg_temp.note() RETURNS INT AS $$
                           BEGIN
                            RAISE NOTICE 'note';
                            RETURN 1;
                           END; $$ LANGUAGE plpgsql", &[]));
    or_panic!(conn.execute("SELECT pg_temp.note()", &[]));

    assert_eq!(unsafe { count }, 1);
}

#[test]
fn test_notification_iterator_none() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    assert!(conn.notifications().next().is_none());
}

fn check_notification(expected: Notification, actual: Notification) {
    assert_eq!(&expected.channel, &actual.channel);
    assert_eq!(&expected.payload, &actual.payload);
}

#[test]
fn test_notification_iterator_some() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let mut it = conn.notifications();
    or_panic!(conn.execute("LISTEN test_notification_iterator_one_channel", &[]));
    or_panic!(conn.execute("LISTEN test_notification_iterator_one_channel2", &[]));
    or_panic!(conn.execute("NOTIFY test_notification_iterator_one_channel, 'hello'", &[]));
    or_panic!(conn.execute("NOTIFY test_notification_iterator_one_channel2, 'world'", &[]));

    check_notification(Notification {
        pid: 0,
        channel: "test_notification_iterator_one_channel".to_string(),
        payload: "hello".to_string()
    }, it.next().unwrap());
    check_notification(Notification {
        pid: 0,
        channel: "test_notification_iterator_one_channel2".to_string(),
        payload: "world".to_string()
    }, it.next().unwrap());
    assert!(it.next().is_none());

    or_panic!(conn.execute("NOTIFY test_notification_iterator_one_channel, '!'", &[]));
    check_notification(Notification {
        pid: 0,
        channel: "test_notification_iterator_one_channel".to_string(),
        payload: "!".to_string()
    }, it.next().unwrap());
    assert!(it.next().is_none());
}

#[test]
fn test_notifications_next_block() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("LISTEN test_notifications_next_block", &[]));

    let _t = thread::spawn(|| {
        let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
        timer::sleep(Duration::milliseconds(500));
        or_panic!(conn.execute("NOTIFY test_notifications_next_block, 'foo'", &[]));
    });

    let mut notifications = conn.notifications();
    check_notification(Notification {
        pid: 0,
        channel: "test_notifications_next_block".to_string(),
        payload: "foo".to_string()
    }, or_panic!(notifications.next_block()));
}

#[test]
fn test_notifications_next_block_for() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("LISTEN test_notifications_next_block_for", &[]));

    let _t = thread::spawn(|| {
        let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
        timer::sleep(Duration::milliseconds(500));
        or_panic!(conn.execute("NOTIFY test_notifications_next_block_for, 'foo'", &[]));
    });

    let mut notifications = conn.notifications();
    check_notification(Notification {
        pid: 0,
        channel: "test_notifications_next_block_for".to_string(),
        payload: "foo".to_string()
    }, or_panic!(notifications.next_block_for(Duration::seconds(2)).unwrap()));
}

#[test]
fn test_notifications_next_block_for_timeout() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("LISTEN test_notifications_next_block_for_timeout", &[]));

    let _t = thread::spawn(|| {
        let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
        timer::sleep(Duration::seconds(2));
        or_panic!(conn.execute("NOTIFY test_notifications_next_block_for_timeout, 'foo'", &[]));
    });

    let mut notifications = conn.notifications();
    match notifications.next_block_for(Duration::milliseconds(500)) {
        None => {}
        Some(Err(e)) => panic!("Unexpected error {:?}", e),
        Some(Ok(_)) => panic!("expected error"),
    }

    or_panic!(conn.execute("SELECT 1", &[]));
}

#[test]
// This test is pretty sad, but I don't think there's a better way :(
fn test_cancel_query() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let cancel_data = conn.cancel_data();

    let _t = thread::spawn(move || {
        timer::sleep(Duration::milliseconds(500));
        assert!(postgres::cancel_query("postgres://postgres@localhost", &SslMode::None,
                                       cancel_data).is_ok());
    });

    match conn.execute("SELECT pg_sleep(10)", &[]) {
        Err(Error::DbError(ref e)) if e.code() == &QueryCanceled => {}
        Err(res) => panic!("Unexpected result {:?}", res),
        _ => panic!("Unexpected result"),
    }
}

#[test]
fn test_require_ssl_conn() {
    let ctx = SslContext::new(SslMethod::Sslv23).unwrap();
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost",
                                                    &SslMode::Require(ctx)));
    or_panic!(conn.execute("SELECT 1::VARCHAR", &[]));
}

#[test]
fn test_prefer_ssl_conn() {
    let ctx = SslContext::new(SslMethod::Sslv23).unwrap();
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost",
                                                    &SslMode::Prefer(ctx)));
    or_panic!(conn.execute("SELECT 1::VARCHAR", &[]));
}

#[test]
fn test_plaintext_pass() {
    or_panic!(Connection::connect("postgres://pass_user:password@localhost/postgres", &SslMode::None));
}

#[test]
fn test_plaintext_pass_no_pass() {
    let ret = Connection::connect("postgres://pass_user@localhost/postgres", &SslMode::None);
    match ret {
        Err(ConnectError::MissingPassword) => (),
        Err(err) => panic!("Unexpected error {:?}", err),
        _ => panic!("Expected error")
    }
}

#[test]
fn test_plaintext_pass_wrong_pass() {
    let ret = Connection::connect("postgres://pass_user:asdf@localhost/postgres", &SslMode::None);
    match ret {
        Err(ConnectError::DbError(ref e)) if e.code() == &InvalidPassword => {}
        Err(err) => panic!("Unexpected error {:?}", err),
        _ => panic!("Expected error")
    }
}

#[test]
fn test_md5_pass() {
    or_panic!(Connection::connect("postgres://md5_user:password@localhost/postgres", &SslMode::None));
}

#[test]
fn test_md5_pass_no_pass() {
    let ret = Connection::connect("postgres://md5_user@localhost/postgres", &SslMode::None);
    match ret {
        Err(ConnectError::MissingPassword) => (),
        Err(err) => panic!("Unexpected error {:?}", err),
        _ => panic!("Expected error")
    }
}

#[test]
fn test_md5_pass_wrong_pass() {
    let ret = Connection::connect("postgres://md5_user:asdf@localhost/postgres", &SslMode::None);
    match ret {
        Err(ConnectError::DbError(ref e)) if e.code() == &InvalidPassword => {}
        Err(err) => panic!("Unexpected error {:?}", err),
        _ => panic!("Expected error")
    }
}

#[test]
fn test_execute_copy_from_err() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT)", &[]));
    let stmt = or_panic!(conn.prepare("COPY foo (id) FROM STDIN"));
    match stmt.execute(&[]) {
        Err(Error::DbError(ref err)) if err.message().contains("COPY") => {}
        Err(err) => panic!("Unexptected error {:?}", err),
        _ => panic!("Expected error"),
    }
    match stmt.query(&[]) {
        Err(Error::DbError(ref err)) if err.message().contains("COPY") => {}
        Err(err) => panic!("Unexptected error {:?}", err),
        _ => panic!("Expected error"),
    }
}

#[test]
fn test_copy_in() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT, name VARCHAR)", &[]));

    let stmt = or_panic!(conn.prepare_copy_in("foo", &["id", "name"]));

    let data = (0i32..2).map(|i| {
        VecStreamIterator::new(vec![Box::new(i) as Box<ToSql>,
                                    Box::new(format!("{}", i)) as Box<ToSql>])
    });

    assert_eq!(Ok(2), stmt.execute(data));

    let stmt = or_panic!(conn.prepare("SELECT id, name FROM foo ORDER BY id"));
    assert_eq!(vec![(0i32, Some("0".to_string())), (1, Some("1".to_string()))],
               or_panic!(stmt.query(&[])).map(|r| (r.get(0), r.get(1))).collect::<Vec<_>>());
}

#[test]
fn test_copy_in_bad_column_count() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT, name VARCHAR)", &[]));

    let stmt = or_panic!(conn.prepare_copy_in("foo", &["id", "name"]));
    let data = vec![
        VecStreamIterator::new(vec![Box::new(1i32) as Box<ToSql>,
                                    Box::new("Steven".to_string()) as Box<ToSql>]),
        VecStreamIterator::new(vec![Box::new(2i32) as Box<ToSql>]),
    ].into_iter();

    let res = stmt.execute(data);
    match res {
        Err(Error::DbError(ref err)) if err.message().contains("Invalid column count") => {}
        Err(err) => panic!("unexpected error {:?}", err),
        _ => panic!("Expected error"),
    }

    let data = vec![
        VecStreamIterator::new(vec![Box::new(1i32) as Box<ToSql>,
                                    Box::new("Steven".to_string()) as Box<ToSql>]),
        VecStreamIterator::new(vec![Box::new(2i32) as Box<ToSql>,
                                    Box::new("Steven".to_string()) as Box<ToSql>,
                                    Box::new(3i64) as Box<ToSql>]),
    ].into_iter();

    let res = stmt.execute(data);
    match res {
        Err(Error::DbError(ref err)) if err.message().contains("Invalid column count") => {}
        Err(err) => panic!("unexpected error {:?}", err),
        _ => panic!("Expected error"),
    }

    or_panic!(conn.execute("SELECT 1", &[]));
}

#[test]
fn test_copy_in_bad_type() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT, name VARCHAR)", &[]));

    let stmt = or_panic!(conn.prepare_copy_in("foo", &["id", "name"]));

    let data = vec![
        VecStreamIterator::new(vec![Box::new(1i32) as Box<ToSql>,
                                    Box::new("Steven".to_string()) as Box<ToSql>]),
        VecStreamIterator::new(vec![Box::new(2i32) as Box<ToSql>,
                                    Box::new(1i32) as Box<ToSql>]),
    ].into_iter();

    let res = stmt.execute(data);
    match res {
        Err(Error::DbError(ref err)) if err.message().contains("saw type Varchar") => {}
        Err(err) => panic!("unexpected error {:?}", err),
        _ => panic!("Expected error"),
    }

    or_panic!(conn.execute("SELECT 1", &[]));
}

#[test]
fn test_batch_execute_copy_from_err() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT)", &[]));
    match conn.batch_execute("COPY foo (id) FROM STDIN") {
        Err(Error::DbError(ref err)) if err.message().contains("COPY") => {}
        Err(err) => panic!("Unexptected error {:?}", err),
        _ => panic!("Expected error"),
    }
}

#[test]
// Just make sure the impls don't infinite loop
fn test_generic_connection() {
    fn f<T>(t: &T) where T: GenericConnection {
        or_panic!(t.execute("SELECT 1", &[]));
    }

    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    f(&conn);
    let trans = or_panic!(conn.transaction());
    f(&trans);
}

#[test]
fn test_custom_range_element_type() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let trans = or_panic!(conn.transaction());
    or_panic!(trans.execute("CREATE TYPE floatrange AS RANGE (
                                subtype = float8,
                                subtype_diff = float8mi
                             )", &[]));
    let stmt = or_panic!(trans.prepare("SELECT $1::floatrange"));
    match &stmt.param_types()[0] {
        &Type::Unknown(ref u) => {
            assert_eq!("floatrange", u.name());
            assert_eq!(&Kind::Range(Type::Float8), u.kind());
        }
        t => panic!("Unexpected type {:?}", t)
    }
}

#[test]
fn test_prepare_cached() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (id INT)", &[]));
    or_panic!(conn.execute("INSERT INTO foo (id) VALUES (1), (2)", &[]));

    let stmt = or_panic!(conn.prepare_cached("SELECT id FROM foo ORDER BY id"));
    assert_eq!(&[1, 2][..], or_panic!(stmt.query(&[])).map(|r| r.get(0)).collect::<Vec<i32>>());
    or_panic!(stmt.finish());

    let stmt = or_panic!(conn.prepare_cached("SELECT id FROM foo ORDER BY id"));
    assert_eq!(&[1, 2][..], or_panic!(stmt.query(&[])).map(|r| r.get(0)).collect::<Vec<i32>>());
    or_panic!(stmt.finish());

    let stmt = or_panic!(conn.prepare_cached("SELECT id FROM foo ORDER BY id DESC"));
    assert_eq!(&[2, 1][..], or_panic!(stmt.query(&[])).map(|r| r.get(0)).collect::<Vec<i32>>());
    or_panic!(stmt.finish());
}

#[test]
fn test_is_active() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    assert!(conn.is_active());
    let trans = or_panic!(conn.transaction());
    assert!(!conn.is_active());
    assert!(trans.is_active());
    {
        let trans2 = or_panic!(trans.transaction());
        assert!(!conn.is_active());
        assert!(!trans.is_active());
        assert!(trans2.is_active());
        or_panic!(trans2.finish());
    }
    assert!(!conn.is_active());
    assert!(trans.is_active());
    or_panic!(trans.finish());
    assert!(conn.is_active());
}

#[test]
fn test_parameter() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    assert_eq!(Some("UTF8".to_string()), conn.parameter("client_encoding"));
    assert_eq!(None, conn.parameter("asdf"));
}

#[test]
fn test_get_bytes() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = or_panic!(conn.prepare("SELECT '\\x00010203'::BYTEA"));
    let mut result = or_panic!(stmt.query(&[]));
    assert_eq!(b"\x00\x01\x02\x03", result.next().unwrap().get_bytes(0).unwrap());
}

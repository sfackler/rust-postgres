extern crate env_logger;
extern crate tokio;
extern crate tokio_postgres;

#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;

use futures::future;
use futures::sync::mpsc;
use std::time::{Duration, Instant};
use tokio::prelude::*;
use tokio::runtime::current_thread::Runtime;
use tokio::timer::Delay;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{Kind, Type};
use tokio_postgres::{AsyncMessage, TlsMode};

fn smoke_test(url: &str) {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(url.parse().unwrap(), TlsMode::None);
    let (mut client, connection) = runtime.block_on(handshake).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    let prepare = client.prepare("SELECT 1::INT4");
    let statement = runtime.block_on(prepare).unwrap();
    let select = client.query(&statement, &[]).collect().map(|rows| {
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].get::<_, i32>(0), 1);
    });
    runtime.block_on(select).unwrap();

    drop(statement);
    drop(client);
    runtime.run().unwrap();
}

#[test]
fn plain_password_missing() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://pass_user@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    match runtime.block_on(handshake) {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.as_connection().is_some() => {}
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn plain_password_wrong() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://pass_user:foo@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    match runtime.block_on(handshake) {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.code() == Some(&SqlState::INVALID_PASSWORD) => {}
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn plain_password_ok() {
    smoke_test("postgres://pass_user:password@localhost:5433/postgres");
}

#[test]
fn md5_password_missing() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://md5_user@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    match runtime.block_on(handshake) {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.as_connection().is_some() => {}
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn md5_password_wrong() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://md5_user:foo@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    match runtime.block_on(handshake) {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.code() == Some(&SqlState::INVALID_PASSWORD) => {}
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn md5_password_ok() {
    smoke_test("postgres://md5_user:password@localhost:5433/postgres");
}

#[test]
fn scram_password_missing() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://scram_user@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    match runtime.block_on(handshake) {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.as_connection().is_some() => {}
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn scram_password_wrong() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://scram_user:foo@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    match runtime.block_on(handshake) {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.code() == Some(&SqlState::INVALID_PASSWORD) => {}
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn scram_password_ok() {
    smoke_test("postgres://scram_user:password@localhost:5433/postgres");
}

#[test]
fn pipelined_prepare() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://postgres@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    let (mut client, connection) = runtime.block_on(handshake).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    let prepare1 = client.prepare("SELECT 1::BIGINT WHERE $1::BOOL");
    let prepare2 = client.prepare("SELECT ''::TEXT, 1::FLOAT4 WHERE $1::VARCHAR IS NOT NULL");
    let prepare = prepare1.join(prepare2);
    let (statement1, statement2) = runtime.block_on(prepare).unwrap();

    assert_eq!(statement1.params(), &[Type::BOOL]);
    assert_eq!(statement1.columns().len(), 1);
    assert_eq!(statement1.columns()[0].type_(), &Type::INT8);

    assert_eq!(statement2.params(), &[Type::VARCHAR]);
    assert_eq!(statement2.columns().len(), 2);
    assert_eq!(statement2.columns()[0].type_(), &Type::TEXT);
    assert_eq!(statement2.columns()[1].type_(), &Type::FLOAT4);

    drop(statement1);
    drop(statement2);
    drop(client);
    runtime.run().unwrap();
}

#[test]
fn insert_select() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://postgres@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    let (mut client, connection) = runtime.block_on(handshake).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL, name TEXT)"))
        .unwrap();

    let insert = client.prepare("INSERT INTO foo (name) VALUES ($1), ($2)");
    let select = client.prepare("SELECT id, name FROM foo ORDER BY id");
    let prepare = insert.join(select);
    let (insert, select) = runtime.block_on(prepare).unwrap();

    let insert = client
        .execute(&insert, &[&"alice", &"bob"])
        .map(|n| assert_eq!(n, 2));
    let select = client.query(&select, &[]).collect().map(|rows| {
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].get::<_, i32>(0), 1);
        assert_eq!(rows[0].get::<_, &str>(1), "alice");
        assert_eq!(rows[1].get::<_, i32>(0), 2);
        assert_eq!(rows[1].get::<_, &str>(1), "bob");
    });
    let tests = insert.join(select);
    runtime.block_on(tests).unwrap();
}

#[test]
fn cancel_query() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://postgres@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    let (mut client, connection) = runtime.block_on(handshake).unwrap();
    let cancel_data = connection.cancel_data();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    let sleep = client
        .batch_execute("SELECT pg_sleep(100)")
        .then(|r| match r {
            Ok(_) => panic!("unexpected success"),
            Err(ref e) if e.code() == Some(&SqlState::QUERY_CANCELED) => Ok::<(), ()>(()),
            Err(e) => panic!("unexpected error {}", e),
        });
    let cancel = Delay::new(Instant::now() + Duration::from_millis(100))
        .then(|r| {
            r.unwrap();
            tokio_postgres::cancel_query(
                "postgres://postgres@localhost:5433".parse().unwrap(),
                TlsMode::None,
                cancel_data,
            )
        })
        .then(|r| {
            r.unwrap();
            Ok::<(), ()>(())
        });

    let ((), ()) = runtime.block_on(sleep.join(cancel)).unwrap();
}

#[test]
fn custom_enum() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://postgres@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    let (mut client, connection) = runtime.block_on(handshake).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE TYPE pg_temp.mood AS ENUM (
                'sad',
                'ok',
                'happy'
            )",
        ))
        .unwrap();

    let select = client.prepare("SELECT $1::mood");
    let select = runtime.block_on(select).unwrap();

    let ty = &select.params()[0];
    assert_eq!("mood", ty.name());
    assert_eq!(
        &Kind::Enum(vec![
            "sad".to_string(),
            "ok".to_string(),
            "happy".to_string(),
        ]),
        ty.kind()
    );
}

#[test]
fn custom_domain() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://postgres@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    let (mut client, connection) = runtime.block_on(handshake).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE DOMAIN pg_temp.session_id AS bytea CHECK(octet_length(VALUE) = 16)",
        ))
        .unwrap();

    let select = client.prepare("SELECT $1::session_id");
    let select = runtime.block_on(select).unwrap();

    let ty = &select.params()[0];
    assert_eq!("session_id", ty.name());
    assert_eq!(&Kind::Domain(Type::BYTEA), ty.kind());
}

#[test]
fn custom_array() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://postgres@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    let (mut client, connection) = runtime.block_on(handshake).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    let select = client.prepare("SELECT $1::HSTORE[]");
    let select = runtime.block_on(select).unwrap();

    let ty = &select.params()[0];
    assert_eq!("_hstore", ty.name());
    match *ty.kind() {
        Kind::Array(ref ty) => {
            assert_eq!("hstore", ty.name());
            assert_eq!(&Kind::Simple, ty.kind());
        }
        _ => panic!("unexpected kind"),
    }
}

#[test]
fn custom_composite() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://postgres@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    let (mut client, connection) = runtime.block_on(handshake).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE TYPE pg_temp.inventory_item AS (
                name TEXT,
                supplier INTEGER,
                price NUMERIC
            )",
        ))
        .unwrap();

    let select = client.prepare("SELECT $1::inventory_item");
    let select = runtime.block_on(select).unwrap();

    let ty = &select.params()[0];
    assert_eq!(ty.name(), "inventory_item");
    match *ty.kind() {
        Kind::Composite(ref fields) => {
            assert_eq!(fields[0].name(), "name");
            assert_eq!(fields[0].type_(), &Type::TEXT);
            assert_eq!(fields[1].name(), "supplier");
            assert_eq!(fields[1].type_(), &Type::INT4);
            assert_eq!(fields[2].name(), "price");
            assert_eq!(fields[2].type_(), &Type::NUMERIC);
        }
        ref t => panic!("bad type {:?}", t),
    }
}

#[test]
fn custom_range() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://postgres@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    let (mut client, connection) = runtime.block_on(handshake).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE TYPE pg_temp.floatrange AS RANGE (
                subtype = float8,
                subtype_diff = float8mi
            )",
        ))
        .unwrap();

    let select = client.prepare("SELECT $1::floatrange");
    let select = runtime.block_on(select).unwrap();

    let ty = &select.params()[0];
    assert_eq!("floatrange", ty.name());
    assert_eq!(&Kind::Range(Type::FLOAT8), ty.kind());
}

#[test]
fn custom_simple() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://postgres@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    let (mut client, connection) = runtime.block_on(handshake).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    let select = client.prepare("SELECT $1::HSTORE");
    let select = runtime.block_on(select).unwrap();

    let ty = &select.params()[0];
    assert_eq!("hstore", ty.name());
    assert_eq!(&Kind::Simple, ty.kind());
}

#[test]
fn notifications() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(
        "postgres://postgres@localhost:5433".parse().unwrap(),
        TlsMode::None,
    );
    let (mut client, mut connection) = runtime.block_on(handshake).unwrap();

    let (tx, rx) = mpsc::unbounded();
    let connection = future::poll_fn(move || {
        while let Some(message) = try_ready!(connection.poll_message().map_err(|e| panic!("{}", e)))
        {
            if let AsyncMessage::Notification(notification) = message {
                debug!("received {}", notification.payload);
                tx.unbounded_send(notification).unwrap();
            }
        }

        Ok(Async::Ready(()))
    });
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute("LISTEN test_notifications"))
        .unwrap();

    runtime
        .block_on(client.batch_execute("NOTIFY test_notifications, 'hello'"))
        .unwrap();

    runtime
        .block_on(client.batch_execute("NOTIFY test_notifications, 'world'"))
        .unwrap();

    drop(client);
    runtime.run().unwrap();

    let notifications = rx.collect().wait().unwrap();
    assert_eq!(notifications.len(), 2);
    assert_eq!(notifications[0].channel, "test_notifications");
    assert_eq!(notifications[0].payload, "hello");
    assert_eq!(notifications[1].channel, "test_notifications");
    assert_eq!(notifications[1].payload, "world");
}

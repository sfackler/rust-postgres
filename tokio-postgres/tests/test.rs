extern crate env_logger;
extern crate tokio;
extern crate tokio_postgres;

#[macro_use]
extern crate futures;
#[macro_use]
extern crate log;

use futures::future;
use futures::stream;
use futures::sync::mpsc;
use std::error::Error;
use std::time::{Duration, Instant};
use tokio::net::TcpStream;
use tokio::prelude::*;
use tokio::runtime::current_thread::Runtime;
use tokio::timer::Delay;
use tokio_postgres::error::SqlState;
use tokio_postgres::types::{Kind, Type};
use tokio_postgres::{AsyncMessage, Client, Connection, NoTls};

fn connect(
    builder: &tokio_postgres::Builder,
) -> impl Future<Item = (Client, Connection<TcpStream>), Error = tokio_postgres::Error> {
    let builder = builder.clone();
    TcpStream::connect(&"127.0.0.1:5433".parse().unwrap())
        .map_err(|e| panic!("{}", e))
        .and_then(move |s| builder.connect(s, NoTls))
}

fn smoke_test(builder: &tokio_postgres::Builder) {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = connect(builder);
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

    let handshake = connect(
        tokio_postgres::Builder::new()
            .user("pass_user")
            .database("postgres"),
    );
    runtime.block_on(handshake).err().unwrap();
}

#[test]
fn plain_password_wrong() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = connect(
        tokio_postgres::Builder::new()
            .user("pass_user")
            .password("foo")
            .database("postgres"),
    );
    match runtime.block_on(handshake) {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.code() == Some(&SqlState::INVALID_PASSWORD) => {}
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn plain_password_ok() {
    smoke_test(
        tokio_postgres::Builder::new()
            .user("pass_user")
            .password("password")
            .database("postgres"),
    );
}

#[test]
fn md5_password_missing() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = connect(
        tokio_postgres::Builder::new()
            .user("md5_user")
            .database("postgres"),
    );
    runtime.block_on(handshake).err().unwrap();
}

#[test]
fn md5_password_wrong() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = connect(
        tokio_postgres::Builder::new()
            .user("md5_user")
            .password("foo")
            .database("postgres"),
    );
    match runtime.block_on(handshake) {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.code() == Some(&SqlState::INVALID_PASSWORD) => {}
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn md5_password_ok() {
    smoke_test(
        tokio_postgres::Builder::new()
            .user("md5_user")
            .password("password")
            .database("postgres"),
    );
}

#[test]
fn scram_password_missing() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = connect(
        tokio_postgres::Builder::new()
            .user("scram_user")
            .database("postgres"),
    );
    runtime.block_on(handshake).err().unwrap();
}

#[test]
fn scram_password_wrong() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let handshake = connect(
        tokio_postgres::Builder::new()
            .user("scram_user")
            .password("foo")
            .database("postgres"),
    );
    match runtime.block_on(handshake) {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.code() == Some(&SqlState::INVALID_PASSWORD) => {}
        Err(e) => panic!("{}", e),
    }
}

#[test]
fn scram_password_ok() {
    smoke_test(
        tokio_postgres::Builder::new()
            .user("scram_user")
            .password("password")
            .database("postgres"),
    );
}

#[test]
fn pipelined_prepare() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    let prepare1 = client.prepare("SELECT $1::HSTORE[]");
    let prepare2 = client.prepare("SELECT $1::HSTORE[]");
    let prepare = prepare1.join(prepare2);
    runtime.block_on(prepare).unwrap();

    drop(client);
    runtime.run().unwrap();
}

#[test]
fn insert_select() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
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
fn query_portal() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE TEMPORARY TABLE foo (id SERIAL, name TEXT);
             INSERT INTO foo (name) VALUES ('alice'), ('bob'), ('charlie');
             BEGIN;",
        )).unwrap();

    let statement = runtime
        .block_on(client.prepare("SELECT id, name FROM foo ORDER BY id"))
        .unwrap();
    let portal = runtime.block_on(client.bind(&statement, &[])).unwrap();

    let f1 = client.query_portal(&portal, 2).collect();
    let f2 = client.query_portal(&portal, 2).collect();
    let f3 = client.query_portal(&portal, 2).collect();
    let (r1, r2, r3) = runtime.block_on(f1.join3(f2, f3)).unwrap();

    assert_eq!(r1.len(), 2);
    assert_eq!(r1[0].get::<_, i32>(0), 1);
    assert_eq!(r1[0].get::<_, &str>(1), "alice");
    assert_eq!(r1[1].get::<_, i32>(0), 2);
    assert_eq!(r1[1].get::<_, &str>(1), "bob");

    assert_eq!(r2.len(), 1);
    assert_eq!(r2[0].get::<_, i32>(0), 3);
    assert_eq!(r2[0].get::<_, &str>(1), "charlie");

    assert_eq!(r3.len(), 0);
}

#[test]
fn cancel_query() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
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
            TcpStream::connect(&"127.0.0.1:5433".parse().unwrap())
        }).then(|r| {
            let s = r.unwrap();
            tokio_postgres::cancel_query(s, NoTls, cancel_data)
        }).then(|r| {
            r.unwrap();
            Ok::<(), ()>(())
        });

    let ((), ()) = runtime.block_on(sleep.join(cancel)).unwrap();
}

#[test]
fn custom_enum() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE TYPE pg_temp.mood AS ENUM (
                'sad',
                'ok',
                'happy'
            )",
        )).unwrap();

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

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE DOMAIN pg_temp.session_id AS bytea CHECK(octet_length(VALUE) = 16)",
        )).unwrap();

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

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
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

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE TYPE pg_temp.inventory_item AS (
                name TEXT,
                supplier INTEGER,
                price NUMERIC
            )",
        )).unwrap();

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

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE TYPE pg_temp.floatrange AS RANGE (
                subtype = float8,
                subtype_diff = float8mi
            )",
        )).unwrap();

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

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
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

    let (mut client, mut connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();

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

#[test]
fn transaction_commit() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL,
                name TEXT
            )",
        )).unwrap();

    let f = client.batch_execute("INSERT INTO foo (name) VALUES ('steven')");
    runtime.block_on(client.transaction(f)).unwrap();

    let rows = runtime
        .block_on(
            client
                .prepare("SELECT name FROM foo")
                .and_then(|s| client.query(&s, &[]).collect()),
        ).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "steven");
}

#[test]
fn transaction_abort() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL,
                name TEXT
            )",
        )).unwrap();

    let f = client
        .batch_execute("INSERT INTO foo (name) VALUES ('steven')")
        .map_err(|e| Box::new(e) as Box<Error>)
        .and_then(|_| Err::<(), _>(Box::<Error>::from("")));
    runtime.block_on(client.transaction(f)).unwrap_err();

    let rows = runtime
        .block_on(
            client
                .prepare("SELECT name FROM foo")
                .and_then(|s| client.query(&s, &[]).collect()),
        ).unwrap();

    assert_eq!(rows.len(), 0);
}

#[test]
fn copy_in() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id INTEGER,
                name TEXT
             )",
        )).unwrap();

    let stream = stream::iter_ok::<_, String>(vec![b"1\tjim\n".to_vec(), b"2\tjoe\n".to_vec()]);
    let rows = runtime
        .block_on(
            client
                .prepare("COPY foo FROM STDIN")
                .and_then(|s| client.copy_in(&s, &[], stream)),
        ).unwrap();
    assert_eq!(rows, 2);

    let rows = runtime
        .block_on(
            client
                .prepare("SELECT id, name FROM foo ORDER BY id")
                .and_then(|s| client.query(&s, &[]).collect()),
        ).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "jim");
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, &str>(1), "joe");
}

#[test]
fn copy_in_error() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id INTEGER,
                name TEXT
             )",
        )).unwrap();

    let stream = stream::iter_result(vec![Ok(b"1\tjim\n".to_vec()), Err("asdf")]);
    let error = runtime
        .block_on(
            client
                .prepare("COPY foo FROM STDIN")
                .and_then(|s| client.copy_in(&s, &[], stream)),
        ).unwrap_err();
    assert!(error.to_string().contains("asdf"));

    let rows = runtime
        .block_on(
            client
                .prepare("SELECT id, name FROM foo ORDER BY id")
                .and_then(|s| client.query(&s, &[]).collect()),
        ).unwrap();

    assert_eq!(rows.len(), 0);
}

#[test]
fn copy_out() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime
        .block_on(connect(tokio_postgres::Builder::new().user("postgres")))
        .unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(client.batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL,
                name TEXT
            );
            INSERT INTO foo (name) VALUES ('jim'), ('joe');",
        )).unwrap();

    let data = runtime
        .block_on(
            client
                .prepare("COPY foo TO STDOUT")
                .and_then(|s| client.copy_out(&s, &[]).concat2()),
        ).unwrap();
    assert_eq!(&data[..], b"1\tjim\n2\tjoe\n");
}

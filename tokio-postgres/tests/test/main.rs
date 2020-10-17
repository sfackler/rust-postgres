#![warn(rust_2018_idioms)]

use bytes::{Bytes, BytesMut};
use futures::channel::mpsc;
use futures::{
    future, join, pin_mut, stream, try_join, FutureExt, SinkExt, StreamExt, TryStreamExt,
};
use std::fmt::Write;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::time;
use tokio_postgres::error::SqlState;
use tokio_postgres::tls::{NoTls, NoTlsStream};
use tokio_postgres::types::{Kind, Type};
use tokio_postgres::{
    AsyncMessage, Client, Config, Connection, Error, IsolationLevel, SimpleQueryMessage,
};

mod binary_copy;
mod parse;
#[cfg(feature = "runtime")]
mod runtime;
mod types;

async fn connect_raw(s: &str) -> Result<(Client, Connection<TcpStream, NoTlsStream>), Error> {
    let socket = TcpStream::connect("127.0.0.1:5433").await.unwrap();
    let config = s.parse::<Config>().unwrap();
    config.connect_raw(socket, NoTls).await
}

async fn connect(s: &str) -> Client {
    let (client, connection) = connect_raw(s).await.unwrap();
    let connection = connection.map(|r| r.unwrap());
    tokio::spawn(connection);
    client
}

#[tokio::test]
async fn plain_password_missing() {
    connect_raw("user=pass_user dbname=postgres")
        .await
        .err()
        .unwrap();
}

#[tokio::test]
async fn plain_password_wrong() {
    match connect_raw("user=pass_user password=foo dbname=postgres").await {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.code() == Some(&SqlState::INVALID_PASSWORD) => {}
        Err(e) => panic!("{}", e),
    }
}

#[tokio::test]
async fn plain_password_ok() {
    connect("user=pass_user password=password dbname=postgres").await;
}

#[tokio::test]
async fn md5_password_missing() {
    connect_raw("user=md5_user dbname=postgres")
        .await
        .err()
        .unwrap();
}

#[tokio::test]
async fn md5_password_wrong() {
    match connect_raw("user=md5_user password=foo dbname=postgres").await {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.code() == Some(&SqlState::INVALID_PASSWORD) => {}
        Err(e) => panic!("{}", e),
    }
}

#[tokio::test]
async fn md5_password_ok() {
    connect("user=md5_user password=password dbname=postgres").await;
}

#[tokio::test]
async fn scram_password_missing() {
    connect_raw("user=scram_user dbname=postgres")
        .await
        .err()
        .unwrap();
}

#[tokio::test]
async fn scram_password_wrong() {
    match connect_raw("user=scram_user password=foo dbname=postgres").await {
        Ok(_) => panic!("unexpected success"),
        Err(ref e) if e.code() == Some(&SqlState::INVALID_PASSWORD) => {}
        Err(e) => panic!("{}", e),
    }
}

#[tokio::test]
async fn scram_password_ok() {
    connect("user=scram_user password=password dbname=postgres").await;
}

#[tokio::test]
async fn pipelined_prepare() {
    let client = connect("user=postgres").await;

    let prepare1 = client.prepare("SELECT $1::HSTORE[]");
    let prepare2 = client.prepare("SELECT $1::BIGINT");

    let (statement1, statement2) = try_join!(prepare1, prepare2).unwrap();

    assert_eq!(statement1.params()[0].name(), "_hstore");
    assert_eq!(statement1.columns()[0].type_().name(), "_hstore");

    assert_eq!(statement2.params()[0], Type::INT8);
    assert_eq!(statement2.columns()[0].type_(), &Type::INT8);
}

#[tokio::test]
async fn insert_select() {
    let client = connect("user=postgres").await;

    client
        .batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL, name TEXT)")
        .await
        .unwrap();

    let insert = client.prepare("INSERT INTO foo (name) VALUES ($1), ($2)");
    let select = client.prepare("SELECT id, name FROM foo ORDER BY id");
    let (insert, select) = try_join!(insert, select).unwrap();

    let insert = client.execute(&insert, &[&"alice", &"bob"]);
    let select = client.query(&select, &[]);
    let (_, rows) = try_join!(insert, select).unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "alice");
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, &str>(1), "bob");
}

#[tokio::test]
async fn custom_enum() {
    let client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TYPE pg_temp.mood AS ENUM (
                'sad',
                'ok',
                'happy'
            )",
        )
        .await
        .unwrap();

    let select = client.prepare("SELECT $1::mood").await.unwrap();

    let ty = &select.params()[0];
    assert_eq!("mood", ty.name());
    assert_eq!(
        &Kind::Enum(vec![
            "sad".to_string(),
            "ok".to_string(),
            "happy".to_string(),
        ]),
        ty.kind(),
    );
}

#[tokio::test]
async fn custom_domain() {
    let client = connect("user=postgres").await;

    client
        .batch_execute("CREATE DOMAIN pg_temp.session_id AS bytea CHECK(octet_length(VALUE) = 16)")
        .await
        .unwrap();

    let select = client.prepare("SELECT $1::session_id").await.unwrap();

    let ty = &select.params()[0];
    assert_eq!("session_id", ty.name());
    assert_eq!(&Kind::Domain(Type::BYTEA), ty.kind());
}

#[tokio::test]
async fn custom_array() {
    let client = connect("user=postgres").await;

    let select = client.prepare("SELECT $1::HSTORE[]").await.unwrap();

    let ty = &select.params()[0];
    assert_eq!("_hstore", ty.name());
    match ty.kind() {
        Kind::Array(ty) => {
            assert_eq!("hstore", ty.name());
            assert_eq!(&Kind::Simple, ty.kind());
        }
        _ => panic!("unexpected kind"),
    }
}

#[tokio::test]
async fn custom_composite() {
    let client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TYPE pg_temp.inventory_item AS (
                name TEXT,
                supplier INTEGER,
                price NUMERIC
            )",
        )
        .await
        .unwrap();

    let select = client.prepare("SELECT $1::inventory_item").await.unwrap();

    let ty = &select.params()[0];
    assert_eq!(ty.name(), "inventory_item");
    match ty.kind() {
        Kind::Composite(fields) => {
            assert_eq!(fields[0].name(), "name");
            assert_eq!(fields[0].type_(), &Type::TEXT);
            assert_eq!(fields[1].name(), "supplier");
            assert_eq!(fields[1].type_(), &Type::INT4);
            assert_eq!(fields[2].name(), "price");
            assert_eq!(fields[2].type_(), &Type::NUMERIC);
        }
        _ => panic!("unexpected kind"),
    }
}

#[tokio::test]
async fn custom_range() {
    let client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TYPE pg_temp.floatrange AS RANGE (
                subtype = float8,
                subtype_diff = float8mi
            )",
        )
        .await
        .unwrap();

    let select = client.prepare("SELECT $1::floatrange").await.unwrap();

    let ty = &select.params()[0];
    assert_eq!("floatrange", ty.name());
    assert_eq!(&Kind::Range(Type::FLOAT8), ty.kind());
}

#[tokio::test]
async fn simple_query() {
    let client = connect("user=postgres").await;

    let messages = client
        .simple_query(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL,
                name TEXT
            );
            INSERT INTO foo (name) VALUES ('steven'), ('joe');
            SELECT * FROM foo ORDER BY id;",
        )
        .await
        .unwrap();

    match messages[0] {
        SimpleQueryMessage::CommandComplete(0) => {}
        _ => panic!("unexpected message"),
    }
    match messages[1] {
        SimpleQueryMessage::CommandComplete(2) => {}
        _ => panic!("unexpected message"),
    }
    match &messages[2] {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(0), Some("1"));
            assert_eq!(row.get(1), Some("steven"));
        }
        _ => panic!("unexpected message"),
    }
    match &messages[3] {
        SimpleQueryMessage::Row(row) => {
            assert_eq!(row.get(0), Some("2"));
            assert_eq!(row.get(1), Some("joe"));
        }
        _ => panic!("unexpected message"),
    }
    match messages[4] {
        SimpleQueryMessage::CommandComplete(2) => {}
        _ => panic!("unexpected message"),
    }
    assert_eq!(messages.len(), 5);
}

#[tokio::test]
async fn cancel_query_raw() {
    let client = connect("user=postgres").await;

    let socket = TcpStream::connect("127.0.0.1:5433").await.unwrap();
    let cancel_token = client.cancel_token();
    let cancel = cancel_token.cancel_query_raw(socket, NoTls);
    let cancel = time::sleep(Duration::from_millis(100)).then(|()| cancel);

    let sleep = client.batch_execute("SELECT pg_sleep(100)");

    match join!(sleep, cancel) {
        (Err(ref e), Ok(())) if e.code() == Some(&SqlState::QUERY_CANCELED) => {}
        t => panic!("unexpected return: {:?}", t),
    }
}

#[tokio::test]
async fn transaction_commit() {
    let mut client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo(
                id SERIAL,
                name TEXT
            )",
        )
        .await
        .unwrap();

    let transaction = client.transaction().await.unwrap();
    transaction
        .batch_execute("INSERT INTO foo (name) VALUES ('steven')")
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let stmt = client.prepare("SELECT name FROM foo").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "steven");
}

#[tokio::test]
async fn transaction_rollback() {
    let mut client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo(
                id SERIAL,
                name TEXT
            )",
        )
        .await
        .unwrap();

    let transaction = client.transaction().await.unwrap();
    transaction
        .batch_execute("INSERT INTO foo (name) VALUES ('steven')")
        .await
        .unwrap();
    transaction.rollback().await.unwrap();

    let stmt = client.prepare("SELECT name FROM foo").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();

    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn transaction_rollback_drop() {
    let mut client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo(
                id SERIAL,
                name TEXT
            )",
        )
        .await
        .unwrap();

    let transaction = client.transaction().await.unwrap();
    transaction
        .batch_execute("INSERT INTO foo (name) VALUES ('steven')")
        .await
        .unwrap();
    drop(transaction);

    let stmt = client.prepare("SELECT name FROM foo").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();

    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn transaction_builder() {
    let mut client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo(
                id SERIAL,
                name TEXT
            )",
        )
        .await
        .unwrap();

    let transaction = client
        .build_transaction()
        .isolation_level(IsolationLevel::Serializable)
        .read_only(true)
        .deferrable(true)
        .start()
        .await
        .unwrap();
    transaction
        .batch_execute("INSERT INTO foo (name) VALUES ('steven')")
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    let stmt = client.prepare("SELECT name FROM foo").await.unwrap();
    let rows = client.query(&stmt, &[]).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "steven");
}

#[tokio::test]
async fn copy_in() {
    let client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id INTEGER,
                name TEXT
            )",
        )
        .await
        .unwrap();

    let mut stream = stream::iter(
        vec![
            Bytes::from_static(b"1\tjim\n"),
            Bytes::from_static(b"2\tjoe\n"),
        ]
        .into_iter()
        .map(Ok::<_, Error>),
    );
    let sink = client.copy_in("COPY foo FROM STDIN").await.unwrap();
    pin_mut!(sink);
    sink.send_all(&mut stream).await.unwrap();
    let rows = sink.finish().await.unwrap();
    assert_eq!(rows, 2);

    let rows = client
        .query("SELECT id, name FROM foo ORDER BY id", &[])
        .await
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "jim");
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, &str>(1), "joe");
}

#[tokio::test]
async fn copy_in_large() {
    let client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id INTEGER,
                name TEXT
            )",
        )
        .await
        .unwrap();

    let a = Bytes::from_static(b"0\tname0\n");
    let mut b = BytesMut::new();
    for i in 1..5_000 {
        writeln!(b, "{0}\tname{0}", i).unwrap();
    }
    let mut c = BytesMut::new();
    for i in 5_000..10_000 {
        writeln!(c, "{0}\tname{0}", i).unwrap();
    }
    let mut stream = stream::iter(
        vec![a, b.freeze(), c.freeze()]
            .into_iter()
            .map(Ok::<_, Error>),
    );

    let sink = client.copy_in("COPY foo FROM STDIN").await.unwrap();
    pin_mut!(sink);
    sink.send_all(&mut stream).await.unwrap();
    let rows = sink.finish().await.unwrap();
    assert_eq!(rows, 10_000);
}

#[tokio::test]
async fn copy_in_error() {
    let client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id INTEGER,
                name TEXT
            )",
        )
        .await
        .unwrap();

    {
        let sink = client.copy_in("COPY foo FROM STDIN").await.unwrap();
        pin_mut!(sink);
        sink.send(Bytes::from_static(b"1\tsteven")).await.unwrap();
    }

    let rows = client
        .query("SELECT id, name FROM foo ORDER BY id", &[])
        .await
        .unwrap();
    assert_eq!(rows.len(), 0);
}

#[tokio::test]
async fn copy_out() {
    let client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
            id SERIAL,
            name TEXT
        );

        INSERT INTO foo (name) VALUES ('jim'), ('joe');",
        )
        .await
        .unwrap();

    let stmt = client.prepare("COPY foo TO STDOUT").await.unwrap();
    let data = client
        .copy_out(&stmt)
        .await
        .unwrap()
        .try_fold(BytesMut::new(), |mut buf, chunk| async move {
            buf.extend_from_slice(&chunk);
            Ok(buf)
        })
        .await
        .unwrap();
    assert_eq!(&data[..], b"1\tjim\n2\tjoe\n");
}

#[tokio::test]
async fn notices() {
    let long_name = "x".repeat(65);
    let (client, mut connection) =
        connect_raw(&format!("user=postgres application_name={}", long_name,))
            .await
            .unwrap();

    let (tx, rx) = mpsc::unbounded();
    let stream = stream::poll_fn(move |cx| connection.poll_message(cx)).map_err(|e| panic!(e));
    let connection = stream.forward(tx).map(|r| r.unwrap());
    tokio::spawn(connection);

    client
        .batch_execute("DROP DATABASE IF EXISTS noexistdb")
        .await
        .unwrap();

    drop(client);

    let notices = rx
        .filter_map(|m| match m {
            AsyncMessage::Notice(n) => future::ready(Some(n)),
            _ => future::ready(None),
        })
        .collect::<Vec<_>>()
        .await;
    assert_eq!(notices.len(), 2);
    assert_eq!(
        notices[0].message(),
        "identifier \"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\" \
         will be truncated to \"xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\""
    );
    assert_eq!(
        notices[1].message(),
        "database \"noexistdb\" does not exist, skipping"
    );
}

#[tokio::test]
async fn notifications() {
    let (client, mut connection) = connect_raw("user=postgres").await.unwrap();

    let (tx, rx) = mpsc::unbounded();
    let stream = stream::poll_fn(move |cx| connection.poll_message(cx)).map_err(|e| panic!(e));
    let connection = stream.forward(tx).map(|r| r.unwrap());
    tokio::spawn(connection);

    client
        .batch_execute(
            "LISTEN test_notifications;
             NOTIFY test_notifications, 'hello';
             NOTIFY test_notifications, 'world';",
        )
        .await
        .unwrap();

    drop(client);

    let notifications = rx
        .filter_map(|m| match m {
            AsyncMessage::Notification(n) => future::ready(Some(n)),
            _ => future::ready(None),
        })
        .collect::<Vec<_>>()
        .await;
    assert_eq!(notifications.len(), 2);
    assert_eq!(notifications[0].channel(), "test_notifications");
    assert_eq!(notifications[0].payload(), "hello");
    assert_eq!(notifications[1].channel(), "test_notifications");
    assert_eq!(notifications[1].payload(), "world");
}

#[tokio::test]
async fn query_portal() {
    let mut client = connect("user=postgres").await;

    client
        .batch_execute(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL,
                name TEXT
            );

            INSERT INTO foo (name) VALUES ('alice'), ('bob'), ('charlie');",
        )
        .await
        .unwrap();

    let stmt = client
        .prepare("SELECT id, name FROM foo ORDER BY id")
        .await
        .unwrap();

    let transaction = client.transaction().await.unwrap();

    let portal = transaction.bind(&stmt, &[]).await.unwrap();
    let f1 = transaction.query_portal(&portal, 2);
    let f2 = transaction.query_portal(&portal, 2);
    let f3 = transaction.query_portal(&portal, 2);

    let (r1, r2, r3) = try_join!(f1, f2, f3).unwrap();

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

#[tokio::test]
async fn require_channel_binding() {
    connect_raw("user=postgres channel_binding=require")
        .await
        .err()
        .unwrap();
}

#[tokio::test]
async fn prefer_channel_binding() {
    connect("user=postgres channel_binding=prefer").await;
}

#[tokio::test]
async fn disable_channel_binding() {
    connect("user=postgres channel_binding=disable").await;
}

#[tokio::test]
async fn check_send() {
    fn is_send<T: Send>(_: &T) {}

    let f = connect("user=postgres");
    is_send(&f);
    let mut client = f.await;

    let f = client.prepare("SELECT $1::TEXT");
    is_send(&f);
    let stmt = f.await.unwrap();

    let f = client.query(&stmt, &[&"hello"]);
    is_send(&f);
    drop(f);

    let f = client.execute(&stmt, &[&"hello"]);
    is_send(&f);
    drop(f);

    let f = client.transaction();
    is_send(&f);
    let trans = f.await.unwrap();

    let f = trans.query(&stmt, &[&"hello"]);
    is_send(&f);
    drop(f);

    let f = trans.execute(&stmt, &[&"hello"]);
    is_send(&f);
    drop(f);
}

#[tokio::test]
async fn query_one() {
    let client = connect("user=postgres").await;

    client
        .batch_execute(
            "
                CREATE TEMPORARY TABLE foo (
                    name TEXT
                );
                INSERT INTO foo (name) VALUES ('alice'), ('bob'), ('carol');
            ",
        )
        .await
        .unwrap();

    client
        .query_one("SELECT * FROM foo WHERE name = 'dave'", &[])
        .await
        .err()
        .unwrap();
    client
        .query_one("SELECT * FROM foo WHERE name = 'alice'", &[])
        .await
        .unwrap();
    client
        .query_one("SELECT * FROM foo", &[])
        .await
        .err()
        .unwrap();
}

#[tokio::test]
async fn query_opt() {
    let client = connect("user=postgres").await;

    client
        .batch_execute(
            "
                CREATE TEMPORARY TABLE foo (
                    name TEXT
                );
                INSERT INTO foo (name) VALUES ('alice'), ('bob'), ('carol');
            ",
        )
        .await
        .unwrap();

    assert!(client
        .query_opt("SELECT * FROM foo WHERE name = 'dave'", &[])
        .await
        .unwrap()
        .is_none());
    client
        .query_opt("SELECT * FROM foo WHERE name = 'alice'", &[])
        .await
        .unwrap()
        .unwrap();
    client
        .query_one("SELECT * FROM foo", &[])
        .await
        .err()
        .unwrap();
}

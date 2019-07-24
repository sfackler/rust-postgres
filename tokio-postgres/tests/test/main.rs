#![warn(rust_2018_idioms)]
#![feature(async_await)]

use futures::{try_join, FutureExt};
use tokio::net::TcpStream;
use tokio_postgres::error::SqlState;
use tokio_postgres::tls::{NoTls, NoTlsStream};
use tokio_postgres::types::Type;
use tokio_postgres::{Client, Config, Connection, Error};

mod parse;
#[cfg(feature = "runtime")]
mod runtime;
/*
mod types;
*/

async fn connect_raw(s: &str) -> Result<(Client, Connection<TcpStream, NoTlsStream>), Error> {
    let socket = TcpStream::connect(&"127.0.0.1:5433".parse().unwrap())
        .await
        .unwrap();
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
    let mut client = connect("user=postgres").await;

    let prepare1 = client.prepare("SELECT $1::TEXT");
    let prepare2 = client.prepare("SELECT $1::BIGINT");

    let (statement1, statement2) = try_join!(prepare1, prepare2).unwrap();

    assert_eq!(statement1.params()[0], Type::TEXT);
    assert_eq!(statement1.columns()[0].type_(), &Type::TEXT);

    assert_eq!(statement2.params()[0], Type::INT8);
    assert_eq!(statement2.columns()[0].type_(), &Type::INT8);
}

/*
#[test]
fn insert_select() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query("CREATE TEMPORARY TABLE foo (id SERIAL, name TEXT)")
                .for_each(|_| Ok(())),
        )
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

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query(
                    "CREATE TEMPORARY TABLE foo (id SERIAL, name TEXT);
                     INSERT INTO foo (name) VALUES ('alice'), ('bob'), ('charlie');
                     BEGIN;",
                )
                .for_each(|_| Ok(())),
        )
        .unwrap();

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
fn cancel_query_raw() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    let sleep = client
        .simple_query("SELECT pg_sleep(100)")
        .for_each(|_| Ok(()))
        .then(|r| match r {
            Ok(_) => panic!("unexpected success"),
            Err(ref e) if e.code() == Some(&SqlState::QUERY_CANCELED) => Ok::<(), ()>(()),
            Err(e) => panic!("unexpected error {}", e),
        });
    let cancel = Delay::new(Instant::now() + Duration::from_millis(100))
        .then(|r| {
            r.unwrap();
            TcpStream::connect(&"127.0.0.1:5433".parse().unwrap())
        })
        .then(|r| {
            let s = r.unwrap();
            client.cancel_query_raw(s, NoTls)
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

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query(
                    "CREATE TYPE pg_temp.mood AS ENUM (
                        'sad',
                        'ok',
                        'happy'
                    )",
                )
                .for_each(|_| Ok(())),
        )
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

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query(
                    "CREATE DOMAIN pg_temp.session_id AS bytea CHECK(octet_length(VALUE) = 16)",
                )
                .for_each(|_| Ok(())),
        )
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

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
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

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query(
                    "CREATE TYPE pg_temp.inventory_item AS (
                        name TEXT,
                        supplier INTEGER,
                        price NUMERIC
                    )",
                )
                .for_each(|_| Ok(())),
        )
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

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query(
                    "CREATE TYPE pg_temp.floatrange AS RANGE (
                        subtype = float8,
                        subtype_diff = float8mi
                    )",
                )
                .for_each(|_| Ok(())),
        )
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

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
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

    let (mut client, mut connection) = runtime.block_on(connect("user=postgres")).unwrap();

    let (tx, rx) = mpsc::unbounded();
    let connection = future::poll_fn(move || {
        while let Some(message) = try_ready!(connection.poll_message().map_err(|e| panic!("{}", e)))
        {
            if let AsyncMessage::Notification(notification) = message {
                debug!("received {}", notification.payload());
                tx.unbounded_send(notification).unwrap();
            }
        }

        Ok(Async::Ready(()))
    });
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query(
                    "LISTEN test_notifications;
                     NOTIFY test_notifications, 'hello';
                     NOTIFY test_notifications, 'world';",
                )
                .for_each(|_| Ok(())),
        )
        .unwrap();

    drop(client);
    runtime.run().unwrap();

    let notifications = rx.collect().wait().unwrap();
    assert_eq!(notifications.len(), 2);
    assert_eq!(notifications[0].channel(), "test_notifications");
    assert_eq!(notifications[0].payload(), "hello");
    assert_eq!(notifications[1].channel(), "test_notifications");
    assert_eq!(notifications[1].payload(), "world");
}

#[test]
fn transaction_commit() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query(
                    "CREATE TEMPORARY TABLE foo (
                        id SERIAL,
                        name TEXT
                    )",
                )
                .for_each(|_| Ok(())),
        )
        .unwrap();

    let f = client
        .simple_query("INSERT INTO foo (name) VALUES ('steven')")
        .for_each(|_| Ok(()));
    runtime
        .block_on(client.build_transaction().build(f))
        .unwrap();

    let rows = runtime
        .block_on(
            client
                .prepare("SELECT name FROM foo")
                .and_then(|s| client.query(&s, &[]).collect()),
        )
        .unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, &str>(0), "steven");
}

#[test]
fn transaction_abort() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query(
                    "CREATE TEMPORARY TABLE foo (
                        id SERIAL,
                        name TEXT
                    )",
                )
                .for_each(|_| Ok(())),
        )
        .unwrap();

    let f = client
        .simple_query("INSERT INTO foo (name) VALUES ('steven')")
        .for_each(|_| Ok(()))
        .map_err(|e| Box::new(e) as Box<dyn Error>)
        .and_then(|_| Err::<(), _>(Box::<dyn Error>::from("")));
    runtime
        .block_on(client.build_transaction().build(f))
        .unwrap_err();

    let rows = runtime
        .block_on(
            client
                .prepare("SELECT name FROM foo")
                .and_then(|s| client.query(&s, &[]).collect()),
        )
        .unwrap();

    assert_eq!(rows.len(), 0);
}

#[test]
fn copy_in() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query(
                    "CREATE TEMPORARY TABLE foo (
                        id INTEGER,
                        name TEXT
                    )",
                )
                .for_each(|_| Ok(())),
        )
        .unwrap();

    let stream = stream::iter_ok::<_, String>(vec![b"1\tjim\n".to_vec(), b"2\tjoe\n".to_vec()]);
    let rows = runtime
        .block_on(
            client
                .prepare("COPY foo FROM STDIN")
                .and_then(|s| client.copy_in(&s, &[], stream)),
        )
        .unwrap();
    assert_eq!(rows, 2);

    let rows = runtime
        .block_on(
            client
                .prepare("SELECT id, name FROM foo ORDER BY id")
                .and_then(|s| client.query(&s, &[]).collect()),
        )
        .unwrap();

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
    assert_eq!(rows[0].get::<_, &str>(1), "jim");
    assert_eq!(rows[1].get::<_, i32>(0), 2);
    assert_eq!(rows[1].get::<_, &str>(1), "joe");
}

#[test]
fn copy_in_large() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query(
                    "CREATE TEMPORARY TABLE foo (
                        id INTEGER,
                        name TEXT
                    )",
                )
                .for_each(|_| Ok(())),
        )
        .unwrap();

    let a = "0\tname0\n".to_string();
    let mut b = String::new();
    for i in 1..5_000 {
        writeln!(b, "{0}\tname{0}", i).unwrap();
    }
    let mut c = String::new();
    for i in 5_000..10_000 {
        writeln!(c, "{0}\tname{0}", i).unwrap();
    }

    let stream = stream::iter_ok::<_, String>(vec![a, b, c]);
    let rows = runtime
        .block_on(
            client
                .prepare("COPY foo FROM STDIN")
                .and_then(|s| client.copy_in(&s, &[], stream)),
        )
        .unwrap();
    assert_eq!(rows, 10_000);
}

#[test]
fn copy_in_error() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query(
                    "CREATE TEMPORARY TABLE foo (
                        id INTEGER,
                        name TEXT
                    )",
                )
                .for_each(|_| Ok(())),
        )
        .unwrap();

    let stream = stream::iter_result(vec![Ok(b"1\tjim\n".to_vec()), Err("asdf")]);
    let error = runtime
        .block_on(
            client
                .prepare("COPY foo FROM STDIN")
                .and_then(|s| client.copy_in(&s, &[], stream)),
        )
        .unwrap_err();
    assert!(error.to_string().contains("asdf"));

    let rows = runtime
        .block_on(
            client
                .prepare("SELECT id, name FROM foo ORDER BY id")
                .and_then(|s| client.query(&s, &[]).collect()),
        )
        .unwrap();

    assert_eq!(rows.len(), 0);
}

#[test]
fn copy_out() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    runtime
        .block_on(
            client
                .simple_query(
                    "CREATE TEMPORARY TABLE foo (
                        id SERIAL,
                        name TEXT
                    );
                    INSERT INTO foo (name) VALUES ('jim'), ('joe');",
                )
                .for_each(|_| Ok(())),
        )
        .unwrap();

    let data = runtime
        .block_on(
            client
                .prepare("COPY foo TO STDOUT")
                .and_then(|s| client.copy_out(&s, &[]).concat2()),
        )
        .unwrap();
    assert_eq!(&data[..], b"1\tjim\n2\tjoe\n");
}

#[test]
fn transaction_builder_around_moved_client() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    let transaction_builder = client.build_transaction();
    let work = client
        .simple_query(
            "CREATE TEMPORARY TABLE transaction_foo (
                id SERIAL,
                name TEXT
            )",
        )
        .for_each(|_| Ok(()))
        .and_then(move |_| {
            client
                .prepare("INSERT INTO transaction_foo (name) VALUES ($1), ($2)")
                .map(|statement| (client, statement))
        })
        .and_then(|(mut client, statement)| {
            client
                .query(&statement, &[&"jim", &"joe"])
                .collect()
                .map(|_res| client)
        });

    let transaction = transaction_builder.build(work);
    let mut client = runtime.block_on(transaction).unwrap();

    let data = runtime
        .block_on(
            client
                .prepare("COPY transaction_foo TO STDOUT")
                .and_then(|s| client.copy_out(&s, &[]).concat2()),
        )
        .unwrap();
    assert_eq!(&data[..], b"1\tjim\n2\tjoe\n");

    drop(client);
    runtime.run().unwrap();
}

#[test]
fn simple_query() {
    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    let f = client
        .simple_query(
            "CREATE TEMPORARY TABLE foo (
                id SERIAL,
                name TEXT
            );
            INSERT INTO foo (name) VALUES ('steven'), ('joe');
            SELECT * FROM foo ORDER BY id;",
        )
        .collect();
    let messages = runtime.block_on(f).unwrap();

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

#[test]
fn poll_idle_running() {
    struct DelayStream(Delay);

    impl Stream for DelayStream {
        type Item = Vec<u8>;
        type Error = tokio_postgres::Error;

        fn poll(&mut self) -> Poll<Option<Vec<u8>>, tokio_postgres::Error> {
            try_ready!(self.0.poll().map_err(|e| panic!("{}", e)));
            QUERY_DONE.store(true, Ordering::SeqCst);
            Ok(Async::Ready(None))
        }
    }

    struct IdleFuture(tokio_postgres::Client);

    impl Future for IdleFuture {
        type Item = ();
        type Error = tokio_postgres::Error;

        fn poll(&mut self) -> Poll<(), tokio_postgres::Error> {
            try_ready!(self.0.poll_idle());
            assert!(QUERY_DONE.load(Ordering::SeqCst));
            Ok(Async::Ready(()))
        }
    }

    static QUERY_DONE: AtomicBool = AtomicBool::new(false);

    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    let execute = client
        .simple_query("CREATE TEMPORARY TABLE foo (id INT)")
        .for_each(|_| Ok(()));
    runtime.block_on(execute).unwrap();

    let prepare = client.prepare("COPY foo FROM STDIN");
    let stmt = runtime.block_on(prepare).unwrap();
    let copy_in = client.copy_in(
        &stmt,
        &[],
        DelayStream(Delay::new(Instant::now() + Duration::from_millis(10))),
    );
    let copy_in = copy_in.map(|_| ()).map_err(|e| panic!("{}", e));
    runtime.spawn(copy_in);

    let future = IdleFuture(client);
    runtime.block_on(future).unwrap();
}

#[test]
fn poll_idle_new() {
    struct IdleFuture {
        client: tokio_postgres::Client,
        prepare: Option<impls::Prepare>,
    }

    impl Future for IdleFuture {
        type Item = ();
        type Error = tokio_postgres::Error;

        fn poll(&mut self) -> Poll<(), tokio_postgres::Error> {
            match self.prepare.take() {
                Some(_future) => {
                    assert!(!self.client.poll_idle().unwrap().is_ready());
                    Ok(Async::NotReady)
                }
                None => {
                    assert!(self.client.poll_idle().unwrap().is_ready());
                    Ok(Async::Ready(()))
                }
            }
        }
    }

    let _ = env_logger::try_init();
    let mut runtime = Runtime::new().unwrap();

    let (mut client, connection) = runtime.block_on(connect("user=postgres")).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.handle().spawn(connection).unwrap();

    let prepare = client.prepare("");
    let future = IdleFuture {
        client,
        prepare: Some(prepare),
    };
    runtime.block_on(future).unwrap();
}
*/

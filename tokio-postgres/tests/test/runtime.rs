use futures::{FutureExt, TryStreamExt};
use std::time::{Duration, Instant};
use tokio::timer::Delay;
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;

async fn smoke_test(s: &str) {
    let (mut client, connection) = tokio_postgres::connect(s, NoTls).await.unwrap();
    let connection = connection.map(|e| e.unwrap());
    tokio::spawn(connection);

    let stmt = client.prepare("SELECT $1::INT").await.unwrap();
    let rows = client
        .query(&stmt, &[&1i32])
        .await
        .unwrap()
        .try_collect::<Vec<_>>()
        .await
        .unwrap();
    assert_eq!(rows[0].get::<_, i32>(0), 1i32);
}

#[tokio::test]
#[ignore] // FIXME doesn't work with our docker-based tests :(
async fn unix_socket() {
    smoke_test("host=/var/run/postgresql port=5433 user=postgres").await;
}

#[tokio::test]
async fn tcp() {
    smoke_test("host=localhost port=5433 user=postgres").await;
}

#[tokio::test]
async fn multiple_hosts_one_port() {
    smoke_test("host=foobar.invalid,localhost port=5433 user=postgres").await;
}

#[tokio::test]
async fn multiple_hosts_multiple_ports() {
    smoke_test("host=foobar.invalid,localhost port=5432,5433 user=postgres").await;
}

#[tokio::test]
async fn wrong_port_count() {
    tokio_postgres::connect("host=localhost port=5433,5433 user=postgres", NoTls)
        .await
        .err()
        .unwrap();
}

/*
#[tokio::test]
async fn target_session_attrs_ok() {
    tokio_postgres::connect(
        "host=localhost port=5433 user=postgres target_session_attrs=read-write",
        NoTls,
    )
    .await
    .err()
    .unwrap();
}

#[tokio::test]
async fn target_session_attrs_err() {
    tokio_postgres::connect(
        "host=localhost port=5433 user=postgres target_session_attrs=read-write
         options='-c default_transaction_read_only=on'",
        NoTls,
    ).await.err().unwrap();
}

#[test]
fn cancel_query() {
    let mut runtime = Runtime::new().unwrap();

    let connect = tokio_postgres::connect("host=localhost port=5433 user=postgres", NoTls);
    let (mut client, connection) = runtime.block_on(connect).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.spawn(connection);

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
            client.cancel_query(NoTls)
        })
        .then(|r| {
            r.unwrap();
            Ok::<(), ()>(())
        });

    let ((), ()) = runtime.block_on(sleep.join(cancel)).unwrap();
}
*/

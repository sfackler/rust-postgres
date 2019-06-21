use futures::{Future, Stream};
use std::time::{Duration, Instant};
use tokio::runtime::current_thread::Runtime;
use tokio::timer::Delay;
use tokio_postgres::error::SqlState;
use tokio_postgres::NoTls;

fn smoke_test(s: &str) {
    let mut runtime = Runtime::new().unwrap();
    let connect = tokio_postgres::connect(s, NoTls);
    let (mut client, connection) = runtime.block_on(connect).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.spawn(connection);

    let execute = client.simple_query("SELECT 1").for_each(|_| Ok(()));
    runtime.block_on(execute).unwrap();
}

#[test]
#[ignore] // FIXME doesn't work with our docker-based tests :(
fn unix_socket() {
    smoke_test("host=/var/run/postgresql port=5433 user=postgres");
}

#[test]
fn tcp() {
    smoke_test("host=localhost port=5433 user=postgres")
}

#[test]
fn multiple_hosts_one_port() {
    smoke_test("host=foobar.invalid,localhost port=5433 user=postgres");
}

#[test]
fn multiple_hosts_multiple_ports() {
    smoke_test("host=foobar.invalid,localhost port=5432,5433 user=postgres");
}

#[test]
fn wrong_port_count() {
    let mut runtime = Runtime::new().unwrap();
    let f = tokio_postgres::connect("host=localhost port=5433,5433 user=postgres", NoTls);
    runtime.block_on(f).err().unwrap();

    let f = tokio_postgres::connect(
        "host=localhost,localhost,localhost port=5433,5433 user=postgres",
        NoTls,
    );
    runtime.block_on(f).err().unwrap();
}

#[test]
fn target_session_attrs_ok() {
    let mut runtime = Runtime::new().unwrap();
    let f = tokio_postgres::connect(
        "host=localhost port=5433 user=postgres target_session_attrs=read-write",
        NoTls,
    );
    let _r = runtime.block_on(f).unwrap();
}

#[test]
fn target_session_attrs_err() {
    let mut runtime = Runtime::new().unwrap();
    let f = tokio_postgres::connect(
        "host=localhost port=5433 user=postgres target_session_attrs=read-write
         options='-c default_transaction_read_only=on'",
        NoTls,
    );
    runtime.block_on(f).err().unwrap();
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

use futures::Future;
use tokio::runtime::current_thread::Runtime;
use tokio_postgres::{Client, Connection, Error, NoTls, Socket};

fn connect(s: &str) -> impl Future<Item = (Client, Connection<Socket>), Error = Error> {
    s.parse::<tokio_postgres::Builder>().unwrap().connect(NoTls)
}

#[test]
#[ignore] // FIXME doesn't work with our docker-based tests :(
fn unix_socket() {
    let mut runtime = Runtime::new().unwrap();

    let connect = connect("host=/var/run/postgresql port=5433 user=postgres");
    let (mut client, connection) = runtime.block_on(connect).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.spawn(connection);

    let execute = client.batch_execute("SELECT 1");
    runtime.block_on(execute).unwrap();
}

#[test]
fn tcp() {
    let mut runtime = Runtime::new().unwrap();

    let connect = connect("host=localhost port=5433 user=postgres");
    let (mut client, connection) = runtime.block_on(connect).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.spawn(connection);

    let execute = client.batch_execute("SELECT 1");
    runtime.block_on(execute).unwrap();
}

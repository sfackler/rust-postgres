use futures::{Future, Stream};
use native_tls::{self, Certificate};
use tokio::net::TcpStream;
use tokio::runtime::current_thread::Runtime;
use tokio_postgres::tls::TlsConnect;

#[cfg(feature = "runtime")]
use crate::MakeTlsConnector;
use crate::TlsConnector;

fn smoke_test<T>(s: &str, tls: T)
where
    T: TlsConnect<TcpStream>,
    T::Stream: 'static,
{
    let mut runtime = Runtime::new().unwrap();

    let builder = s.parse::<tokio_postgres::Config>().unwrap();

    let handshake = TcpStream::connect(&"127.0.0.1:5433".parse().unwrap())
        .map_err(|e| panic!("{}", e))
        .and_then(|s| builder.connect_raw(s, tls));
    let (client, connection) = runtime.block_on(handshake).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.spawn(connection);

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
fn require() {
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(
            Certificate::from_pem(include_bytes!("../../test/server.crt")).unwrap(),
        )
        .build()
        .unwrap();
    smoke_test(
        "user=ssl_user dbname=postgres sslmode=require",
        TlsConnector::new(connector, "localhost"),
    );
}

#[test]
fn prefer() {
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(
            Certificate::from_pem(include_bytes!("../../test/server.crt")).unwrap(),
        )
        .build()
        .unwrap();
    smoke_test(
        "user=ssl_user dbname=postgres",
        TlsConnector::new(connector, "localhost"),
    );
}

#[test]
fn scram_user() {
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(
            Certificate::from_pem(include_bytes!("../../test/server.crt")).unwrap(),
        )
        .build()
        .unwrap();
    smoke_test(
        "user=scram_user password=password dbname=postgres sslmode=require",
        TlsConnector::new(connector, "localhost"),
    );
}

#[test]
#[cfg(feature = "runtime")]
fn runtime() {
    let mut runtime = Runtime::new().unwrap();

    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(
            Certificate::from_pem(include_bytes!("../../test/server.crt")).unwrap(),
        )
        .build()
        .unwrap();
    let connector = MakeTlsConnector::new(connector);

    let connect = tokio_postgres::connect(
        "host=localhost port=5433 user=postgres sslmode=require",
        connector,
    );
    let (client, connection) = runtime.block_on(connect).unwrap();
    let connection = connection.map_err(|e| panic!("{}", e));
    runtime.spawn(connection);

    let execute = client.simple_query("SELECT 1").for_each(|_| Ok(()));
    runtime.block_on(execute).unwrap();
}

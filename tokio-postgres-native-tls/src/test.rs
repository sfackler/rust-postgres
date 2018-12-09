use futures::{Future, Stream};
use native_tls::{self, Certificate};
use tokio::net::TcpStream;
use tokio::runtime::current_thread::Runtime;
use tokio_postgres::{self, PreferTls, RequireTls, TlsMode};

use crate::TlsConnector;

fn smoke_test<T>(builder: &tokio_postgres::Builder, tls: T)
where
    T: TlsMode<TcpStream>,
    T::Stream: 'static,
{
    let mut runtime = Runtime::new().unwrap();

    let handshake = TcpStream::connect(&"127.0.0.1:5433".parse().unwrap())
        .map_err(|e| panic!("{}", e))
        .and_then(|s| builder.connect(s, tls));
    let (mut client, connection) = runtime.block_on(handshake).unwrap();
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
        tokio_postgres::Builder::new()
            .user("ssl_user")
            .database("postgres"),
        RequireTls(TlsConnector::with_connector(connector, "localhost")),
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
        tokio_postgres::Builder::new()
            .user("ssl_user")
            .database("postgres"),
        PreferTls(TlsConnector::with_connector(connector, "localhost")),
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
        tokio_postgres::Builder::new()
            .user("scram_user")
            .password("password")
            .database("postgres"),
        RequireTls(TlsConnector::with_connector(connector, "localhost")),
    );
}

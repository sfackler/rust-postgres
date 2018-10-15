use futures::{Future, Stream};
use native_tls::{self, Certificate};
use tokio::runtime::current_thread::Runtime;
use tokio_postgres::{self, TlsMode};

use TlsConnector;

fn smoke_test(url: &str, tls: TlsMode) {
    let mut runtime = Runtime::new().unwrap();

    let handshake = tokio_postgres::connect(url.parse().unwrap(), tls);
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
fn require() {
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(
            Certificate::from_pem(include_bytes!("../../test/server.crt")).unwrap(),
        ).build()
        .unwrap();
    let connector = TlsConnector::with_connector(connector);
    smoke_test(
        "postgres://ssl_user@localhost:5433/postgres",
        TlsMode::Require(Box::new(connector)),
    );
}

#[test]
fn prefer() {
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(
            Certificate::from_pem(include_bytes!("../../test/server.crt")).unwrap(),
        ).build()
        .unwrap();
    let connector = TlsConnector::with_connector(connector);
    smoke_test(
        "postgres://ssl_user@localhost:5433/postgres",
        TlsMode::Prefer(Box::new(connector)),
    );
}

#[test]
fn scram_user() {
    let connector = native_tls::TlsConnector::builder()
        .add_root_certificate(
            Certificate::from_pem(include_bytes!("../../test/server.crt")).unwrap(),
        ).build()
        .unwrap();
    let connector = TlsConnector::with_connector(connector);
    smoke_test(
        "postgres://scram_user:password@localhost:5433/postgres",
        TlsMode::Require(Box::new(connector)),
    );
}

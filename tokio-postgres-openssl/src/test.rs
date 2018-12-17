use futures::{Future, Stream};
use openssl::ssl::{SslConnector, SslMethod};
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
        .and_then(|s| builder.handshake(s, tls));
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
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    smoke_test(
        tokio_postgres::Builder::new()
            .user("ssl_user")
            .dbname("postgres"),
        RequireTls(TlsConnector::new(ctx.configure().unwrap(), "localhost")),
    );
}

#[test]
fn prefer() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    smoke_test(
        tokio_postgres::Builder::new()
            .user("ssl_user")
            .dbname("postgres"),
        PreferTls(TlsConnector::new(ctx.configure().unwrap(), "localhost")),
    );
}

#[test]
fn scram_user() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    smoke_test(
        tokio_postgres::Builder::new()
            .user("scram_user")
            .password("password")
            .dbname("postgres"),
        RequireTls(TlsConnector::new(ctx.configure().unwrap(), "localhost")),
    );
}

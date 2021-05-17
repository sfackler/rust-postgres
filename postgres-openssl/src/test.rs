use futures::FutureExt;
use openssl::ssl::{SslConnector, SslMethod};
use tokio::net::TcpStream;
use tokio_postgres::tls::TlsConnect;

use super::*;

async fn smoke_test<T>(s: &str, tls: T)
where
    T: TlsConnect<TcpStream>,
    T::Stream: 'static + Send,
{
    let stream = TcpStream::connect("127.0.0.1:5433").await.unwrap();

    let builder = s.parse::<tokio_postgres::Config>().unwrap();
    let (client, connection) = builder.connect_raw(stream, tls).await.unwrap();

    let connection = connection.map(|r| r.unwrap());
    tokio::spawn(connection);

    let stmt = client.prepare("SELECT $1::INT4").await.unwrap();
    let rows = client.query(&stmt, &[&1i32]).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
}

async fn from_tls_config_smoke_test(config: TlsConfig) {
    let mut connector = MakeTlsConnector::from_tls_config(config).unwrap();
    smoke_test(
        "user=ssl_user dbname=postgres",
        MakeTlsConnect::<TcpStream>::make_tls_connect(&mut connector, "localhost").unwrap(),
    )
    .await
}

#[tokio::test]
async fn require() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    smoke_test(
        "user=ssl_user dbname=postgres sslmode=require",
        TlsConnector::new(ctx.configure().unwrap(), "localhost"),
    )
    .await;
}

#[tokio::test]
async fn prefer() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    smoke_test(
        "user=ssl_user dbname=postgres",
        TlsConnector::new(ctx.configure().unwrap(), "localhost"),
    )
    .await;
}

#[tokio::test]
async fn scram_user() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    smoke_test(
        "user=scram_user password=password dbname=postgres sslmode=require",
        TlsConnector::new(ctx.configure().unwrap(), "localhost"),
    )
    .await;
}

#[tokio::test]
async fn require_channel_binding_err() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    let connector = TlsConnector::new(ctx.configure().unwrap(), "localhost");

    let stream = TcpStream::connect("127.0.0.1:5433").await.unwrap();
    let builder = "user=pass_user password=password dbname=postgres channel_binding=require"
        .parse::<tokio_postgres::Config>()
        .unwrap();
    builder.connect_raw(stream, connector).await.err().unwrap();
}

#[tokio::test]
async fn require_channel_binding_ok() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let ctx = builder.build();
    smoke_test(
        "user=scram_user password=password dbname=postgres channel_binding=require",
        TlsConnector::new(ctx.configure().unwrap(), "localhost"),
    )
    .await;
}

#[tokio::test]
#[cfg(feature = "runtime")]
async fn runtime() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let connector = MakeTlsConnector::new(builder.build());

    let (client, connection) = tokio_postgres::connect(
        "host=localhost port=5433 user=postgres sslmode=require",
        connector,
    )
    .await
    .unwrap();
    let connection = connection.map(|r| r.unwrap());
    tokio::spawn(connection);

    let stmt = client.prepare("SELECT $1::INT4").await.unwrap();
    let rows = client.query(&stmt, &[&1i32]).await.unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get::<_, i32>(0), 1);
}

#[tokio::test]
async fn from_tls_config_base() {
    from_tls_config_smoke_test(TlsConfig {
        mode: SslMode::Disable,
        client_cert: None,
        root_cert: None,
    })
    .await;

    from_tls_config_smoke_test(TlsConfig {
        mode: SslMode::Prefer,
        client_cert: None,
        root_cert: None,
    })
    .await;

    from_tls_config_smoke_test(TlsConfig {
        mode: SslMode::Require,
        client_cert: None,
        root_cert: None,
    })
    .await;

    from_tls_config_smoke_test(TlsConfig {
        mode: SslMode::Require,
        client_cert: None,
        root_cert: Some(PathBuf::from("../test/root.crt")),
    })
    .await;

    from_tls_config_smoke_test(TlsConfig {
        mode: SslMode::VerifyCa,
        client_cert: None,
        root_cert: Some(PathBuf::from("../test/root.crt")),
    })
    .await;

    from_tls_config_smoke_test(TlsConfig {
        mode: SslMode::VerifyFull,
        client_cert: None,
        root_cert: Some(PathBuf::from("../test/root.crt")),
    })
    .await;
}

#[tokio::test]
#[should_panic(expected = "certificate verify failed")]
async fn from_tls_config_require_with_wrong_root_cert_err() {
    from_tls_config_smoke_test(TlsConfig {
        mode: SslMode::Require,
        client_cert: None,
        root_cert: Some(PathBuf::from("../test/other.crt")),
    })
    .await;
}

#[tokio::test]
#[should_panic(expected = "certificate verify failed")]
async fn from_tls_config_verify_ca_with_wrong_root_cert_err() {
    from_tls_config_smoke_test(TlsConfig {
        mode: SslMode::VerifyCa,
        client_cert: None,
        root_cert: Some(PathBuf::from("../test/other.crt")),
    })
    .await;
}

#[tokio::test]
#[should_panic(expected = "certificate verify failed")]
async fn from_tls_config_verify_full_with_wrong_root_cert_err() {
    from_tls_config_smoke_test(TlsConfig {
        mode: SslMode::VerifyFull,
        client_cert: None,
        root_cert: Some(PathBuf::from("../test/other.crt")),
    })
    .await;
}

#[tokio::test]
#[should_panic(expected = "Hostname mismatch")]
async fn from_tls_config_verify_full_with_wrong_hostname_err() {
    let tls_config = TlsConfig {
        mode: SslMode::VerifyFull,
        client_cert: None,
        root_cert: Some(PathBuf::from("../test/root.crt")),
    };
    let mut connector = MakeTlsConnector::from_tls_config(tls_config).unwrap();
    smoke_test(
        "user=ssl_user dbname=postgres",
        MakeTlsConnect::<TcpStream>::make_tls_connect(&mut connector, "otherhost").unwrap(),
    )
    .await
}

#[tokio::test]
async fn from_tls_config_client_cert_verify_ca() {
    let tls_config = TlsConfig {
        mode: SslMode::VerifyCa,
        client_cert: Some((
            PathBuf::from("../test/postgres.crt"),
            PathBuf::from("../test/postgres.key"),
        )),
        root_cert: Some(PathBuf::from("../test/root.crt")),
    };
    let mut connector = MakeTlsConnector::from_tls_config(tls_config).unwrap();
    smoke_test(
        "user=cert_user_ca dbname=postgres",
        MakeTlsConnect::<TcpStream>::make_tls_connect(&mut connector, "localhost").unwrap(),
    )
    .await;
}

#[tokio::test]
async fn from_tls_config_client_cert_verify_full() {
    let tls_config = TlsConfig {
        mode: SslMode::VerifyFull,
        client_cert: Some((
            PathBuf::from("../test/cert_user_full.crt"),
            PathBuf::from("../test/cert_user_full.key"),
        )),
        root_cert: Some(PathBuf::from("../test/root.crt")),
    };
    let mut connector = MakeTlsConnector::from_tls_config(tls_config).unwrap();
    smoke_test(
        "user=cert_user_full dbname=postgres",
        MakeTlsConnect::<TcpStream>::make_tls_connect(&mut connector, "localhost").unwrap(),
    )
    .await;
}

#[tokio::test]
#[should_panic(expected = "connection requires a valid client certificate")]
async fn from_tls_config_client_cert_verify_full_no_cert_err() {
    let tls_config = TlsConfig {
        mode: SslMode::VerifyFull,
        client_cert: None,
        root_cert: Some(PathBuf::from("../test/root.crt")),
    };
    let mut connector = MakeTlsConnector::from_tls_config(tls_config).unwrap();
    smoke_test(
        "user=cert_user_full dbname=postgres",
        MakeTlsConnect::<TcpStream>::make_tls_connect(&mut connector, "localhost").unwrap(),
    )
    .await;
}

#[tokio::test]
#[should_panic(expected = "\\\"trust\\\" authentication failed for user \\\"cert_user_full\\\"")]
async fn from_tls_config_client_cert_verify_full_wrong_cert_err() {
    let tls_config = TlsConfig {
        mode: SslMode::VerifyFull,
        client_cert: Some((
            PathBuf::from("../test/postgres.crt"),
            PathBuf::from("../test/postgres.key"),
        )),
        root_cert: Some(PathBuf::from("../test/root.crt")),
    };
    let mut connector = MakeTlsConnector::from_tls_config(tls_config).unwrap();
    smoke_test(
        "user=cert_user_full dbname=postgres",
        MakeTlsConnect::<TcpStream>::make_tls_connect(&mut connector, "localhost").unwrap(),
    )
    .await;
}

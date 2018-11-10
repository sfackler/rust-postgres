use openssl::ssl::{SslConnector, SslMethod};
use postgres::{Connection, TlsMode};

use OpenSsl;

#[test]
fn require() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let negotiator = OpenSsl::with_connector(builder.build());
    let conn = Connection::connect(
        "postgres://ssl_user@localhost:5433/postgres",
        TlsMode::Require(&negotiator),
    ).unwrap();
    conn.execute("SELECT 1::VARCHAR", &[]).unwrap();
}

#[test]
fn prefer() {
    let mut builder = SslConnector::builder(SslMethod::tls()).unwrap();
    builder.set_ca_file("../test/server.crt").unwrap();
    let negotiator = OpenSsl::with_connector(builder.build());
    let conn = Connection::connect(
        "postgres://ssl_user@localhost:5433/postgres",
        TlsMode::Require(&negotiator),
    ).unwrap();
    conn.execute("SELECT 1::VARCHAR", &[]).unwrap();
}

use native_tls::{Certificate, TlsConnector};
use postgres::{Connection, TlsMode};

use NativeTls;

#[test]
fn connect() {
    let cert = include_bytes!("../../test/server.crt");
    let cert = Certificate::from_pem(cert).unwrap();

    let mut builder = TlsConnector::builder();
    builder.add_root_certificate(cert);
    let connector = builder.build().unwrap();

    let handshake = NativeTls::with_connector(connector);
    let conn = Connection::connect(
        "postgres://ssl_user@localhost:5433/postgres",
        TlsMode::Require(&handshake),
    ).unwrap();
    conn.execute("SELECT 1::VARCHAR", &[]).unwrap();
}

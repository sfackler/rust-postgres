extern mod postgres;

use postgres::PostgresConnection;

#[test]
fn test_connect() {
    let conn = PostgresConnection::connect("postgres://127.0.0.1:54322",
                                           "postgres");

    let stmt = conn.prepare("CREATE TABLE foo (id BIGINT PRIMARY KEY)");
}

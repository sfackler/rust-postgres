extern mod postgres;

use postgres::PostgresConnection;

#[test]
fn test_connect() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    let stmt = conn.prepare("CREATE TABLE foo (id BIGINT PRIMARY KEY)");
    stmt.query();
}

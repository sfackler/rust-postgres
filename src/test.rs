extern mod postgres;

use postgres::PostgresConnection;

#[test]
fn test_basic() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    do conn.in_transaction |conn| {
        conn.prepare("CREATE TABLE foo (id BIGINT PRIMARY KEY)").update();

        Err::<(), ()>(())
    };
}

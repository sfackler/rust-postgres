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

#[test]
fn test_query() {
    let conn = PostgresConnection::connect("postgres://postgres@127.0.0.1:5432");

    do conn.in_transaction |conn| {
        conn.prepare("CREATE TABLE foo (id BIGINT PRIMARY KEY)").update();
        conn.prepare("INSERT INTO foo (id) VALUES (1), (2)").update();
        let stmt = conn.prepare("SELECT * from foo ORDER BY id");
        let result = stmt.query();

        assert_eq!(~[1, 2], result.iter().map(|row| { row[0] }).collect());

        Err::<(), ()>(())
    };
}

extern mod postgres;

use postgres::{PostgresConnection, PostgresRow};

macro_rules! chk(
    ($e:expr) => (
        match $e {
            Ok(ok) => ok,
            Err(err) => fail!(err)
        }
    )
)

#[test]
fn test_conn() {
	let conn = chk!(PostgresConnection::new("postgres://postgres@localhost"));

    chk!(conn.update("DROP TABLE IF EXISTS foo"));
    chk!(conn.update("CREATE TABLE foo (foo INT PRIMARY KEY)"));
    chk!(conn.update("INSERT INTO foo (foo) VALUES (101)"));

    let res = chk!(conn.query("SELECT foo from foo"));
    assert_eq!(1, res.len());
    let rows: ~[PostgresRow] = res.iter().collect();
    assert_eq!(1, rows.len());
    assert_eq!(1, rows[0].len());
    assert_eq!(Some(101), rows[0][0]);
}

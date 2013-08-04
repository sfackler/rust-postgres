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
fn test_basic() {
    let conn = chk!(PostgresConnection::new("postgres://postgres@localhost"));

    do conn.in_transaction |conn| {
        chk!(conn.update("CREATE TABLE basic (id INT PRIMARY KEY)", []));
        chk!(conn.update("INSERT INTO basic (id) VALUES (101)", []));

        let res = chk!(conn.query("SELECT id from basic WHERE id = 101", []));
        assert_eq!(1, res.len());
        let rows: ~[PostgresRow] = res.iter().collect();
        assert_eq!(1, rows.len());
        assert_eq!(1, rows[0].len());
        assert_eq!(Some(101), rows[0][0]);

        Err::<(), ~str>(~"")
    };
}

#[test]
fn test_params() {
	let conn = chk!(PostgresConnection::new("postgres://postgres@localhost"));

    do conn.in_transaction |conn| {
        chk!(conn.update("CREATE TABLE basic (id INT PRIMARY KEY)", []));
        chk!(conn.update("INSERT INTO basic (id) VALUES ($1)", [~"101"]));

        let res = chk!(conn.query("SELECT id from basic WHERE id = $1", [~"101"]));
        assert_eq!(1, res.len());
        let rows: ~[PostgresRow] = res.iter().collect();
        assert_eq!(1, rows.len());
        assert_eq!(1, rows[0].len());
        assert_eq!(Some(101), rows[0][0]);

        Err::<(), ~str>(~"")
    };
}

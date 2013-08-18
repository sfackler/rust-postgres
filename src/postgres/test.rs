extern mod postgres;

use postgres::{PostgresConnection, ToSql};

macro_rules! params(
    ($($e:expr),+) => (
       [$(
          $e as &ToSql
       ),+]
    )
)

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
        assert_eq!(1, res.get(0).len());
        assert_eq!(1, res.get(0).len());
        assert_eq!(Some(101), res.get(0)[0]);

        Err::<(), ~str>(~"")
    };
}

#[test]
fn test_params() {
	let conn = chk!(PostgresConnection::new("postgres://postgres@localhost"));

    do conn.in_transaction |conn| {
        chk!(conn.update("CREATE TABLE basic (id INT PRIMARY KEY)", []));
        chk!(conn.update("INSERT INTO basic (id) VALUES ($1)",
                         params!(&101)));

        let res = chk!(conn.query("SELECT id from basic WHERE id = $1",
                                  params!(&101)));
        assert_eq!(Some(101), res.get(0)[0]);

        Err::<(), ~str>(~"")
    };
}

#[test]
fn test_null() {
    let conn = chk!(PostgresConnection::new("postgres://postgres@localhost"));

    do conn.in_transaction |conn| {
        chk!(conn.update("CREATE TABLE basic (id INT PRIMARY KEY, foo INT)",
                         []));
        chk!(conn.update("INSERT INTO basic (id, foo) VALUES ($1, $2), ($3, $4)",
                         params!(&101, &None::<int>, &102, &0)));

        let res = chk!(conn.query("SELECT foo from basic ORDER BY id", []));
        assert_eq!(None, res.get(0).get::<Option<int>>(0));
        assert_eq!(Some(0), res.get(1).get::<Option<int>>(0));

        Err::<(), ~str>(~"")
    };
}

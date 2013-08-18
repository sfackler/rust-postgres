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
fn test_multiple_stmts() {
    let conn = chk!(PostgresConnection::new("postgres://postgres@localhost"));

    do conn.in_transaction |conn| {
        chk!(conn.update("CREATE TABLE foo (id INT PRIMARY KEY)", []));
        let stmt1 = chk!(conn.prepare("INSERT INTO foo (id) VALUES (101)"));
        let stmt2 = chk!(conn.prepare("INSERT INTO foo (id) VALUES (102)"));

        chk!(stmt1.update([]));
        chk!(stmt2.update([]));

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

#[test]
fn test_types() {
    let conn = chk!(PostgresConnection::new("postgres://postgres@localhost"));

    do conn.in_transaction |conn| {
        chk!(conn.update(
            "CREATE TABLE foo (
                id INT PRIMARY KEY,
                str VARCHAR,
                float REAL
             )", []));
        chk!(conn.update("INSERT INTO foo (id, str, float)
                          VALUES ($1, $2, $3), ($4, $5, $6)",
                         params!(&101, & &"foobar", &10.5,
                                 &102, &None::<~str>, &None::<float>)));

        let res = chk!(conn.query("SELECT str, float from foo ORDER BY id", []));
        assert_eq!(~"foobar", res.get(0).get::<~str>(0));
        assert_eq!(10.5, res.get(0).get::<float>(1));
        assert_eq!(None::<~str>, res.get(1).get::<Option<~str>>(0));
        assert_eq!(None::<float>, res.get(1).get::<Option<float>>(1));

        Err::<(), ~str>(~"")
    };
}

extern crate test;
extern crate postgres;

use test::Bencher;

use postgres::{Connection, SslMode};

#[bench]
fn bench_naiive_execute(b: &mut test::Bencher) {
    let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
    conn.execute("CREATE TEMPORARY TABLE foo (id INT)", &[]).unwrap();

    b.iter(|| {
        let stmt = conn.prepare("UPDATE foo SET id = 1").unwrap();
        let out = stmt.execute(&[]).unwrap();
        stmt.finish().unwrap();
        out
    });
}

#[bench]
fn bench_execute(b: &mut test::Bencher) {
    let conn = Connection::connect("postgres://postgres@localhost", &SslMode::None).unwrap();
    conn.execute("CREATE TEMPORARY TABLE foo (id INT)", &[]).unwrap();

    b.iter(|| {
        conn.execute("UPDATE foo SET id = 1", &[]).unwrap()
    });
}

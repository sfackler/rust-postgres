use futures::Future;
use tokio_core::reactor::Core;

use super::*;
use error::{Error, ConnectError, SqlState};

#[test]
fn basic() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://postgres@localhost", &handle)
        .then(|c| c.unwrap().close());
    l.run(done).unwrap();
}

#[test]
fn md5_user() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://md5_user:password@localhost/postgres", &handle);
    l.run(done).unwrap();
}

#[test]
fn md5_user_no_pass() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://md5_user@localhost/postgres", &handle);
    match l.run(done) {
        Err(ConnectError::ConnectParams(_)) => {}
        Err(e) => panic!("unexpected error {}", e),
        Ok(_) => panic!("unexpected success"),
    }
}

#[test]
fn md5_user_wrong_pass() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://md5_user:foobar@localhost/postgres", &handle);
    match l.run(done) {
        Err(ConnectError::Db(ref e)) if e.code == SqlState::InvalidPassword => {}
        Err(e) => panic!("unexpected error {}", e),
        Ok(_) => panic!("unexpected success"),
    }
}

#[test]
fn pass_user() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://pass_user:password@localhost/postgres", &handle);
    l.run(done).unwrap();
}

#[test]
fn pass_user_no_pass() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://pass_user@localhost/postgres", &handle);
    match l.run(done) {
        Err(ConnectError::ConnectParams(_)) => {}
        Err(e) => panic!("unexpected error {}", e),
        Ok(_) => panic!("unexpected success"),
    }
}

#[test]
fn pass_user_wrong_pass() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://pass_user:foobar@localhost/postgres", &handle);
    match l.run(done) {
        Err(ConnectError::Db(ref e)) if e.code == SqlState::InvalidPassword => {}
        Err(e) => panic!("unexpected error {}", e),
        Ok(_) => panic!("unexpected success"),
    }
}

#[test]
fn batch_execute_ok() {
    let mut l = Core::new().unwrap();
    let done = Connection::connect("postgres://postgres@localhost", &l.handle())
        .then(|c| c.unwrap().batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL);"));
    l.run(done).unwrap();
}

#[test]
fn batch_execute_err() {
    let mut l = Core::new().unwrap();
    let done = Connection::connect("postgres://postgres@localhost", &l.handle())
        .then(|r| r.unwrap().batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL); \
                                            INSERT INTO foo DEFAULT VALUES;"))
        .and_then(|c| c.batch_execute("SELECT * FROM bogo"))
        .then(|r| {
             match r {
                 Err(Error::Db(e, s)) => {
                     assert!(e.code == SqlState::UndefinedTable);
                     s.batch_execute("SELECT * FROM foo")
                 }
                 Err(e) => panic!("unexpected error: {}", e),
                 Ok(_) => panic!("unexpected success"),
             }
        });
    l.run(done).unwrap();
}

#[test]
fn prepare_execute() {
    let mut l = Core::new().unwrap();
    let done = Connection::connect("postgres://postgres@localhost", &l.handle())
        .then(|c| {
            c.unwrap().prepare("CREATE TEMPORARY TABLE foo (id SERIAL PRIMARY KEY, name VARCHAR)")
        })
        .and_then(|(s, c)| c.execute(&s, &[]))
        .and_then(|(n, c)| {
            assert_eq!(0, n);
            c.prepare("INSERT INTO foo (name) VALUES ($1), ($2)")
        })
        .and_then(|(s, c)| c.execute(&s, &[&"steven", &"bob"]))
        .map(|(n, _)| assert_eq!(n, 2));
    l.run(done).unwrap();
}

use tokio_core::reactor::Core;

use super::*;
use error::{ConnectError, SqlState};

#[test]
fn basic() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://postgres@localhost", &handle);
    let conn = l.run(done).unwrap();
    assert!(conn.cancel_data().process_id != 0);
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

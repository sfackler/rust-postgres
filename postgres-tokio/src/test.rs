use tokio_core::reactor::Core;

use super::*;

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
fn pass_user() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://pass_user:password@localhost/postgres", &handle);
    l.run(done).unwrap();
}
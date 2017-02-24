use futures::{Future, Stream};
use futures_state_stream::StateStream;
use std::error::Error as StdError;
use std::path::PathBuf;
use std::time::Duration;
use tokio_core::reactor::{Core, Interval};

use super::*;
use error::{Error, ConnectError, SqlState};
use params::{ConnectParams, Host};
use types::{ToSql, FromSql, Type, IsNull, Kind};

#[test]
fn md5_user() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://md5_user:password@localhost/postgres",
                                   TlsMode::None,
                                   &handle);
    l.run(done).unwrap();
}

#[test]
fn md5_user_no_pass() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://md5_user@localhost/postgres",
                                   TlsMode::None,
                                   &handle);
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
    let done = Connection::connect("postgres://md5_user:foobar@localhost/postgres",
                                   TlsMode::None,
                                   &handle);
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
    let done = Connection::connect("postgres://pass_user:password@localhost/postgres",
                                   TlsMode::None,
                                   &handle);
    l.run(done).unwrap();
}

#[test]
fn pass_user_no_pass() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://pass_user@localhost/postgres",
                                   TlsMode::None,
                                   &handle);
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
    let done = Connection::connect("postgres://pass_user:foobar@localhost/postgres",
                                   TlsMode::None,
                                   &handle);
    match l.run(done) {
        Err(ConnectError::Db(ref e)) if e.code == SqlState::InvalidPassword => {}
        Err(e) => panic!("unexpected error {}", e),
        Ok(_) => panic!("unexpected success"),
    }
}

#[test]
fn batch_execute_ok() {
    let mut l = Core::new().unwrap();
    let done = Connection::connect("postgres://postgres@localhost", TlsMode::None, &l.handle())
        .then(|c| c.unwrap().batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL);"));
    l.run(done).unwrap();
}

#[test]
fn batch_execute_err() {
    let mut l = Core::new().unwrap();
    let done = Connection::connect("postgres://postgres@localhost", TlsMode::None, &l.handle())
        .then(|r| {
            r.unwrap()
                .batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL); INSERT INTO foo DEFAULT \
                                VALUES;")
        })
        .and_then(|c| c.batch_execute("SELECT * FROM bogo"))
        .then(|r| match r {
            Err(Error::Db(e, s)) => {
                assert!(e.code == SqlState::UndefinedTable);
                s.batch_execute("SELECT * FROM foo")
            }
            Err(e) => panic!("unexpected error: {}", e),
            Ok(_) => panic!("unexpected success"),
        });
    l.run(done).unwrap();
}

#[test]
fn prepare_execute() {
    let mut l = Core::new().unwrap();
    let done = Connection::connect("postgres://postgres@localhost", TlsMode::None, &l.handle())
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

#[test]
fn query() {
    let mut l = Core::new().unwrap();
    let done = Connection::connect("postgres://postgres@localhost", TlsMode::None, &l.handle())
        .then(|c| {
            c.unwrap().batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL, name VARCHAR);
                                      INSERT INTO foo (name) VALUES ('joe'), ('bob')")
        })
        .and_then(|c| c.prepare("SELECT id, name FROM foo ORDER BY id"))
        .and_then(|(s, c)| c.query(&s, &[]).collect())
        .and_then(|(r, c)| {
            assert_eq!(r[0].get::<i32, _>("id"), 1);
            assert_eq!(r[0].get::<String, _>("name"), "joe");
            assert_eq!(r[1].get::<i32, _>("id"), 2);
            assert_eq!(r[1].get::<String, _>("name"), "bob");
            c.prepare("")
        })
        .and_then(|(s, c)| c.query(&s, &[]).collect())
        .map(|(r, _)| assert!(r.is_empty()));
    l.run(done).unwrap();
}

#[test]
fn transaction() {
    let mut l = Core::new().unwrap();
    let done = Connection::connect("postgres://postgres@localhost", TlsMode::None, &l.handle())
        .then(|c| {
            c.unwrap().batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL, name VARCHAR);")
        })
        .then(|c| c.unwrap().transaction())
        .then(|t| t.unwrap().batch_execute("INSERT INTO foo (name) VALUES ('joe');"))
        .then(|t| t.unwrap().rollback())
        .then(|c| c.unwrap().transaction())
        .then(|t| t.unwrap().batch_execute("INSERT INTO foo (name) VALUES ('bob');"))
        .then(|t| t.unwrap().commit())
        .then(|c| c.unwrap().prepare("SELECT name FROM foo"))
        .and_then(|(s, c)| c.query(&s, &[]).collect())
        .map(|(r, _)| {
            assert_eq!(r.len(), 1);
            assert_eq!(r[0].get::<String, _>("name"), "bob");
        });
    l.run(done).unwrap();
}

#[test]
fn unix_socket() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://postgres@localhost", TlsMode::None, &handle)
        .then(|c| c.unwrap().prepare("SHOW unix_socket_directories"))
        .and_then(|(s, c)| c.query(&s, &[]).collect())
        .then(|r| {
            let r = r.unwrap().0;
            let params = ConnectParams::builder()
                .user("postgres", None)
                .build(Host::Unix(PathBuf::from(r[0].get::<String, _>(0))));
            Connection::connect(params, TlsMode::None, &handle)
        })
        .then(|c| c.unwrap().batch_execute(""));
    l.run(done).unwrap();
}

#[test]
fn ssl_user_ssl_required() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();

    let done = Connection::connect("postgres://ssl_user@localhost/postgres",
                                   TlsMode::None,
                                   &handle);

    match l.run(done) {
        Err(ConnectError::Db(e)) => assert!(e.code == SqlState::InvalidAuthorizationSpecification),
        Err(e) => panic!("unexpected error {}", e),
        Ok(_) => panic!("unexpected success"),
    }
}

#[cfg(feature = "with-openssl")]
#[test]
fn openssl_required() {
    use openssl::ssl::{SslMethod, SslConnectorBuilder};
    use tls::openssl::OpenSsl;

    let mut builder = SslConnectorBuilder::new(SslMethod::tls()).unwrap();
    builder.builder_mut().set_ca_file("../.travis/server.crt").unwrap();
    let negotiator = OpenSsl::from(builder.build());

    let mut l = Core::new().unwrap();
    let done = Connection::connect("postgres://ssl_user@localhost/postgres",
                                   TlsMode::Require(Box::new(negotiator)),
                                   &l.handle())
        .then(|c| c.unwrap().prepare("SELECT 1"))
        .and_then(|(s, c)| c.query(&s, &[]).collect())
        .map(|(r, _)| assert_eq!(r[0].get::<i32, _>(0), 1));
    l.run(done).unwrap();
}

#[test]
fn domain() {
    #[derive(Debug, PartialEq)]
    struct SessionId(Vec<u8>);

    impl ToSql for SessionId {
        fn to_sql(&self,
                  ty: &Type,
                  out: &mut Vec<u8>)
                  -> Result<IsNull, Box<StdError + Sync + Send>> {
            let inner = match *ty.kind() {
                Kind::Domain(ref inner) => inner,
                _ => unreachable!(),
            };
            self.0.to_sql(inner, out)
        }

        fn accepts(ty: &Type) -> bool {
            match *ty.kind() {
                Kind::Domain(Type::Bytea) => ty.name() == "session_id",
                _ => false,
            }
        }

        to_sql_checked!();
    }

    impl FromSql for SessionId {
        fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<StdError + Sync + Send>> {
            Vec::<u8>::from_sql(ty, raw).map(SessionId)
        }

        fn accepts(ty: &Type) -> bool {
            // This is super weird!
            <Vec<u8> as FromSql>::accepts(ty)
        }
    }

    let mut l = Core::new().unwrap();
    let handle = l.handle();
    let done = Connection::connect("postgres://postgres@localhost", TlsMode::None, &handle)
        .then(|c| {
            c.unwrap().batch_execute("CREATE DOMAIN pg_temp.session_id AS bytea \
                                      CHECK(octet_length(VALUE) = 16);
                 CREATE \
                                      TABLE pg_temp.foo (id pg_temp.session_id);")
        })
        .and_then(|c| c.prepare("INSERT INTO pg_temp.foo (id) VALUES ($1)"))
        .and_then(|(s, c)| {
            let id = SessionId(b"0123456789abcdef".to_vec());
            c.execute(&s, &[&id])
        })
        .and_then(|(_, c)| c.prepare("SELECT id FROM pg_temp.foo"))
        .and_then(|(s, c)| c.query(&s, &[]).collect())
        .map(|(r, _)| {
            let id = SessionId(b"0123456789abcdef".to_vec());
            assert_eq!(id, r[0].get(0));
        });

    l.run(done).unwrap();
}

#[test]
fn composite() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();

    let done = Connection::connect("postgres://postgres@localhost", TlsMode::None, &handle)
        .then(|c| {
            c.unwrap().batch_execute("CREATE TYPE pg_temp.inventory_item AS (
                                          name TEXT,
                                          supplier INTEGER,
                                          price NUMERIC
                                      )")
        })
        .and_then(|c| c.prepare("SELECT $1::inventory_item"))
        .map(|(s, _)| {
            let type_ = &s.parameters()[0];
            assert_eq!(type_.name(), "inventory_item");
            match *type_.kind() {
                Kind::Composite(ref fields) => {
                    assert_eq!(fields[0].name(), "name");
                    assert_eq!(fields[0].type_(), &Type::Text);
                    assert_eq!(fields[1].name(), "supplier");
                    assert_eq!(fields[1].type_(), &Type::Int4);
                    assert_eq!(fields[2].name(), "price");
                    assert_eq!(fields[2].type_(), &Type::Numeric);
                }
                ref t => panic!("bad type {:?}", t),
            }
        });
    l.run(done).unwrap();
}

#[test]
fn enum_() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();

    let done = Connection::connect("postgres://postgres@localhost", TlsMode::None, &handle)
        .then(|c| {
            c.unwrap().batch_execute("CREATE TYPE pg_temp.mood AS ENUM ('sad', 'ok', 'happy');")
        })
        .and_then(|c| c.prepare("SELECT $1::mood"))
        .map(|(s, _)| {
            let type_ = &s.parameters()[0];
            assert_eq!(type_.name(), "mood");
            match *type_.kind() {
                Kind::Enum(ref variants) => {
                    assert_eq!(variants,
                               &["sad".to_owned(), "ok".to_owned(), "happy".to_owned()]);
                }
                _ => panic!("bad type"),
            }
        });

    l.run(done).unwrap();
}

#[test]
fn cancel() {
    let mut l = Core::new().unwrap();
    let handle = l.handle();

    let done = Connection::connect("postgres://postgres@localhost", TlsMode::None, &handle)
        .then(move |c| {
            let c = c.unwrap();
            let cancel_data = c.cancel_data();
            let cancel = Interval::new(Duration::from_secs(1), &handle)
                .unwrap()
                .into_future()
                .then(move |r| {
                    assert!(r.is_ok());
                    cancel_query("postgres://postgres@localhost",
                                 TlsMode::None,
                                 cancel_data,
                                 &handle)
                })
                .then(Ok::<_, ()>);
            c.batch_execute("SELECT pg_sleep(10)")
                .then(Ok::<_, ()>)
                .join(cancel)
        });

    let (select, cancel) = l.run(done).unwrap();
    cancel.unwrap();
    match select {
        Err(Error::Db(e, _)) => assert_eq!(e.code, SqlState::QueryCanceled),
        Err(e) => panic!("unexpected error {}", e),
        Ok(_) => panic!("unexpected success"),
    }
}

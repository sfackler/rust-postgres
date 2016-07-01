use std::collections::HashMap;
use std::f32;
use std::f64;
use std::fmt;
use std::io::{Read, Write};

use postgres::{Connection, TlsMode, Result};
use postgres::error::Error;
use postgres::types::{ToSql, FromSql, WrongType, Type, IsNull, Kind, SessionInfo};

#[cfg(feature = "bit-vec")]
mod bit_vec;
#[cfg(feature = "eui48")]
mod eui48;
#[cfg(feature = "uuid")]
mod uuid;
#[cfg(feature = "time")]
mod time;
#[cfg(feature = "rustc-serialize")]
mod rustc_serialize;
#[cfg(feature = "serde_json")]
mod serde_json;
#[cfg(feature = "chrono")]
mod chrono;

fn test_type<T: PartialEq+FromSql+ToSql, S: fmt::Display>(sql_type: &str, checks: &[(T, S)]) {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", TlsMode::None));
    for &(ref val, ref repr) in checks.iter() {
        let stmt = or_panic!(conn.prepare(&*format!("SELECT {}::{}", *repr, sql_type)));
        let result = or_panic!(stmt.query(&[])).iter().next().unwrap().get(0);
        assert_eq!(val, &result);

        let stmt = or_panic!(conn.prepare(&*format!("SELECT $1::{}", sql_type)));
        let result = or_panic!(stmt.query(&[val])).iter().next().unwrap().get(0);
        assert_eq!(val, &result);
    }
}

#[test]
fn test_ref_tosql() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", TlsMode::None));
    let stmt = conn.prepare("SELECT $1::Int").unwrap();
    let num: &ToSql = &&7;
    stmt.query(&[num]).unwrap();
}

#[test]
fn test_bool_params() {
    test_type("BOOL", &[(Some(true), "'t'"), (Some(false), "'f'"),
                       (None, "NULL")]);
}

#[test]
fn test_i8_params() {
    test_type("\"char\"", &[(Some('a' as i8), "'a'"), (None, "NULL")]);
}

#[test]
fn test_name_params() {
    test_type("NAME", &[(Some("hello world".to_owned()), "'hello world'"),
                       (Some("イロハニホヘト チリヌルヲ".to_owned()), "'イロハニホヘト チリヌルヲ'"),
                       (None, "NULL")]);
}

#[test]
fn test_i16_params() {
    test_type("SMALLINT", &[(Some(15001i16), "15001"),
                           (Some(-15001i16), "-15001"), (None, "NULL")]);
}

#[test]
fn test_i32_params() {
    test_type("INT", &[(Some(2147483548i32), "2147483548"),
                      (Some(-2147483548i32), "-2147483548"), (None, "NULL")]);
}

#[test]
fn test_oid_params() {
    test_type("OID", &[(Some(2147483548u32), "2147483548"),
                       (Some(4000000000), "4000000000"), (None, "NULL")]);
}

#[test]
fn test_i64_params() {
    test_type("BIGINT", &[(Some(9223372036854775708i64), "9223372036854775708"),
                         (Some(-9223372036854775708i64), "-9223372036854775708"),
                         (None, "NULL")]);
}

#[test]
fn test_f32_params() {
    test_type("REAL", &[(Some(f32::INFINITY), "'infinity'"),
                       (Some(f32::NEG_INFINITY), "'-infinity'"),
                       (Some(1000.55), "1000.55"), (None, "NULL")]);
}

#[test]
fn test_f64_params() {
    test_type("DOUBLE PRECISION", &[(Some(f64::INFINITY), "'infinity'"),
                                   (Some(f64::NEG_INFINITY), "'-infinity'"),
                                   (Some(10000.55), "10000.55"),
                                   (None, "NULL")]);
}

#[test]
fn test_varchar_params() {
    test_type("VARCHAR", &[(Some("hello world".to_owned()), "'hello world'"),
                          (Some("イロハニホヘト チリヌルヲ".to_owned()), "'イロハニホヘト チリヌルヲ'"),
                          (None, "NULL")]);
}

#[test]
fn test_text_params() {
    test_type("TEXT", &[(Some("hello world".to_owned()), "'hello world'"),
                       (Some("イロハニホヘト チリヌルヲ".to_owned()), "'イロハニホヘト チリヌルヲ'"),
                       (None, "NULL")]);
}

#[test]
fn test_bpchar_params() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", TlsMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (
                            id SERIAL PRIMARY KEY,
                            b CHAR(5)
                           )", &[]));
    or_panic!(conn.execute("INSERT INTO foo (b) VALUES ($1), ($2), ($3)",
                           &[&Some("12345"), &Some("123"), &None::<&'static str>]));
    let stmt = or_panic!(conn.prepare("SELECT b FROM foo ORDER BY id"));
    let res = or_panic!(stmt.query(&[]));

    assert_eq!(vec!(Some("12345".to_owned()), Some("123  ".to_owned()), None),
               res.iter().map(|row| row.get(0)).collect::<Vec<_>>());
}

#[test]
fn test_citext_params() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", TlsMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (
                            id SERIAL PRIMARY KEY,
                            b CITEXT
                           )", &[]));
    or_panic!(conn.execute("INSERT INTO foo (b) VALUES ($1), ($2), ($3)",
                           &[&Some("foobar"), &Some("FooBar"), &None::<&'static str>]));
    let stmt = or_panic!(conn.prepare("SELECT id FROM foo WHERE b = 'FOOBAR' ORDER BY id"));
    let res = or_panic!(stmt.query(&[]));

    assert_eq!(vec!(Some(1i32), Some(2i32)), res.iter().map(|row| row.get(0)).collect::<Vec<_>>());
}

#[test]
fn test_bytea_params() {
    test_type("BYTEA", &[(Some(vec!(0u8, 1, 2, 3, 254, 255)), "'\\x00010203feff'"),
                        (None, "NULL")]);
}

#[test]
fn test_hstore_params() {
    macro_rules! make_map {
        ($($k:expr => $v:expr),+) => ({
            let mut map = HashMap::new();
            $(map.insert($k, $v);)+
            map
        })
    }
    test_type("hstore",
              &[(Some(make_map!("a".to_owned() => Some("1".to_owned()))), "'a=>1'"),
               (Some(make_map!("hello".to_owned() => Some("world!".to_owned()),
                               "hola".to_owned() => Some("mundo!".to_owned()),
                               "what".to_owned() => None)),
                "'hello=>world!,hola=>mundo!,what=>NULL'"),
                (None, "NULL")]);
}

#[test]
fn test_array_params() {
    test_type("integer[]", &[(Some(vec!(1i32, 2i32)), "ARRAY[1,2]"),
                             (Some(vec!(1i32)), "ARRAY[1]"),
                             (Some(vec!()), "ARRAY[]"),
                             (None, "NULL")]);
}

fn test_nan_param<T: PartialEq+ToSql+FromSql>(sql_type: &str) {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", TlsMode::None));
    let stmt = or_panic!(conn.prepare(&*format!("SELECT 'NaN'::{}", sql_type)));
    let result = or_panic!(stmt.query(&[]));
    let val: T = result.iter().next().unwrap().get(0);
    assert!(val != val);
}

#[test]
fn test_f32_nan_param() {
    test_nan_param::<f32>("REAL");
}

#[test]
fn test_f64_nan_param() {
    test_nan_param::<f64>("DOUBLE PRECISION");
}

#[test]
fn test_pg_database_datname() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", TlsMode::None));
    let stmt = or_panic!(conn.prepare("SELECT datname FROM pg_database"));
    let result = or_panic!(stmt.query(&[]));

    let next = result.iter().next().unwrap();
    or_panic!(next.get_opt::<_, String>(0).unwrap());
    or_panic!(next.get_opt::<_, String>("datname").unwrap());
}

#[test]
fn test_slice() {
    let conn = Connection::connect("postgres://postgres@localhost", TlsMode::None).unwrap();
    conn.batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL PRIMARY KEY, f VARCHAR);
                        INSERT INTO foo (f) VALUES ('a'), ('b'), ('c'), ('d');").unwrap();

    let stmt = conn.prepare("SELECT f FROM foo WHERE id = ANY($1)").unwrap();
    let result = stmt.query(&[&&[1i32, 3, 4][..]]).unwrap();
    assert_eq!(vec!["a".to_owned(), "c".to_owned(), "d".to_owned()],
               result.iter().map(|r| r.get::<_, String>(0)).collect::<Vec<_>>());
}

#[test]
fn test_slice_wrong_type() {
    let conn = Connection::connect("postgres://postgres@localhost", TlsMode::None).unwrap();
    conn.batch_execute("CREATE TEMPORARY TABLE foo (id SERIAL PRIMARY KEY)").unwrap();

    let stmt = conn.prepare("SELECT * FROM foo WHERE id = ANY($1)").unwrap();
    match stmt.query(&[&&["hi"][..]]) {
        Ok(_) => panic!("Unexpected success"),
        Err(Error::Conversion(ref e)) if e.is::<WrongType>() => {}
        Err(e) => panic!("Unexpected error {:?}", e),
    };
}

#[test]
fn test_slice_range() {
    let conn = Connection::connect("postgres://postgres@localhost", TlsMode::None).unwrap();

    let stmt = conn.prepare("SELECT $1::INT8RANGE").unwrap();
    match stmt.query(&[&&[1i64][..]]) {
        Ok(_) => panic!("Unexpected success"),
        Err(Error::Conversion(ref e)) if e.is::<WrongType>() => {}
        Err(e) => panic!("Unexpected error {:?}", e),
    };
}

#[test]
fn domain() {
    #[derive(Debug, PartialEq)]
    struct SessionId(Vec<u8>);

    impl ToSql for SessionId {
        fn to_sql<W: ?Sized>(&self, ty: &Type, out: &mut W, ctx: &SessionInfo) -> Result<IsNull>
            where W: Write
        {
            let inner = match *ty.kind() {
                Kind::Domain(ref inner) => inner,
                _ => unreachable!(),
            };
            self.0.to_sql(inner, out, ctx)
        }

        fn accepts(ty: &Type) -> bool {
            ty.name() == "session_id" && match *ty.kind() { Kind::Domain(_) => true, _ => false }
        }

        to_sql_checked!();
    }

    impl FromSql for SessionId {
        fn from_sql<R: Read>(ty: &Type, raw: &mut R, ctx: &SessionInfo) -> Result<Self> {
            Vec::<u8>::from_sql(ty, raw, ctx).map(SessionId)
        }

        fn accepts(ty: &Type) -> bool {
            // This is super weird!
            <Vec<u8> as FromSql>::accepts(ty)
        }
    }

    let conn = Connection::connect("postgres://postgres@localhost", TlsMode::None).unwrap();
    conn.batch_execute("CREATE DOMAIN pg_temp.session_id AS bytea CHECK(octet_length(VALUE) = 16);
                        CREATE TABLE pg_temp.foo (id pg_temp.session_id);")
        .unwrap();

    let id = SessionId(b"0123456789abcdef".to_vec());
    conn.execute("INSERT INTO pg_temp.foo (id) VALUES ($1)", &[&id]).unwrap();
    let rows = conn.query("SELECT id FROM pg_temp.foo", &[]).unwrap();
    assert_eq!(id, rows.get(0).get(0));
}

#[test]
fn composite() {
    let conn = Connection::connect("postgres://postgres@localhost", TlsMode::None).unwrap();
    conn.batch_execute("CREATE TYPE pg_temp.inventory_item AS (
                            name TEXT,
                            supplier INTEGER,
                            price NUMERIC
                        )")
        .unwrap();

    let stmt = conn.prepare("SELECT $1::inventory_item").unwrap();
    let type_ = &stmt.param_types()[0];
    assert_eq!(type_.name(), "inventory_item");
    match type_.kind() {
        &Kind::Composite(ref fields) => {
            assert_eq!(fields[0].name(), "name");
            assert_eq!(fields[0].type_(), &Type::Text);
            assert_eq!(fields[1].name(), "supplier");
            assert_eq!(fields[1].type_(), &Type::Int4);
            assert_eq!(fields[2].name(), "price");
            assert_eq!(fields[2].type_(), &Type::Numeric);
        }
        t => panic!("bad type {:?}", t),
    }
}

#[test]
fn enum_() {
    let conn = Connection::connect("postgres://postgres@localhost", TlsMode::None).unwrap();
    conn.batch_execute("CREATE TYPE pg_temp.mood AS ENUM ('sad', 'ok', 'happy');").unwrap();

    let stmt = conn.prepare("SELECT $1::mood").unwrap();
    let type_ = &stmt.param_types()[0];
    assert_eq!(type_.name(), "mood");
    match type_.kind() {
        &Kind::Enum(ref variants) => {
            assert_eq!(variants, &["sad".to_owned(), "ok".to_owned(), "happy".to_owned()]);
        }
        _ => panic!("bad type"),
    }
}

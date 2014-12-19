use serialize::json;
use std::collections::HashMap;
use std::f32;
use std::f64;
use std::fmt;
use std::num::Float;
use std::io::net::ip::IpAddr;

use postgres::{Connection, SslMode};
use postgres::types::{ToSql, FromSql};

macro_rules! test_array_params {
    ($name:expr, $v1:expr, $s1:expr, $v2:expr, $s2:expr, $v3:expr, $s3:expr) => ({
        use postgres::types::array::ArrayBase;
        use types::test_type;

        let tests = &[(Some(ArrayBase::from_vec(vec!(Some($v1), Some($v2), None), 1)),
                      format!("'{{{},{},NULL}}'", $s1, $s2).into_string()),
                     (None, "NULL".to_string())];
        test_type(format!("{}[]", $name)[], tests);
        let mut a = ArrayBase::from_vec(vec!(Some($v1), Some($v2)), 0);
        a.wrap(-1);
        a.push_move(ArrayBase::from_vec(vec!(None, Some($v3)), 0));
        let tests = &[(Some(a), format!("'[-1:0][0:1]={{{{{},{}}},{{NULL,{}}}}}'",
                                       $s1, $s2, $s3).into_string())];
        test_type(format!("{}[][]", $name)[], tests);
    })
}

macro_rules! test_range {
    ($name:expr, $t:ty, $low:expr, $low_str:expr, $high:expr, $high_str:expr) => ({
        let tests = &[(Some(range!('(', ')')), "'(,)'".to_string()),
                     (Some(range!('[' $low, ')')), format!("'[{},)'", $low_str)),
                     (Some(range!('(' $low, ')')), format!("'({},)'", $low_str)),
                     (Some(range!('(', $high ']')), format!("'(,{}]'", $high_str)),
                     (Some(range!('(', $high ')')), format!("'(,{})'", $high_str)),
                     (Some(range!('[' $low, $high ']')),
                      format!("'[{},{}]'", $low_str, $high_str)),
                     (Some(range!('[' $low, $high ')')),
                      format!("'[{},{})'", $low_str, $high_str)),
                     (Some(range!('(' $low, $high ']')),
                      format!("'({},{}]'", $low_str, $high_str)),
                     (Some(range!('(' $low, $high ')')),
                      format!("'({},{})'", $low_str, $high_str)),
                     (Some(range!(empty)), "'empty'".to_string()),
                     (None, "NULL".to_string())];
        test_type($name, tests);
    })
}

mod array;
mod range;
#[cfg(feature = "uuid")]
mod uuid;
#[cfg(feature = "time")]
mod time;

fn test_type<T: PartialEq+FromSql+ToSql, S: fmt::Show>(sql_type: &str, checks: &[(T, S)]) {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    for &(ref val, ref repr) in checks.iter() {
        let stmt = or_panic!(conn.prepare(format!("SELECT {}::{}", *repr, sql_type)[]));
        let result = or_panic!(stmt.query(&[])).next().unwrap().get(0u);
        assert!(val == &result);

        let stmt = or_panic!(conn.prepare(format!("SELECT $1::{}", sql_type)[]));
        let result = or_panic!(stmt.query(&[val])).next().unwrap().get(0u);
        assert!(val == &result);
    }
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
    test_type("NAME", &[(Some("hello world".to_string()), "'hello world'"),
                       (Some("イロハニホヘト チリヌルヲ".to_string()), "'イロハニホヘト チリヌルヲ'"),
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
    test_type("VARCHAR", &[(Some("hello world".to_string()), "'hello world'"),
                          (Some("イロハニホヘト チリヌルヲ".to_string()), "'イロハニホヘト チリヌルヲ'"),
                          (None, "NULL")]);
}

#[test]
fn test_text_params() {
    test_type("TEXT", &[(Some("hello world".to_string()), "'hello world'"),
                       (Some("イロハニホヘト チリヌルヲ".to_string()), "'イロハニホヘト チリヌルヲ'"),
                       (None, "NULL")]);
}

#[test]
fn test_bpchar_params() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (
                            id SERIAL PRIMARY KEY,
                            b CHAR(5)
                           )", &[]));
    or_panic!(conn.execute("INSERT INTO foo (b) VALUES ($1), ($2), ($3)",
                           &[&Some("12345"), &Some("123"), &None::<&'static str>]));
    let stmt = or_panic!(conn.prepare("SELECT b FROM foo ORDER BY id"));
    let res = or_panic!(stmt.query(&[]));

    assert_eq!(vec!(Some("12345".to_string()), Some("123  ".to_string()), None),
               res.map(|row| row.get(0u)).collect::<Vec<_>>());
}

#[test]
fn test_citext_params() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    or_panic!(conn.execute("CREATE TEMPORARY TABLE foo (
                            id SERIAL PRIMARY KEY,
                            b CITEXT
                           )", &[]));
    or_panic!(conn.execute("INSERT INTO foo (b) VALUES ($1), ($2), ($3)",
                           &[&Some("foobar"), &Some("FooBar"), &None::<&'static str>]));
    let stmt = or_panic!(conn.prepare("SELECT id FROM foo WHERE b = 'FOOBAR' ORDER BY id"));
    let res = or_panic!(stmt.query(&[]));

    assert_eq!(vec!(Some(1i32), Some(2i32)), res.map(|row| row.get(0u)).collect::<Vec<_>>());
}

#[test]
fn test_bytea_params() {
    test_type("BYTEA", &[(Some(vec!(0u8, 1, 2, 3, 254, 255)), "'\\x00010203feff'"),
                        (None, "NULL")]);
}

#[test]
fn test_json_params() {
    test_type("JSON", &[(Some(json::from_str("[10, 11, 12]").unwrap()),
                        "'[10, 11, 12]'"),
                       (Some(json::from_str("{\"f\": \"asd\"}").unwrap()),
                        "'{\"f\": \"asd\"}'"),
                       (None, "NULL")])
}

#[test]
fn test_inet_params() {
    test_type("INET", &[(Some(from_str::<IpAddr>("127.0.0.1").unwrap()),
                         "'127.0.0.1'"),
                        (Some(from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap()),
                         "'2001:0db8:85a3:0000:0000:8a2e:0370:7334'"),
                        (None, "NULL")])
}

#[test]
fn test_cidr_params() {
    test_type("CIDR", &[(Some(from_str::<IpAddr>("127.0.0.1").unwrap()),
                         "'127.0.0.1'"),
                        (Some(from_str("2001:0db8:85a3:0000:0000:8a2e:0370:7334").unwrap()),
                         "'2001:0db8:85a3:0000:0000:8a2e:0370:7334'"),
                        (None, "NULL")])
}

#[test]
fn test_int4range_params() {
    test_range!("INT4RANGE", i32, 100i32, "100", 200i32, "200")
}

#[test]
fn test_int8range_params() {
    test_range!("INT8RANGE", i64, 100i64, "100", 200i64, "200")
}

#[test]
fn test_boolarray_params() {
    test_array_params!("BOOL", false, "f", true, "t", true, "t");
}

#[test]
fn test_byteaarray_params() {
    test_array_params!("BYTEA", vec!(0u8, 1), r#""\\x0001""#, vec!(254u8, 255u8),
                       r#""\\xfeff""#, vec!(10u8, 11u8), r#""\\x0a0b""#);
}

#[test]
fn test_chararray_params() {
    test_array_params!("\"char\"", 'a' as i8, "a", 'z' as i8, "z",
                       '0' as i8, "0");
}

#[test]
fn test_namearray_params() {
    test_array_params!("NAME", "hello".to_string(), "hello", "world".to_string(),
                       "world", "!".to_string(), "!");
}

#[test]
fn test_int2array_params() {
    test_array_params!("INT2", 0i16, "0", 1i16, "1", 2i16, "2");
}

#[test]
fn test_int4array_params() {
    test_array_params!("INT4", 0i32, "0", 1i32, "1", 2i32, "2");
}

#[test]
fn test_textarray_params() {
    test_array_params!("TEXT", "hello".to_string(), "hello", "world".to_string(),
                       "world", "!".to_string(), "!");
}

#[test]
fn test_charnarray_params() {
    test_array_params!("CHAR(5)", "hello".to_string(), "hello",
                       "world".to_string(), "world", "!    ".to_string(), "!");
}

#[test]
fn test_varchararray_params() {
    test_array_params!("VARCHAR", "hello".to_string(), "hello",
                       "world".to_string(), "world", "!".to_string(), "!");
}

#[test]
fn test_int8array_params() {
    test_array_params!("INT8", 0i64, "0", 1i64, "1", 2i64, "2");
}

#[test]
fn test_float4array_params() {
    test_array_params!("FLOAT4", 0f32, "0", 1.5f32, "1.5", 0.009f32, ".009");
}

#[test]
fn test_float8array_params() {
    test_array_params!("FLOAT8", 0f64, "0", 1.5f64, "1.5", 0.009f64, ".009");
}

#[test]
fn test_int4rangearray_params() {
    test_array_params!("INT4RANGE",
                       range!('(', ')'), "\"(,)\"",
                       range!('[' 10i32, ')'), "\"[10,)\"",
                       range!('(', 10i32 ')'), "\"(,10)\"");
}

#[test]
fn test_int8rangearray_params() {
    test_array_params!("INT8RANGE",
                       range!('(', ')'), "\"(,)\"",
                       range!('[' 10i64, ')'), "\"[10,)\"",
                       range!('(', 10i64 ')'), "\"(,10)\"");
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
              &[(Some(make_map!("a".to_string() => Some("1".to_string()))), "'a=>1'"),
               (Some(make_map!("hello".to_string() => Some("world!".to_string()),
                               "hola".to_string() => Some("mundo!".to_string()),
                               "what".to_string() => None)),
                "'hello=>world!,hola=>mundo!,what=>NULL'"),
                (None, "NULL")]);
}

fn test_nan_param<T: Float+ToSql+FromSql>(sql_type: &str) {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = or_panic!(conn.prepare(format!("SELECT 'NaN'::{}", sql_type)[]));
    let mut result = or_panic!(stmt.query(&[]));
    let val: T = result.next().unwrap().get(0u);
    assert!(val.is_nan());

    let nan: T = Float::nan();
    let stmt = or_panic!(conn.prepare(format!("SELECT $1::{}", sql_type)[]));
    let mut result = or_panic!(stmt.query(&[&nan]));
    let val: T = result.next().unwrap().get(0u);
    assert!(val.is_nan())
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
fn test_jsonarray_params() {
    test_array_params!("JSON",
                       json::from_str("[10, 11, 12]").unwrap(),
                       "\"[10,11,12]\"",
                       json::from_str(r#"{"a": 10, "b": null}"#).unwrap(),
                       r#""{\"a\": 10, \"b\": null}""#,
                       json::from_str(r#"{"a": [10], "b": true}"#).unwrap(),
                       r#""{\"a\": [10], \"b\": true}""#);
}

#[test]
fn test_pg_database_datname() {
    let conn = or_panic!(Connection::connect("postgres://postgres@localhost", &SslMode::None));
    let stmt = or_panic!(conn.prepare("SELECT datname FROM pg_database"));
    let mut result = or_panic!(stmt.query(&[]));

    let next = result.next().unwrap();
    or_panic!(next.get_opt::<uint, String>(0));
    or_panic!(next.get_opt::<&str, String>("datname"));
}

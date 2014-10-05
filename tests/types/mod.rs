use serialize::json;
use std::collections::HashMap;
use std::f32;
use std::f64;
use time;
use time::Timespec;

use postgres::{PostgresConnection, NoSsl};
use postgres::types::array::ArrayBase;
use postgres::types::{ToSql, FromSql};

mod array;
mod range;

fn test_type<T: PartialEq+FromSql+ToSql, S: Str>(sql_type: &str, checks: &[(T, S)]) {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    for &(ref val, ref repr) in checks.iter() {
        let stmt = or_fail!(conn.prepare(format!("SELECT {:s}::{}", *repr, sql_type)[]));
        let result = or_fail!(stmt.query([])).next().unwrap().get(0u);
        assert!(val == &result);

        let stmt = or_fail!(conn.prepare(format!("SELECT $1::{}", sql_type)[]));
        let result = or_fail!(stmt.query(&[val])).next().unwrap().get(0u);
        assert!(val == &result);
    }
}

#[test]
fn test_bool_params() {
    test_type("BOOL", [(Some(true), "'t'"), (Some(false), "'f'"),
                       (None, "NULL")]);
}

#[test]
fn test_i8_params() {
    test_type("\"char\"", [(Some('a' as i8), "'a'"), (None, "NULL")]);
}

#[test]
fn test_name_params() {
    test_type("NAME", [(Some("hello world".to_string()), "'hello world'"),
                       (Some("イロハニホヘト チリヌルヲ".to_string()), "'イロハニホヘト チリヌルヲ'"),
                       (None, "NULL")]);
}

#[test]
fn test_i16_params() {
    test_type("SMALLINT", [(Some(15001i16), "15001"),
                           (Some(-15001i16), "-15001"), (None, "NULL")]);
}

#[test]
fn test_i32_params() {
    test_type("INT", [(Some(2147483548i32), "2147483548"),
                      (Some(-2147483548i32), "-2147483548"), (None, "NULL")]);
}

#[test]
fn test_i64_params() {
    test_type("BIGINT", [(Some(9223372036854775708i64), "9223372036854775708"),
                         (Some(-9223372036854775708i64), "-9223372036854775708"),
                         (None, "NULL")]);
}

#[test]
fn test_f32_params() {
    test_type("REAL", [(Some(f32::INFINITY), "'infinity'"),
                       (Some(f32::NEG_INFINITY), "'-infinity'"),
                       (Some(1000.55), "1000.55"), (None, "NULL")]);
}

#[test]
fn test_f64_params() {
    test_type("DOUBLE PRECISION", [(Some(f64::INFINITY), "'infinity'"),
                                   (Some(f64::NEG_INFINITY), "'-infinity'"),
                                   (Some(10000.55), "10000.55"),
                                   (None, "NULL")]);
}

#[test]
fn test_varchar_params() {
    test_type("VARCHAR", [(Some("hello world".to_string()), "'hello world'"),
                          (Some("イロハニホヘト チリヌルヲ".to_string()), "'イロハニホヘト チリヌルヲ'"),
                          (None, "NULL")]);
}

#[test]
fn test_text_params() {
    test_type("TEXT", [(Some("hello world".to_string()), "'hello world'"),
                       (Some("イロハニホヘト チリヌルヲ".to_string()), "'イロハニホヘト チリヌルヲ'"),
                       (None, "NULL")]);
}

#[test]
fn test_bpchar_params() {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    or_fail!(conn.execute("CREATE TEMPORARY TABLE foo (
                            id SERIAL PRIMARY KEY,
                            b CHAR(5)
                           )", []));
    or_fail!(conn.execute("INSERT INTO foo (b) VALUES ($1), ($2), ($3)",
                          &[&Some("12345"), &Some("123"), &None::<&'static str>]));
    let stmt = or_fail!(conn.prepare("SELECT b FROM foo ORDER BY id"));
    let res = or_fail!(stmt.query([]));

    assert_eq!(vec!(Some("12345".to_string()), Some("123  ".to_string()), None),
               res.map(|row| row.get(0u)).collect());
}

#[test]
fn test_bytea_params() {
    test_type("BYTEA", [(Some(vec!(0u8, 1, 2, 3, 254, 255)), "'\\x00010203feff'"),
                        (None, "NULL")]);
}

#[test]
fn test_json_params() {
    test_type("JSON", [(Some(json::from_str("[10, 11, 12]").unwrap()),
                        "'[10, 11, 12]'"),
                       (Some(json::from_str("{\"f\": \"asd\"}").unwrap()),
                        "'{\"f\": \"asd\"}'"),
                       (None, "NULL")])
}

#[test]
fn test_tm_params() {
    fn make_check<'a>(time: &'a str) -> (Option<Timespec>, &'a str) {
        (Some(time::strptime(time, "'%Y-%m-%d %H:%M:%S.%f'").unwrap().to_timespec()), time)
    }
    test_type("TIMESTAMP",
              [make_check("'1970-01-01 00:00:00.01'"),
               make_check("'1965-09-25 11:19:33.100314'"),
               make_check("'2010-02-09 23:11:45.1202'"),
               (None, "NULL")]);
    test_type("TIMESTAMP WITH TIME ZONE",
              [make_check("'1970-01-01 00:00:00.01'"),
               make_check("'1965-09-25 11:19:33.100314'"),
               make_check("'2010-02-09 23:11:45.1202'"),
               (None, "NULL")]);
}

macro_rules! test_range(
    ($name:expr, $t:ty, $low:expr, $low_str:expr, $high:expr, $high_str:expr) => ({
        let tests = [(Some(range!('(', ')')), "'(,)'".to_string()),
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
)

#[test]
fn test_int4range_params() {
    test_range!("INT4RANGE", i32, 100i32, "100", 200i32, "200")
}

#[test]
fn test_int8range_params() {
    test_range!("INT8RANGE", i64, 100i64, "100", 200i64, "200")
}

fn test_timespec_range_params(sql_type: &str) {
    fn t(time: &str) -> Timespec {
        time::strptime(time, "%Y-%m-%d").unwrap().to_timespec()
    }
    let low = "1970-01-01";
    let high = "1980-01-01";
    test_range!(sql_type, Timespec, t(low), low, t(high), high);
}

#[test]
fn test_tsrange_params() {
    test_timespec_range_params("TSRANGE");
}

#[test]
fn test_tstzrange_params() {
    test_timespec_range_params("TSTZRANGE");
}

macro_rules! test_array_params(
    ($name:expr, $v1:expr, $s1:expr, $v2:expr, $s2:expr, $v3:expr, $s3:expr) => ({
        let tests = [(Some(ArrayBase::from_vec(vec!(Some($v1), Some($v2), None), 1)),
                      format!("'{{{},{},NULL}}'", $s1, $s2).into_string()),
                     (None, "NULL".to_string())];
        test_type(format!("{}[]", $name)[], tests);
        let mut a = ArrayBase::from_vec(vec!(Some($v1), Some($v2)), 0);
        a.wrap(-1);
        a.push_move(ArrayBase::from_vec(vec!(None, Some($v3)), 0));
        let tests = [(Some(a), format!("'[-1:0][0:1]={{{{{},{}}},{{NULL,{}}}}}'",
                                       $s1, $s2, $s3).into_string())];
        test_type(format!("{}[][]", $name)[], tests);
    })
)

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
fn test_timestamparray_params() {
    fn make_check<'a>(time: &'a str) -> (Timespec, &'a str) {
        (time::strptime(time, "%Y-%m-%d %H:%M:%S.%f").unwrap().to_timespec(), time)
    }
    let (v1, s1) = make_check("1970-01-01 00:00:00.01");
    let (v2, s2) = make_check("1965-09-25 11:19:33.100314");
    let (v3, s3) = make_check("2010-02-09 23:11:45.1202");
    test_array_params!("TIMESTAMP", v1, s1, v2, s2, v3, s3);
    test_array_params!("TIMESTAMPTZ", v1, s1, v2, s2, v3, s3);
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
fn test_tsrangearray_params() {
    fn make_check<'a>(time: &'a str) -> (Timespec, &'a str) {
        (time::strptime(time, "%Y-%m-%d").unwrap().to_timespec(), time)
    }
    let (v1, s1) = make_check("1970-10-11");
    let (v2, s2) = make_check("1990-01-01");
    let r1 = range!('(', ')');
    let rs1 = "\"(,)\"";
    let r2 = range!('[' v1, ')');
    let rs2 = format!("\"[{},)\"", s1);
    let r3 = range!('(', v2 ')');
    let rs3 = format!("\"(,{})\"", s2);
    test_array_params!("TSRANGE", r1, rs1, r2, rs2, r3, rs3);
    test_array_params!("TSTZRANGE", r1, rs1, r2, rs2, r3, rs3);
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
    macro_rules! make_map(
        ($($k:expr => $v:expr),+) => ({
            let mut map = HashMap::new();
            $(map.insert($k, $v);)+
            map
        })
    )
    test_type("hstore",
              [(Some(make_map!("a".to_string() => Some("1".to_string()))), "'a=>1'"),
               (Some(make_map!("hello".to_string() => Some("world!".to_string()),
                               "hola".to_string() => Some("mundo!".to_string()),
                               "what".to_string() => None)),
                "'hello=>world!,hola=>mundo!,what=>NULL'"),
                (None, "NULL")]);
}

fn test_nan_param<T: Float+ToSql+FromSql>(sql_type: &str) {
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare(format!("SELECT 'NaN'::{}", sql_type)[]));
    let mut result = or_fail!(stmt.query([]));
    let val: T = result.next().unwrap().get(0u);
    assert!(val.is_nan());

    let nan: T = Float::nan();
    let stmt = or_fail!(conn.prepare(format!("SELECT $1::{}", sql_type)[]));
    let mut result = or_fail!(stmt.query(&[&nan]));
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
    let conn = or_fail!(PostgresConnection::connect("postgres://postgres@localhost", &NoSsl));
    let stmt = or_fail!(conn.prepare("SELECT datname FROM pg_database"));
    let mut result = or_fail!(stmt.query([]));

    let next = result.next().unwrap();
    or_fail!(next.get_opt::<uint, String>(0));
    or_fail!(next.get_opt::<&str, String>("datname"));
}

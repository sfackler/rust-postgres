extern crate time;

use self::time::Timespec;
use types::test_type;

#[test]
fn test_tm_params() {
    fn make_check<'a>(time: &'a str) -> (Option<Timespec>, &'a str) {
        (Some(time::strptime(time, "'%Y-%m-%d %H:%M:%S.%f'").unwrap().to_timespec()), time)
    }
    test_type("TIMESTAMP",
              &[make_check("'1970-01-01 00:00:00.01'"),
               make_check("'1965-09-25 11:19:33.100314'"),
               make_check("'2010-02-09 23:11:45.1202'"),
               (None, "NULL")]);
    test_type("TIMESTAMP WITH TIME ZONE",
              &[make_check("'1970-01-01 00:00:00.01'"),
               make_check("'1965-09-25 11:19:33.100314'"),
               make_check("'2010-02-09 23:11:45.1202'"),
               (None, "NULL")]);
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


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

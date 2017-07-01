extern crate chrono;

use self::chrono::{TimeZone, NaiveDate, NaiveTime, NaiveDateTime, DateTime, Utc};
use types::test_type;

use postgres::types::{Date, Timestamp};

#[test]
fn test_naive_date_time_params() {
    fn make_check<'a>(time: &'a str) -> (Option<NaiveDateTime>, &'a str) {
        (
            Some(
                NaiveDateTime::parse_from_str(time, "'%Y-%m-%d %H:%M:%S.%f'").unwrap(),
            ),
            time,
        )
    }
    test_type(
        "TIMESTAMP",
        &[
            make_check("'1970-01-01 00:00:00.010000000'"),
            make_check("'1965-09-25 11:19:33.100314000'"),
            make_check("'2010-02-09 23:11:45.120200000'"),
            (None, "NULL"),
        ],
    );
}

#[test]
fn test_with_special_naive_date_time_params() {
    fn make_check<'a>(time: &'a str) -> (Timestamp<NaiveDateTime>, &'a str) {
        (
            Timestamp::Value(
                NaiveDateTime::parse_from_str(time, "'%Y-%m-%d %H:%M:%S.%f'").unwrap(),
            ),
            time,
        )
    }
    test_type(
        "TIMESTAMP",
        &[
            make_check("'1970-01-01 00:00:00.010000000'"),
            make_check("'1965-09-25 11:19:33.100314000'"),
            make_check("'2010-02-09 23:11:45.120200000'"),
            (Timestamp::PosInfinity, "'infinity'"),
            (Timestamp::NegInfinity, "'-infinity'"),
        ],
    );
}

#[test]
fn test_date_time_params() {
    fn make_check<'a>(time: &'a str) -> (Option<DateTime<Utc>>, &'a str) {
        (
            Some(
                Utc.datetime_from_str(time, "'%Y-%m-%d %H:%M:%S.%f'")
                    .unwrap(),
            ),
            time,
        )
    }
    test_type(
        "TIMESTAMP WITH TIME ZONE",
        &[
            make_check("'1970-01-01 00:00:00.010000000'"),
            make_check("'1965-09-25 11:19:33.100314000'"),
            make_check("'2010-02-09 23:11:45.120200000'"),
            (None, "NULL"),
        ],
    );
}

#[test]
fn test_with_special_date_time_params() {
    fn make_check<'a>(time: &'a str) -> (Timestamp<DateTime<Utc>>, &'a str) {
        (
            Timestamp::Value(
                Utc.datetime_from_str(time, "'%Y-%m-%d %H:%M:%S.%f'")
                    .unwrap(),
            ),
            time,
        )
    }
    test_type(
        "TIMESTAMP WITH TIME ZONE",
        &[
            make_check("'1970-01-01 00:00:00.010000000'"),
            make_check("'1965-09-25 11:19:33.100314000'"),
            make_check("'2010-02-09 23:11:45.120200000'"),
            (Timestamp::PosInfinity, "'infinity'"),
            (Timestamp::NegInfinity, "'-infinity'"),
        ],
    );
}

#[test]
fn test_date_params() {
    fn make_check<'a>(time: &'a str) -> (Option<NaiveDate>, &'a str) {
        (
            Some(NaiveDate::parse_from_str(time, "'%Y-%m-%d'").unwrap()),
            time,
        )
    }
    test_type(
        "DATE",
        &[
            make_check("'1970-01-01'"),
            make_check("'1965-09-25'"),
            make_check("'2010-02-09'"),
            (None, "NULL"),
        ],
    );
}

#[test]
fn test_with_special_date_params() {
    fn make_check<'a>(date: &'a str) -> (Date<NaiveDate>, &'a str) {
        (
            Date::Value(NaiveDate::parse_from_str(date, "'%Y-%m-%d'").unwrap()),
            date,
        )
    }
    test_type(
        "DATE",
        &[
            make_check("'1970-01-01'"),
            make_check("'1965-09-25'"),
            make_check("'2010-02-09'"),
            (Date::PosInfinity, "'infinity'"),
            (Date::NegInfinity, "'-infinity'"),
        ],
    );
}

#[test]
fn test_time_params() {
    fn make_check<'a>(time: &'a str) -> (Option<NaiveTime>, &'a str) {
        (
            Some(NaiveTime::parse_from_str(time, "'%H:%M:%S.%f'").unwrap()),
            time,
        )
    }
    test_type(
        "TIME",
        &[
            make_check("'00:00:00.010000000'"),
            make_check("'11:19:33.100314000'"),
            make_check("'23:11:45.120200000'"),
            (None, "NULL"),
        ],
    );
}

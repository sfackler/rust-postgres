extern crate time;

use self::time::Timespec;
use types::test_type;

use postgres::types::Timestamp;

#[test]
fn test_tm_params() {
    fn make_check<'a>(time: &'a str) -> (Option<Timespec>, &'a str) {
        (
            Some(
                time::strptime(time, "'%Y-%m-%d %H:%M:%S.%f'")
                    .unwrap()
                    .to_timespec(),
            ),
            time,
        )
    }
    test_type(
        "TIMESTAMP",
        &[
            make_check("'1970-01-01 00:00:00.01'"),
            make_check("'1965-09-25 11:19:33.100314'"),
            make_check("'2010-02-09 23:11:45.1202'"),
            (None, "NULL"),
        ],
    );
    test_type(
        "TIMESTAMP WITH TIME ZONE",
        &[
            make_check("'1970-01-01 00:00:00.01'"),
            make_check("'1965-09-25 11:19:33.100314'"),
            make_check("'2010-02-09 23:11:45.1202'"),
            (None, "NULL"),
        ],
    );
}

#[test]
fn test_with_special_tm_params() {
    fn make_check<'a>(time: &'a str) -> (Timestamp<Timespec>, &'a str) {
        (
            Timestamp::Value(
                time::strptime(time, "'%Y-%m-%d %H:%M:%S.%f'")
                    .unwrap()
                    .to_timespec(),
            ),
            time,
        )
    }
    test_type(
        "TIMESTAMP",
        &[
            make_check("'1970-01-01 00:00:00.01'"),
            make_check("'1965-09-25 11:19:33.100314'"),
            make_check("'2010-02-09 23:11:45.1202'"),
            (Timestamp::PosInfinity, "'infinity'"),
            (Timestamp::NegInfinity, "'-infinity'"),
        ],
    );
    test_type(
        "TIMESTAMP WITH TIME ZONE",
        &[
            make_check("'1970-01-01 00:00:00.01'"),
            make_check("'1965-09-25 11:19:33.100314'"),
            make_check("'2010-02-09 23:11:45.1202'"),
            (Timestamp::PosInfinity, "'infinity'"),
            (Timestamp::NegInfinity, "'-infinity'"),
        ],
    );
}

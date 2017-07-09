extern crate time;

use self::time::Timespec;
use std::error::Error;
use postgres_protocol::types;

use types::{Type, FromSql, ToSql, IsNull, TIMESTAMP, TIMESTAMPTZ};

const USEC_PER_SEC: i64 = 1_000_000;
const NSEC_PER_USEC: i64 = 1_000;

// Number of seconds from 1970-01-01 to 2000-01-01
const TIME_SEC_CONVERSION: i64 = 946684800;

impl FromSql for Timespec {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Timespec, Box<Error + Sync + Send>> {
        let t = types::timestamp_from_sql(raw)?;
        let mut sec = t / USEC_PER_SEC + TIME_SEC_CONVERSION;
        let mut usec = t % USEC_PER_SEC;

        if usec < 0 {
            sec -= 1;
            usec = USEC_PER_SEC + usec;
        }

        Ok(Timespec::new(sec, (usec * NSEC_PER_USEC) as i32))
    }

    accepts!(TIMESTAMP, TIMESTAMPTZ);
}

impl ToSql for Timespec {
    fn to_sql(&self, _: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        let t = (self.sec - TIME_SEC_CONVERSION) * USEC_PER_SEC + self.nsec as i64 / NSEC_PER_USEC;
        types::timestamp_to_sql(t, w);
        Ok(IsNull::No)
    }

    accepts!(TIMESTAMP, TIMESTAMPTZ);
    to_sql_checked!();
}

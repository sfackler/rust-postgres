use time::Timespec;
use Result;
use types::{RawFromSql, Type, RawToSql};

const USEC_PER_SEC: i64 = 1_000_000;
const NSEC_PER_USEC: i64 = 1_000;

// Number of seconds from 1970-01-01 to 2000-01-01
const TIME_SEC_CONVERSION: i64 = 946684800;

impl RawFromSql for Timespec {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<Timespec> {
        let t = try!(raw.read_be_i64());
        let mut sec = t / USEC_PER_SEC + TIME_SEC_CONVERSION;
        let mut usec = t % USEC_PER_SEC;

        if usec < 0 {
            sec -= 1;
            usec = USEC_PER_SEC + usec;
        }

        Ok(Timespec::new(sec, (usec * NSEC_PER_USEC) as i32))
    }
}

from_raw_from_impl!(Type::Timestamp | Type::TimestampTZ, Timespec);

impl RawToSql for Timespec {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()> {
        let t = (self.sec - TIME_SEC_CONVERSION) * USEC_PER_SEC + self.nsec as i64 / NSEC_PER_USEC;
        Ok(try!(w.write_be_i64(t)))
    }
}

to_raw_to_impl!(Type::Timestamp | Type::TimestampTZ, Timespec);


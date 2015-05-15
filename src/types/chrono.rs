extern crate chrono;

use std::io::prelude::*;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use self::chrono::{Duration, NaiveDate, NaiveTime, NaiveDateTime, DateTime, UTC};

use Result;
use types::{FromSql, ToSql, IsNull, Type};

const USEC_PER_SEC: i64 = 1_000_000;
const NSEC_PER_USEC: i64 = 1_000;

// Number of seconds from 1970-01-01 to 2000-01-01
const TIME_SEC_CONVERSION: i64 = 946684800;

fn base() -> NaiveDateTime {
    NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0)
}

impl FromSql for NaiveDateTime {
    fn from_sql<R: Read>(_: &Type, raw: &mut R) -> Result<NaiveDateTime> {
        // FIXME should be this, blocked on lifthrasiir/rust-chrono#37
        /*
        let t = try!(raw.read_i64::<BigEndian>());
        Ok(base() + Duration::microseconds(t))
        */

        let t = try!(raw.read_i64::<BigEndian>());
        let mut sec = t / USEC_PER_SEC + TIME_SEC_CONVERSION;
        let mut usec = t % USEC_PER_SEC;

        if usec < 0 {
            sec -= 1;
            usec = USEC_PER_SEC + usec;
        }

        Ok(NaiveDateTime::from_timestamp(sec, (usec * NSEC_PER_USEC) as u32))
    }

    accepts!(Type::Timestamp);
}

impl ToSql for NaiveDateTime {
    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W) -> Result<IsNull> {
        let t = (*self - base()).num_microseconds().unwrap();
        try!(w.write_i64::<BigEndian>(t));
        Ok(IsNull::No)
    }

    accepts!(Type::Timestamp);
    to_sql_checked!();
}

impl FromSql for DateTime<UTC> {
    fn from_sql<R: Read>(type_: &Type, raw: &mut R) -> Result<DateTime<UTC>> {
        let naive = try!(NaiveDateTime::from_sql(type_, raw));
        Ok(DateTime::from_utc(naive, UTC))
    }

    accepts!(Type::TimestampTZ);
}

impl ToSql for DateTime<UTC> {
    fn to_sql<W: Write+?Sized>(&self, type_: &Type, mut w: &mut W) -> Result<IsNull> {
        self.naive_utc().to_sql(type_, w)
    }

    accepts!(Type::TimestampTZ);
    to_sql_checked!();
}

impl FromSql for NaiveDate {
    fn from_sql<R: Read>(_: &Type, raw: &mut R) -> Result<NaiveDate> {
        let jd = try!(raw.read_i32::<BigEndian>());
        Ok(base().date() + Duration::days(jd as i64))
    }

    accepts!(Type::Date);
}

impl ToSql for NaiveDate {
    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W) -> Result<IsNull> {
        let jd = *self - base().date();
        try!(w.write_i32::<BigEndian>(jd.num_days() as i32));
        Ok(IsNull::No)
    }

    accepts!(Type::Date);
    to_sql_checked!();
}

impl FromSql for NaiveTime {
    fn from_sql<R: Read>(_: &Type, raw: &mut R) -> Result<NaiveTime> {
        let usec = try!(raw.read_i64::<BigEndian>());
        Ok(NaiveTime::from_hms(0, 0, 0) + Duration::microseconds(usec))
    }

    accepts!(Type::Time);
}

impl ToSql for NaiveTime {
    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W) -> Result<IsNull> {
        let delta = *self - NaiveTime::from_hms(0, 0, 0);
        try!(w.write_i64::<BigEndian>(delta.num_microseconds().unwrap()));
        Ok(IsNull::No)
    }

    accepts!(Type::Time);
    to_sql_checked!();
}

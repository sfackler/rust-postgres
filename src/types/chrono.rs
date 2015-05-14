extern crate chrono;

use std::io::prelude::*;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use self::chrono::{Timelike, Datelike, NaiveDate, NaiveTime, NaiveDateTime, DateTime, UTC};

use Result;
use types::{FromSql, ToSql, IsNull, Type};

const USEC_PER_SEC: i64 = 1_000_000;
const NSEC_PER_USEC: i64 = 1_000;

// Number of seconds from 1970-01-01 to 2000-01-01
const TIME_SEC_CONVERSION: i64 = 946684800;

const POSTGRES_EPOCH_JDATE: i32 = 2451545;

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
    // adapted from j2date in src/interfaces/ecpg/pgtypeslib/dt_common.c
    fn from_sql<R: Read>(_: &Type, raw: &mut R) -> Result<NaiveDate> {
        let jd = try!(raw.read_i32::<BigEndian>());

        let mut julian: u32 = (jd + POSTGRES_EPOCH_JDATE) as u32;
        julian += 32044;
        let mut quad: u32 = julian / 146097;
        let extra: u32 = (julian - quad * 146097) * 4 + 3;
        julian += 60 + quad * 3 + extra / 146097;
        quad = julian / 1461;
        julian -= quad * 1461;
        let mut y: i32 = (julian * 4 / 1461) as i32;
        julian = if y != 0 { (julian + 305) % 365 } else { (julian + 306) % 366 } + 123;
        y += (quad * 4) as i32;
        let year = y - 4800;
        quad = julian * 2141 / 65536;
        let day = julian - 7834 * quad / 256;
        let month = (quad + 10) % 12 + 1;

        Ok(NaiveDate::from_ymd(year, month as u32, day as u32))
    }

    accepts!(Type::Date);
}

impl ToSql for NaiveDate {
    // adapted from date2j in src/interfaces/ecpg/pgtypeslib/dt_common.c
    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W) -> Result<IsNull> {
        let mut y = self.year();
        let mut m = self.month() as i32;
        let d = self.day() as i32;

        if m > 2 {
            m += 1;
            y += 4800;
        } else {
            m += 13;
            y += 4799;
        }

        let century = y / 100;
        let mut julian = y * 365 - 32167;
        julian += y / 4 - century + century / 4;
        julian += 7834 * m / 256 + d;

        try!(w.write_i32::<BigEndian>(julian - POSTGRES_EPOCH_JDATE));
        Ok(IsNull::No)
    }

    accepts!(Type::Date);
    to_sql_checked!();
}

impl FromSql for NaiveTime {
    fn from_sql<R: Read>(_: &Type, raw: &mut R) -> Result<NaiveTime> {
        let mut usec = try!(raw.read_i64::<BigEndian>());
        let mut sec = usec / USEC_PER_SEC;
        usec -= sec * USEC_PER_SEC;
        let mut min = sec / 60;
        sec -= min * 60;
        let hr = min / 60;
        min -= hr * 60;

        Ok(NaiveTime::from_hms_micro(hr as u32, min as u32, sec as u32, usec as u32))
    }

    accepts!(Type::Time);
}

impl ToSql for NaiveTime {
    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W) -> Result<IsNull> {
        let hr = self.hour() as i64;
        let min = hr * 60 + self.minute() as i64;
        let sec = min * 60 + self.second() as i64;
        let usec = sec * USEC_PER_SEC + self.nanosecond() as i64 / NSEC_PER_USEC;

        try!(w.write_i64::<BigEndian>(usec));
        Ok(IsNull::No)
    }

    accepts!(Type::Time);
    to_sql_checked!();
}

extern crate chrono;

use std::error;
use std::io::prelude::*;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use self::chrono::{Duration, NaiveDate, NaiveTime, NaiveDateTime, DateTime, UTC};

use Result;
use error::Error;
use types::{FromSql, ToSql, IsNull, Type, SessionInfo};

fn base() -> NaiveDateTime {
    NaiveDate::from_ymd(2000, 1, 1).and_hms(0, 0, 0)
}

impl FromSql for NaiveDateTime {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo) -> Result<NaiveDateTime> {
        let t = try!(raw.read_i64::<BigEndian>());
        Ok(base() + Duration::microseconds(t))
    }

    accepts!(Type::Timestamp);
}

impl ToSql for NaiveDateTime {
    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W, _: &SessionInfo) -> Result<IsNull> {
        let t = (*self - base()).num_microseconds().unwrap();
        try!(w.write_i64::<BigEndian>(t));
        Ok(IsNull::No)
    }

    accepts!(Type::Timestamp);
    to_sql_checked!();
}

impl FromSql for DateTime<UTC> {
    fn from_sql<R: Read>(type_: &Type, raw: &mut R, info: &SessionInfo) -> Result<DateTime<UTC>> {
        let naive = try!(NaiveDateTime::from_sql(type_, raw, info));
        Ok(DateTime::from_utc(naive, UTC))
    }

    accepts!(Type::TimestampTZ);
}

impl ToSql for DateTime<UTC> {
    fn to_sql<W: Write+?Sized>(&self, type_: &Type, mut w: &mut W, info: &SessionInfo)
                               -> Result<IsNull> {
        self.naive_utc().to_sql(type_, w, info)
    }

    accepts!(Type::TimestampTZ);
    to_sql_checked!();
}

impl FromSql for NaiveDate {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo) -> Result<NaiveDate> {
        let jd = try!(raw.read_i32::<BigEndian>());
        Ok(base().date() + Duration::days(jd as i64))
    }

    accepts!(Type::Date);
}

impl ToSql for NaiveDate {
    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W, _: &SessionInfo) -> Result<IsNull> {
        let jd = (*self - base().date()).num_days();
        if jd > i32::max_value() as i64 || jd < i32::min_value() as i64 {
            let err: Box<error::Error+Sync+Send> = "value too large to transmit".into();
            return Err(Error::Conversion(err));
        }

        try!(w.write_i32::<BigEndian>(jd as i32));
        Ok(IsNull::No)
    }

    accepts!(Type::Date);
    to_sql_checked!();
}

impl FromSql for NaiveTime {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo) -> Result<NaiveTime> {
        let usec = try!(raw.read_i64::<BigEndian>());
        Ok(NaiveTime::from_hms(0, 0, 0) + Duration::microseconds(usec))
    }

    accepts!(Type::Time);
}

impl ToSql for NaiveTime {
    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W, _: &SessionInfo) -> Result<IsNull> {
        let delta = *self - NaiveTime::from_hms(0, 0, 0);
        try!(w.write_i64::<BigEndian>(delta.num_microseconds().unwrap()));
        Ok(IsNull::No)
    }

    accepts!(Type::Time);
    to_sql_checked!();
}

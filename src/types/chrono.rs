extern crate chrono;

use std::io::prelude::*;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};
use self::chrono::{Duration, NaiveDate, NaiveTime, NaiveDateTime, DateTime, UTC, Local,
                   FixedOffset};

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
    fn to_sql<W: Write + ?Sized>(&self,
                                 _: &Type,
                                 mut w: &mut W,
                                 _: &SessionInfo)
                                 -> Result<IsNull> {
        let time = match (*self - base()).num_microseconds() {
            Some(time) => time,
            None => return Err(Error::Conversion("value too large to transmit".into())),
        };
        try!(w.write_i64::<BigEndian>(time));
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

    accepts!(Type::Timestamptz);
}

impl ToSql for DateTime<UTC> {
    fn to_sql<W: Write + ?Sized>(&self,
                                 type_: &Type,
                                 mut w: &mut W,
                                 info: &SessionInfo)
                                 -> Result<IsNull> {
        self.naive_utc().to_sql(type_, w, info)
    }

    accepts!(Type::Timestamptz);
    to_sql_checked!();
}

impl FromSql for DateTime<Local> {
    fn from_sql<R: Read>(type_: &Type, raw: &mut R, info: &SessionInfo) -> Result<DateTime<Local>> {
        let utc = try!(DateTime::<UTC>::from_sql(type_, raw, info));
        Ok(utc.with_timezone(&Local))
    }

    accepts!(Type::Timestamptz);
}

impl ToSql for DateTime<Local> {
    fn to_sql<W: Write + ?Sized>(&self,
                                 type_: &Type,
                                 mut w: &mut W,
                                 info: &SessionInfo)
                                 -> Result<IsNull> {
        self.with_timezone(&UTC).to_sql(type_, w, info)
    }

    accepts!(Type::Timestamptz);
    to_sql_checked!();
}

impl FromSql for DateTime<FixedOffset> {
    fn from_sql<R: Read>(type_: &Type,
                         raw: &mut R,
                         info: &SessionInfo)
                         -> Result<DateTime<FixedOffset>> {
        let utc = try!(DateTime::<UTC>::from_sql(type_, raw, info));
        Ok(utc.with_timezone(&FixedOffset::east(0)))
    }

    accepts!(Type::Timestamptz);
}

impl ToSql for DateTime<FixedOffset> {
    fn to_sql<W: Write + ?Sized>(&self,
                                 type_: &Type,
                                 mut w: &mut W,
                                 info: &SessionInfo)
                                 -> Result<IsNull> {
        self.with_timezone(&UTC).to_sql(type_, w, info)
    }

    accepts!(Type::Timestamptz);
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
    fn to_sql<W: Write + ?Sized>(&self,
                                 _: &Type,
                                 mut w: &mut W,
                                 _: &SessionInfo)
                                 -> Result<IsNull> {
        let jd = (*self - base().date()).num_days();
        if jd > i32::max_value() as i64 || jd < i32::min_value() as i64 {
            return Err(Error::Conversion("value too large to transmit".into()));
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
    fn to_sql<W: Write + ?Sized>(&self,
                                 _: &Type,
                                 mut w: &mut W,
                                 _: &SessionInfo)
                                 -> Result<IsNull> {
        let delta = *self - NaiveTime::from_hms(0, 0, 0);
        let time = match delta.num_microseconds() {
            Some(time) => time,
            None => return Err(Error::Conversion("value too large to transmit".into())),
        };
        try!(w.write_i64::<BigEndian>(time));
        Ok(IsNull::No)
    }

    accepts!(Type::Time);
    to_sql_checked!();
}

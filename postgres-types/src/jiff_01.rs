use bytes::BytesMut;
use jiff_01::{
    civil::{Date, DateTime, Time},
    Span, SpanRound, Timestamp, Unit,
};
use postgres_protocol::types;
use std::error::Error;

use crate::{FromSql, IsNull, ToSql, Type};

const fn base() -> DateTime {
    DateTime::constant(2000, 1, 1, 0, 0, 0, 0)
}

/// The number of seconds from the Unix epoch to 2000-01-01 00:00:00 UTC.
const PG_EPOCH: i64 = 946684800;

fn base_ts() -> Timestamp {
    Timestamp::new(PG_EPOCH, 0).unwrap()
}

fn round_us<'a>() -> SpanRound<'a> {
    SpanRound::new().largest(Unit::Microsecond)
}

impl<'a> FromSql<'a> for DateTime {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<DateTime, Box<dyn Error + Sync + Send>> {
        let t = types::timestamp_from_sql(raw)?;
        Ok(base().checked_add(Span::new().microseconds(t))?)
    }

    accepts!(TIMESTAMP);
}

impl ToSql for DateTime {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let span = self.since(base())?.round(round_us())?;
        types::timestamp_to_sql(span.get_microseconds(), w);
        Ok(IsNull::No)
    }

    accepts!(TIMESTAMP);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Timestamp {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Timestamp, Box<dyn Error + Sync + Send>> {
        let t = types::timestamp_from_sql(raw)?;
        Ok(base_ts().checked_add(Span::new().microseconds(t))?)
    }

    accepts!(TIMESTAMPTZ);
}

impl ToSql for Timestamp {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let span = self.since(base_ts())?.round(round_us())?;
        types::timestamp_to_sql(span.get_microseconds(), w);
        Ok(IsNull::No)
    }

    accepts!(TIMESTAMPTZ);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Date {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Date, Box<dyn Error + Sync + Send>> {
        let jd = types::date_from_sql(raw)?;
        Ok(base().date().checked_add(Span::new().days(jd))?)
    }

    accepts!(DATE);
}

impl ToSql for Date {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let jd = self.since(base().date())?.get_days();
        types::date_to_sql(jd, w);
        Ok(IsNull::No)
    }

    accepts!(DATE);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Time {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Time, Box<dyn Error + Sync + Send>> {
        let usec = types::time_from_sql(raw)?;
        Ok(Time::midnight() + Span::new().microseconds(usec))
    }

    accepts!(TIME);
}

impl ToSql for Time {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let span = self.since(Time::midnight())?.round(round_us())?;
        types::time_to_sql(span.get_microseconds(), w);
        Ok(IsNull::No)
    }

    accepts!(TIME);
    to_sql_checked!();
}

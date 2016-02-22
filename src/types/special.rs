use std::io::prelude::*;
use std::{i32, i64};

use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use Result;
use error::Error;
use types::{Type, FromSql, ToSql, IsNull, SessionInfo};

/// A wrapper that can be used to represent infinity with `Type::Date` types.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Date<T> {
    /// Represents `infinity`, a date that is later than all other dates.
    PosInfinity,
    /// Represents `-infinity`, a date that is earlier than all other dates.
    NegInfinity,
    /// The wrapped date.
    Value(T),
}

impl<T: FromSql> FromSql for Date<T> {
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, ctx: &SessionInfo) -> Result<Self> {
        if *ty != Type::Date {
            return Err(Error::Conversion("expected date type".into()));
        }

        let mut buf = [0; 4];
        try!(raw.read_exact(buf.as_mut()));

        match try!(buf.as_ref().read_i32::<BigEndian>()) {
            i32::MAX => Ok(Date::PosInfinity),
            i32::MIN => Ok(Date::NegInfinity),
            _ => T::from_sql(ty, &mut &mut buf.as_ref(), ctx).map(Date::Value),
        }
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::Date && T::accepts(ty)
    }
}
impl<T: ToSql> ToSql for Date<T> {
    fn to_sql<W: Write+?Sized>(&self, ty: &Type, out: &mut W, ctx: &SessionInfo) -> Result<IsNull> {
        if *ty != Type::Date {
            return Err(Error::Conversion("expected date type".into()));
        }

        let value = match *self {
            Date::PosInfinity => i32::MAX,
            Date::NegInfinity => i32::MIN,
            Date::Value(ref v) => return v.to_sql(ty, out, ctx),
        };

        try!(out.write_i32::<BigEndian>(value));
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        *ty == Type::Date && T::accepts(ty)
    }

    to_sql_checked!();
}

/// A wrapper that can be used to represent infinity with `Type::Timestamp` and `Type::Timestamptz`
/// types.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum Timestamp<T> {
    /// Represents `infinity`, a timestamp that is later than all other timestamps.
    PosInfinity,
    /// Represents `-infinity`, a timestamp that is earlier than all other timestamps.
    NegInfinity,
    /// The wrapped timestamp.
    Value(T),
}

impl<T: FromSql> FromSql for Timestamp<T> {
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, ctx: &SessionInfo) -> Result<Self> {
        if *ty != Type::Timestamp && *ty != Type::Timestamptz {
            return Err(Error::Conversion("expected timestamp or timestamptz type".into()));
        }

        let mut buf = [0; 8];
        try!(raw.read_exact(buf.as_mut()));

        match try!(buf.as_ref().read_i64::<BigEndian>()) {
            i64::MAX => Ok(Timestamp::PosInfinity),
            i64::MIN => Ok(Timestamp::NegInfinity),
            _ => T::from_sql(ty, &mut &mut buf.as_ref(), ctx).map(Timestamp::Value),
        }
    }

    fn accepts(ty: &Type) -> bool {
        (*ty == Type::Timestamp || *ty == Type::Timestamptz) && T::accepts(ty)
    }
}

impl<T: ToSql> ToSql for Timestamp<T> {
    fn to_sql<W: Write+?Sized>(&self, ty: &Type, out: &mut W, ctx: &SessionInfo) -> Result<IsNull> {
        if *ty != Type::Timestamp && *ty != Type::Timestamptz {
            return Err(Error::Conversion("expected timestamp or timestamptz type".into()));
        }

        let value = match *self {
            Timestamp::PosInfinity => i64::MAX,
            Timestamp::NegInfinity => i64::MIN,
            Timestamp::Value(ref v) => return v.to_sql(ty, out, ctx),
        };

        try!(out.write_i64::<BigEndian>(value));
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        (*ty == Type::Timestamp || *ty == Type::Timestamptz) && T::accepts(ty)
    }

    to_sql_checked!();
}

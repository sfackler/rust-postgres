//! Traits dealing with Postgres data types

extern mod extra;

use extra::time::Timespec;
use extra::json;
use extra::json::Json;
use extra::uuid::Uuid;
use std::rt::io::Decorator;
use std::rt::io::extensions::{WriterByteConversions, ReaderByteConversions};
use std::rt::io::mem::{MemWriter, BufReader};
use std::str;

/// A Postgres OID
pub type Oid = i32;

// Values from pg_type.h
static BOOLOID: Oid = 16;
static BYTEAOID: Oid = 17;
static CHAROID: Oid = 18;
static INT8OID: Oid = 20;
static INT2OID: Oid = 21;
static INT4OID: Oid = 23;
static TEXTOID: Oid = 25;
static JSONOID: Oid = 114;
static FLOAT4OID: Oid = 700;
static FLOAT8OID: Oid = 701;
static BPCHAROID: Oid = 1042;
static VARCHAROID: Oid = 1043;
static TIMESTAMPOID: Oid = 1114;
static TIMESTAMPZOID: Oid = 1184;
static UUIDOID: Oid = 2950;

static USEC_PER_SEC: i64 = 1_000_000;
static NSEC_PER_USEC: i64 = 1_000;

// Number of seconds from 1970-01-01 to 2000-01-01
static TIME_SEC_CONVERSION: i64 = 946684800;

/// A Postgres type
#[deriving(Eq)]
pub enum PostgresType {
    /// BOOL
    PgBool,
    /// BYTEA
    PgByteA,
    /// "char"
    PgChar,
    /// INT8/BIGINT
    PgInt8,
    /// INT2/SMALLINT
    PgInt2,
    /// INT4/INT
    PgInt4,
    /// TEXT
    PgText,
    /// JSON
    PgJson,
    /// FLOAT4/REAL
    PgFloat4,
    /// FLOAT8/DOUBLE PRECISION
    PgFloat8,
    /// TIMESTAMP
    PgTimestamp,
    /// TIMESTAMP WITH TIME ZONE
    PgTimestampZ,
    /// CHAR(n)/CHARACTER(n)
    PgCharN,
    /// VARCHAR/CHARACTER VARYING
    PgVarchar,
    /// UUID
    PgUuid,
    /// An unknown type along with its OID
    PgUnknownType(Oid)
}

impl PostgresType {
    /// Creates a PostgresType from a Postgres OID.
    pub fn from_oid(oid: Oid) -> PostgresType {
        match oid {
            BOOLOID => PgBool,
            BYTEAOID => PgByteA,
            CHAROID => PgChar,
            INT8OID => PgInt8,
            INT2OID => PgInt2,
            INT4OID => PgInt4,
            TEXTOID => PgText,
            JSONOID => PgJson,
            FLOAT4OID => PgFloat4,
            FLOAT8OID => PgFloat8,
            TIMESTAMPOID => PgTimestamp,
            TIMESTAMPZOID => PgTimestampZ,
            BPCHAROID => PgCharN,
            VARCHAROID => PgVarchar,
            UUIDOID => PgUuid,
            oid => PgUnknownType(oid)
        }
    }

    /// Returns the wire format needed for the value of `self`.
    pub fn result_format(&self) -> Format {
        match *self {
            PgUnknownType(*) => Text,
            _ => Binary
        }
    }
}

/// The wire format of a Postgres value
pub enum Format {
    /// A user-readable string format
    Text = 0,
    /// A machine-readable binary format
    Binary = 1
}

macro_rules! check_types(
    ($($expected:pat)|+, $actual:ident) => (
        match $actual {
            $($expected)|+ => (),
            actual => fail2!("Invalid Postgres type {:?}", actual)
        }
    )
)

/// A trait for types that can be created from a Postgres value
pub trait FromSql {
    /// Creates a new value of this type from a buffer of Postgres data.
    ///
    /// If the value was `NULL`, the buffer will be `None`.
    ///
    /// # Failure
    ///
    /// Fails if this type can not be created from the provided Postgres type.
    fn from_sql(ty: PostgresType, raw: &Option<~[u8]>) -> Self;
}

macro_rules! from_map_impl(
    ($($expected:pat)|+, $t:ty, $blk:expr) => (
        impl FromSql for Option<$t> {
            fn from_sql(ty: PostgresType, raw: &Option<~[u8]>) -> Option<$t> {
                check_types!($($expected)|+, ty)
                raw.as_ref().map($blk)
            }
        }
    )
)

macro_rules! from_conversions_impl(
    ($expected:pat, $t:ty, $f:ident) => (
        from_map_impl!($expected, $t, |buf| {
            let mut reader = BufReader::new(buf.as_slice());
            reader.$f()
        })
    )
)

macro_rules! from_option_impl(
    ($t:ty) => (
        impl FromSql for $t {
            fn from_sql(ty: PostgresType, raw: &Option<~[u8]>) -> $t {
                // FIXME when you can specify Self types properly
                let ret: Option<$t> = FromSql::from_sql(ty, raw);
                ret.unwrap()
            }
        }
    )
)

from_map_impl!(PgBool, bool, |buf| { buf[0] != 0 })
from_option_impl!(bool)

from_conversions_impl!(PgChar, i8, read_i8_)
from_option_impl!(i8)
from_conversions_impl!(PgInt2, i16, read_be_i16_)
from_option_impl!(i16)
from_conversions_impl!(PgInt4, i32, read_be_i32_)
from_option_impl!(i32)
from_conversions_impl!(PgInt8, i64, read_be_i64_)
from_option_impl!(i64)
from_conversions_impl!(PgFloat4, f32, read_be_f32_)
from_option_impl!(f32)
from_conversions_impl!(PgFloat8, f64, read_be_f64_)
from_option_impl!(f64)

from_map_impl!(PgVarchar | PgText | PgCharN, ~str, |buf| {
    str::from_utf8(buf.as_slice())
})
from_option_impl!(~str)

impl FromSql for Option<~[u8]> {
    fn from_sql(ty: PostgresType, raw: &Option<~[u8]>) -> Option<~[u8]> {
        check_types!(PgByteA, ty)
        raw.clone()
    }
}
from_option_impl!(~[u8])

from_map_impl!(PgJson, Json, |buf| {
    json::from_str(str::from_utf8_slice(buf.as_slice())).unwrap()
})
from_option_impl!(Json)

from_map_impl!(PgUuid, Uuid, |buf| {
    Uuid::from_bytes(buf.as_slice()).unwrap()
})
from_option_impl!(Uuid)

from_map_impl!(PgTimestamp | PgTimestampZ, Timespec, |buf| {
    let mut rdr = BufReader::new(buf.as_slice());
    let t = rdr.read_be_i64_();
    let mut sec = t / USEC_PER_SEC + TIME_SEC_CONVERSION;
    let mut usec = t % USEC_PER_SEC;

    if usec < 0 {
        sec -= 1;
        usec = USEC_PER_SEC + usec;
    }

    Timespec::new(sec, (usec * NSEC_PER_USEC) as i32)
})
from_option_impl!(Timespec)

/// A trait for types that can be converted into Postgres values
pub trait ToSql {
    /// Converts the value of `self` into a format appropriate for the Postgres
    /// backend.
    ///
    /// # Failure
    ///
    /// Fails if this type cannot be converted into the specified Postgres
    /// type.
    fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>);
}

macro_rules! to_option_impl(
    ($($oid:ident)|+, $t:ty) => (
        impl ToSql for Option<$t> {
            fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
                check_types!($($oid)|+, ty)

                match *self {
                    None => (Text, None),
                    Some(ref val) => val.to_sql(ty)
                }
            }
        }
    );
    (self, $($oid:ident)|+, $t:ty) => (
        impl<'self> ToSql for Option<$t> {
            fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
                check_types!($($oid)|+, ty)

                match *self {
                    None => (Text, None),
                    Some(ref val) => val.to_sql(ty)
                }
            }
        }
    )
)

macro_rules! to_conversions_impl(
    ($($oid:ident)|+, $t:ty, $f:ident) => (
        impl ToSql for $t {
            fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
                check_types!($($oid)|+, ty)

                let mut writer = MemWriter::new();
                writer.$f(*self);
                (Binary, Some(writer.inner()))
            }
        }
    )
)

impl ToSql for bool {
    fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
        check_types!(PgBool, ty)
        (Binary, Some(~[*self as u8]))
    }
}
to_option_impl!(PgBool, bool)

to_conversions_impl!(PgChar, i8, write_i8_)
to_option_impl!(PgChar, i8)
to_conversions_impl!(PgInt2, i16, write_be_i16_)
to_option_impl!(PgInt2, i16)
to_conversions_impl!(PgInt4, i32, write_be_i32_)
to_option_impl!(PgInt4, i32)
to_conversions_impl!(PgInt8, i64, write_be_i64_)
to_option_impl!(PgInt8, i64)
to_conversions_impl!(PgFloat4, f32, write_be_f32_)
to_option_impl!(PgFloat4, f32)
to_conversions_impl!(PgFloat8, f64, write_be_f64_)
to_option_impl!(PgFloat8, f64)

impl ToSql for ~str {
    fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
        check_types!(PgVarchar | PgText | PgCharN, ty)
        (Text, Some(self.as_bytes().to_owned()))
    }
}

impl<'self> ToSql for &'self str {
    fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
        check_types!(PgVarchar | PgText | PgCharN, ty)
        (Text, Some(self.as_bytes().to_owned()))
    }
}

to_option_impl!(PgVarchar | PgText | PgCharN, ~str)
to_option_impl!(self, PgVarchar | PgText | PgCharN, &'self str)

impl ToSql for ~[u8] {
    fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
        check_types!(PgByteA, ty)
        (Binary, Some(self.to_owned()))
    }
}

impl<'self> ToSql for &'self [u8] {
    fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
        check_types!(PgByteA, ty)
        (Binary, Some(self.to_owned()))
    }
}

to_option_impl!(PgByteA, ~[u8])
to_option_impl!(self, PgByteA, &'self [u8])

impl ToSql for Json {
    fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
        check_types!(PgJson, ty)
        (Text, Some(self.to_str().into_bytes()))
    }
}

to_option_impl!(PgJson, Json)

impl ToSql for Uuid {
    fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
        check_types!(PgUuid, ty)
        (Binary, Some(self.to_bytes().to_owned()))
    }
}

to_option_impl!(PgUuid, Uuid)

impl ToSql for Timespec {
    fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
        check_types!(PgTimestamp | PgTimestampZ, ty)
        let t = (self.sec - TIME_SEC_CONVERSION) * USEC_PER_SEC
            + self.nsec as i64 / NSEC_PER_USEC;
        let mut buf = MemWriter::new();
        buf.write_be_i64_(t);
        (Binary, Some(buf.inner()))
    }
}

to_option_impl!(PgTimestamp | PgTimestampZ, Timespec)

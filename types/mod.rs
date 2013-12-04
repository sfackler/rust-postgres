//! Traits dealing with Postgres data types

extern mod extra;

use extra::time::Timespec;
use extra::json;
use extra::json::Json;
use extra::uuid::Uuid;
use std::io::Decorator;
use std::io::mem::{MemWriter, BufReader};
use std::mem;
use std::str;
use std::vec;

use self::array::{Array, ArrayBase, DimensionInfo};
use self::range::{RangeBound, Inclusive, Exclusive, Range};

pub mod array;
pub mod range;

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
static INT4ARRAYOID: Oid = 1007;
static INT8ARRAYOID: Oid = 1016;
static BPCHAROID: Oid = 1042;
static VARCHAROID: Oid = 1043;
static TIMESTAMPOID: Oid = 1114;
static TIMESTAMPZOID: Oid = 1184;
static UUIDOID: Oid = 2950;
static INT4RANGEOID: Oid = 3904;
static TSRANGEOID: Oid = 3908;
static TSTZRANGEOID: Oid = 3910;
static INT8RANGEOID: Oid = 3926;

static USEC_PER_SEC: i64 = 1_000_000;
static NSEC_PER_USEC: i64 = 1_000;

// Number of seconds from 1970-01-01 to 2000-01-01
static TIME_SEC_CONVERSION: i64 = 946684800;

static RANGE_UPPER_UNBOUNDED: i8 = 0b0001_0000;
static RANGE_LOWER_UNBOUNDED: i8 = 0b0000_1000;
static RANGE_UPPER_INCLUSIVE: i8 = 0b0000_0100;
static RANGE_LOWER_INCLUSIVE: i8 = 0b0000_0010;
static RANGE_EMPTY: i8           = 0b0000_0001;

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
    /// INT4[]
    PgInt4Array,
    /// INT8[]
    PgInt8Array,
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
    /// INT4RANGE
    PgInt4Range,
    /// INT8RANGE
    PgInt8Range,
    /// TSRANGE
    PgTsRange,
    /// TSTZRANGE
    PgTstzRange,
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
            INT4ARRAYOID => PgInt4Array,
            INT8ARRAYOID => PgInt8Array,
            TIMESTAMPOID => PgTimestamp,
            TIMESTAMPZOID => PgTimestampZ,
            BPCHAROID => PgCharN,
            VARCHAROID => PgVarchar,
            UUIDOID => PgUuid,
            INT4RANGEOID => PgInt4Range,
            INT8RANGEOID => PgInt8Range,
            TSRANGEOID => PgTsRange,
            TSTZRANGEOID => PgTstzRange,
            oid => PgUnknownType(oid)
        }
    }

    /// Returns the wire format needed for the value of `self`.
    pub fn result_format(&self) -> Format {
        match *self {
            PgUnknownType(..) => Text,
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
            actual => fail!("Invalid Postgres type {:?}", actual)
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

trait RawFromSql {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Self;
}

macro_rules! raw_from_impl(
    ($t:ty, $f:ident) => (
        impl RawFromSql for $t {
            fn raw_from_sql<R: Reader>(raw: &mut R) -> $t {
                raw.$f()
            }
        }
    )
)

raw_from_impl!(i32, read_be_i32)
raw_from_impl!(i64, read_be_i64)

impl RawFromSql for Timespec {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Timespec {
        let t = raw.read_be_i64();
        let mut sec = t / USEC_PER_SEC + TIME_SEC_CONVERSION;
        let mut usec = t % USEC_PER_SEC;

        if usec < 0 {
            sec -= 1;
            usec = USEC_PER_SEC + usec;
        }

        Timespec::new(sec, (usec * NSEC_PER_USEC) as i32)
    }
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

macro_rules! from_raw_from_impl(
    ($($expected:pat)|+, $t:ty) => (
        from_map_impl!($($expected)|+, $t, |buf| {
            let mut reader = BufReader::new(buf.as_slice());
            RawFromSql::raw_from_sql(&mut reader)
        })
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

from_raw_from_impl!(PgInt4, i32)
from_option_impl!(i32)
from_raw_from_impl!(PgInt8, i64)
from_option_impl!(i64)

from_conversions_impl!(PgChar, i8, read_i8)
from_option_impl!(i8)
from_conversions_impl!(PgInt2, i16, read_be_i16)
from_option_impl!(i16)
from_conversions_impl!(PgFloat4, f32, read_be_f32)
from_option_impl!(f32)
from_conversions_impl!(PgFloat8, f64, read_be_f64)
from_option_impl!(f64)

from_map_impl!(PgVarchar | PgText | PgCharN, ~str, |buf| {
    str::from_utf8(buf.as_slice())
})
from_option_impl!(~str)

from_map_impl!(PgByteA, ~[u8], |buf| {
    buf.clone()
})
from_option_impl!(~[u8])

from_map_impl!(PgJson, Json, |buf| {
    json::from_str(str::from_utf8_slice(buf.as_slice())).unwrap()
})
from_option_impl!(Json)

from_map_impl!(PgUuid, Uuid, |buf| {
    Uuid::from_bytes(buf.as_slice()).unwrap()
})
from_option_impl!(Uuid)

from_raw_from_impl!(PgTimestamp | PgTimestampZ, Timespec)
from_option_impl!(Timespec)

macro_rules! from_range_impl(
    ($($oid:ident)|+, $t:ty) => (
        from_map_impl!($($oid)|+, Range<$t>, |buf| {
            let mut rdr = BufReader::new(buf.as_slice());
            let t = rdr.read_i8();

            if t & RANGE_EMPTY != 0 {
                Range::empty()
            } else {
                let lower = match t & RANGE_LOWER_UNBOUNDED {
                    0 => {
                        let type_ = match t & RANGE_LOWER_INCLUSIVE {
                            0 => Exclusive,
                            _ => Inclusive
                        };
                        rdr.read_be_i32();
                        Some(RangeBound::new(RawFromSql::raw_from_sql(&mut rdr),
                                             type_))
                    }
                    _ => None
                };
                let upper = match t & RANGE_UPPER_UNBOUNDED {
                    0 => {
                        let type_ = match t & RANGE_UPPER_INCLUSIVE {
                            0 => Exclusive,
                            _ => Inclusive
                        };
                        rdr.read_be_i32();
                        Some(RangeBound::new(RawFromSql::raw_from_sql(&mut rdr),
                                             type_))
                    }
                    _ => None
                };

                Range::new(lower, upper)
            }
        })
    )
)

from_range_impl!(PgInt4Range, i32)
from_option_impl!(Range<i32>)

from_range_impl!(PgInt8Range, i64)
from_option_impl!(Range<i64>)

from_range_impl!(PgTsRange | PgTstzRange, Timespec)
from_option_impl!(Range<Timespec>)

macro_rules! from_array_impl(
    ($($oid:ident)|+, $t:ty) => (
        from_map_impl!($($oid)|+, ArrayBase<Option<$t>>, |buf| {
            let mut rdr = BufReader::new(buf.as_slice());

            let ndim = rdr.read_be_i32() as uint;
            let _has_null = rdr.read_be_i32() == 1;
            let _element_type: Oid = rdr.read_be_i32();

            let mut dim_info = vec::with_capacity(ndim);
            for _ in range(0, ndim) {
                dim_info.push(DimensionInfo {
                    len: rdr.read_be_i32() as uint,
                    lower_bound: rdr.read_be_i32() as int
                });
            }
            let nele = dim_info.iter().fold(1, |acc, info| acc * info.len);

            let mut elements = vec::with_capacity(nele);
            for _ in range(0, nele) {
                let len = rdr.read_be_i32();
                if len < 0 {
                    elements.push(None);
                } else {
                    elements.push(Some(RawFromSql::raw_from_sql(&mut rdr)));
                }
            }

            ArrayBase::from_raw(elements, dim_info)
        })
    )
)

from_array_impl!(PgInt4Array, i32)
from_option_impl!(ArrayBase<Option<i32>>)

from_array_impl!(PgInt8Array, i64)
from_option_impl!(ArrayBase<Option<i64>>)

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

trait RawToSql {
    fn raw_to_sql<W: Writer>(&self, w: &mut W);

    fn raw_size(&self) -> uint;
}

macro_rules! raw_to_impl(
    ($t:ty, $f:ident) => (
        impl RawToSql for $t {
            fn raw_to_sql<W: Writer>(&self, w: &mut W) {
                w.$f(*self)
            }

            fn raw_size(&self) -> uint {
                mem::size_of::<$t>()
            }
        }
    )
)

raw_to_impl!(i32, write_be_i32)
raw_to_impl!(i64, write_be_i64)

impl RawToSql for Timespec {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) {
        let t = (self.sec - TIME_SEC_CONVERSION) * USEC_PER_SEC
            + self.nsec as i64 / NSEC_PER_USEC;
        w.write_be_i64(t);
    }

    fn raw_size(&self) -> uint {
        8
    }
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

macro_rules! to_raw_to_impl(
    ($($oid:ident)|+, $t:ty) => (
        impl ToSql for $t {
            fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
                check_types!($($oid)|+, ty)

                let mut writer = MemWriter::new();
                self.raw_to_sql(&mut writer);
                (Binary, Some(writer.inner()))
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

to_raw_to_impl!(PgInt4, i32)
to_option_impl!(PgInt4, i32)
to_raw_to_impl!(PgInt8, i64)
to_option_impl!(PgInt8, i64)

to_conversions_impl!(PgChar, i8, write_i8)
to_option_impl!(PgChar, i8)
to_conversions_impl!(PgInt2, i16, write_be_i16)
to_option_impl!(PgInt2, i16)
to_conversions_impl!(PgFloat4, f32, write_be_f32)
to_option_impl!(PgFloat4, f32)
to_conversions_impl!(PgFloat8, f64, write_be_f64)
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

to_raw_to_impl!(PgTimestamp | PgTimestampZ, Timespec)
to_option_impl!(PgTimestamp | PgTimestampZ, Timespec)

macro_rules! to_range_impl(
    ($($oid:ident)|+, $t:ty) => (
        impl ToSql for Range<$t> {
            fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
                check_types!($($oid)|+, ty)
                let mut buf = MemWriter::new();

                let mut tag = 0;
                if self.is_empty() {
                    tag |= RANGE_EMPTY;
                } else {
                    match self.lower() {
                        &None => tag |= RANGE_LOWER_UNBOUNDED,
                        &Some(RangeBound { type_: Inclusive, .. }) =>
                            tag |= RANGE_LOWER_INCLUSIVE,
                        _ => {}
                    }
                    match self.upper() {
                        &None => tag |= RANGE_UPPER_UNBOUNDED,
                        &Some(RangeBound { type_: Inclusive, .. }) =>
                            tag |= RANGE_UPPER_INCLUSIVE,
                        _ => {}
                    }
                }

                buf.write_i8(tag);

                match self.lower() {
                    &Some(ref bound) => {
                        buf.write_be_i32(bound.value.raw_size() as i32);
                        bound.value.raw_to_sql(&mut buf);
                    }
                    &None => {}
                }
                match self.upper() {
                    &Some(ref bound) => {
                        buf.write_be_i32(bound.value.raw_size() as i32);
                        bound.value.raw_to_sql(&mut buf);
                    }
                    &None => {}
                }

                (Binary, Some(buf.inner()))
            }
        }
    )
)

to_range_impl!(PgInt4Range, i32)
to_option_impl!(PgInt4Range, Range<i32>)

to_range_impl!(PgInt8Range, i64)
to_option_impl!(PgInt8Range, Range<i64>)

to_range_impl!(PgTsRange | PgTstzRange, Timespec)
to_option_impl!(PgTsRange | PgTstzRange, Range<Timespec>)

macro_rules! to_array_impl(
    ($($oid:ident)|+, $base_oid:ident, $t:ty) => (
        impl ToSql for ArrayBase<Option<$t>> {
            fn to_sql(&self, ty: PostgresType) -> (Format, Option<~[u8]>) {
                check_types!($($oid)|+, ty)
                let mut buf = MemWriter::new();

                buf.write_be_i32(self.dimension_info().len() as i32);
                buf.write_be_i32(1);
                buf.write_be_i32($base_oid);

                for info in self.dimension_info().iter() {
                    buf.write_be_i32(info.len as i32);
                    buf.write_be_i32(info.lower_bound as i32);
                }

                for v in self.values() {
                    match *v {
                        Some(ref val) => {
                            buf.write_be_i32(val.raw_size() as i32);
                            val.raw_to_sql(&mut buf);
                        }
                        None => buf.write_be_i32(-1)
                    }
                }

                (Binary, Some(buf.inner()))
            }
        }
    )
)

to_array_impl!(PgInt4Array, INT4OID, i32)
to_option_impl!(PgInt4Array, ArrayBase<Option<i32>>)

to_array_impl!(PgInt8Array, INT8OID, i64)
to_option_impl!(PgInt8Array, ArrayBase<Option<i64>>)

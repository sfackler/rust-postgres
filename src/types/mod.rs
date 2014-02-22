//! Traits dealing with Postgres data types
#[macro_escape];

extern crate extra;

use time::Timespec;
use extra::json;
use extra::json::Json;
use uuid::Uuid;
use std::hashmap::HashMap;
use std::io::{MemWriter, BufReader};
use std::io::util::LimitReader;
use std::str;
use std::vec;

use self::array::{Array, ArrayBase, DimensionInfo};
use self::range::{RangeBound, Inclusive, Exclusive, Range};

pub mod array;
pub mod range;

/// A Postgres OID
pub type Oid = u32;

// Values from pg_type.h
static BOOLOID: Oid = 16;
static BYTEAOID: Oid = 17;
static CHAROID: Oid = 18;
static INT8OID: Oid = 20;
static INT2OID: Oid = 21;
static INT4OID: Oid = 23;
static TEXTOID: Oid = 25;
static JSONOID: Oid = 114;
static JSONARRAYOID: Oid = 199;
static FLOAT4OID: Oid = 700;
static FLOAT8OID: Oid = 701;
static BOOLARRAYOID: Oid = 1000;
static BYTEAARRAYOID: Oid = 1001;
static CHARARRAYOID: Oid = 1002;
static INT2ARRAYOID: Oid = 1005;
static INT4ARRAYOID: Oid = 1007;
static TEXTARRAYOID: Oid = 1009;
static BPCHARARRAYOID: Oid = 1014;
static VARCHARARRAYOID: Oid = 1015;
static INT8ARRAYOID: Oid = 1016;
static FLOAT4ARRAYOID: Oid = 1021;
static FLAOT8ARRAYOID: Oid = 1022;
static BPCHAROID: Oid = 1042;
static VARCHAROID: Oid = 1043;
static TIMESTAMPOID: Oid = 1114;
static TIMESTAMPARRAYOID: Oid = 1115;
static TIMESTAMPZOID: Oid = 1184;
static TIMESTAMPZARRAYOID: Oid = 1185;
static UUIDOID: Oid = 2950;
static UUIDARRAYOID: Oid = 2951;
static INT4RANGEOID: Oid = 3904;
static INT4RANGEARRAYOID: Oid = 3905;
static TSRANGEOID: Oid = 3908;
static TSRANGEARRAYOID: Oid = 3909;
static TSTZRANGEOID: Oid = 3910;
static TSTZRANGEARRAYOID: Oid = 3911;
static INT8RANGEOID: Oid = 3926;
static INT8RANGEARRAYOID: Oid = 3927;

static USEC_PER_SEC: i64 = 1_000_000;
static NSEC_PER_USEC: i64 = 1_000;

// Number of seconds from 1970-01-01 to 2000-01-01
static TIME_SEC_CONVERSION: i64 = 946684800;

static RANGE_UPPER_UNBOUNDED: i8 = 0b0001_0000;
static RANGE_LOWER_UNBOUNDED: i8 = 0b0000_1000;
static RANGE_UPPER_INCLUSIVE: i8 = 0b0000_0100;
static RANGE_LOWER_INCLUSIVE: i8 = 0b0000_0010;
static RANGE_EMPTY: i8           = 0b0000_0001;

macro_rules! make_postgres_type(
    ($($doc:attr $oid:ident => $variant:ident $(member $member:ident)*),+) => (
        /// A Postgres type
        #[deriving(Eq, Show)]
        pub enum PostgresType {
            $(
                $doc
                $variant,
            )+
            /// An unknown type
            PgUnknownType {
                /// The name of the type
                name: ~str,
                /// The OID of the type
                oid: Oid
            }
        }

        impl PostgresType {
            #[doc(hidden)]
            pub fn from_oid(oid: Oid) -> PostgresType {
                match oid {
                    $($oid => $variant,)+
                    // We have to load an empty string now, it'll get filled in later
                    oid => PgUnknownType { name: ~"", oid: oid }
                }
            }

            #[doc(hidden)]
            pub fn to_oid(&self) -> Oid {
                match *self {
                    $($variant => $oid,)+
                    PgUnknownType { oid, .. } => oid
                }
            }

            fn member_type(&self) -> PostgresType {
                match *self {
                    $(
                        $($variant => $member,)*
                    )+
                    _ => unreachable!()
                }
            }

            /// Returns the wire format needed for the value of `self`.
            pub fn result_format(&self) -> Format {
                match *self {
                    PgUnknownType { name: ~"hstore", .. } => Binary,
                    PgUnknownType { .. } => Text,
                    _ => Binary
                }
            }
        }
    )
)

make_postgres_type!(
    #[doc="BOOL"]
    BOOLOID => PgBool,
    #[doc="BYTEA"]
    BYTEAOID => PgByteA,
    #[doc="\"char\""]
    CHAROID => PgChar,
    #[doc="INT8/BIGINT"]
    INT8OID => PgInt8,
    #[doc="INT2/SMALLINT"]
    INT2OID => PgInt2,
    #[doc="INT4/INT"]
    INT4OID => PgInt4,
    #[doc="TEXT"]
    TEXTOID => PgText,
    #[doc="JSON"]
    JSONOID => PgJson,
    #[doc="JSON[]"]
    JSONARRAYOID => PgJsonArray member PgJson,
    #[doc="FLOAT4/REAL"]
    FLOAT4OID => PgFloat4,
    #[doc="FLOAT8/DOUBLE PRECISION"]
    FLOAT8OID => PgFloat8,
    #[doc="BOOL[]"]
    BOOLARRAYOID => PgBoolArray member PgBool,
    #[doc="BYTEA[]"]
    BYTEAARRAYOID => PgByteAArray member PgByteA,
    #[doc="\"char\"[]"]
    CHARARRAYOID => PgCharArray member PgChar,
    #[doc="INT2[]"]
    INT2ARRAYOID => PgInt2Array member PgInt2,
    #[doc="INT4[]"]
    INT4ARRAYOID => PgInt4Array member PgInt4,
    #[doc="TEXT[]"]
    TEXTARRAYOID => PgTextArray member PgText,
    #[doc="CHAR(n)[]"]
    BPCHARARRAYOID => PgCharNArray member PgCharN,
    #[doc="VARCHAR[]"]
    VARCHARARRAYOID => PgVarcharArray member PgVarchar,
    #[doc="INT8[]"]
    INT8ARRAYOID => PgInt8Array member PgInt8,
    #[doc="FLOAT4[]"]
    FLOAT4ARRAYOID => PgFloat4Array member PgFloat4,
    #[doc="FLOAT8[]"]
    FLAOT8ARRAYOID => PgFloat8Array member PgFloat8,
    #[doc="TIMESTAMP"]
    TIMESTAMPOID => PgTimestamp,
    #[doc="TIMESTAMP[]"]
    TIMESTAMPARRAYOID => PgTimestampArray member PgTimestamp,
    #[doc="TIMESTAMP WITH TIME ZONE"]
    TIMESTAMPZOID => PgTimestampTZ,
    #[doc="TIMESTAMP WITH TIME ZONE[]"]
    TIMESTAMPZARRAYOID => PgTimestampTZArray member PgTimestampTZ,
    #[doc="CHAR(n)/CHARACTER(n)"]
    BPCHAROID => PgCharN,
    #[doc="VARCHAR/CHARACTER VARYING"]
    VARCHAROID => PgVarchar,
    #[doc="UUID"]
    UUIDOID => PgUuid,
    #[doc="UUID[]"]
    UUIDARRAYOID => PgUuidArray member PgUuid,
    #[doc="INT4RANGE"]
    INT4RANGEOID => PgInt4Range,
    #[doc="INT4RANGE[]"]
    INT4RANGEARRAYOID => PgInt4RangeArray member PgInt4Range,
    #[doc="TSRANGE"]
    TSRANGEOID => PgTsRange,
    #[doc="TSRANGE[]"]
    TSRANGEARRAYOID => PgTsRangeArray member PgTsRange,
    #[doc="TSTZRANGE"]
    TSTZRANGEOID => PgTstzRange,
    #[doc="TSTZRANGE[]"]
    TSTZRANGEARRAYOID => PgTstzRangeArray member PgTstzRange,
    #[doc="INT8RANGE"]
    INT8RANGEOID => PgInt8Range,
    #[doc="INT8RANGE[]"]
    INT8RANGEARRAYOID => PgInt8RangeArray member PgInt8Range
)

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
            $(&$expected)|+ => (),
            actual => fail!("Invalid Postgres type {}", *actual)
        }
    )
)

macro_rules! or_fail(
    ($e:expr) => (
        match $e {
            Ok(ok) => ok,
            Err(err) => fail!("{}", err)
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
    fn from_sql(ty: &PostgresType, raw: &Option<~[u8]>) -> Self;
}

#[doc(hidden)]
trait RawFromSql {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Self;
}

macro_rules! raw_from_impl(
    ($t:ty, $f:ident) => (
        impl RawFromSql for $t {
            fn raw_from_sql<R: Reader>(raw: &mut R) -> $t {
                or_fail!(raw.$f())
            }
        }
    )
)

impl RawFromSql for bool {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> bool {
        (or_fail!(raw.read_u8())) != 0
    }
}

impl RawFromSql for ~[u8] {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> ~[u8] {
        or_fail!(raw.read_to_end())
    }
}

impl RawFromSql for ~str {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> ~str {
        str::from_utf8_owned(or_fail!(raw.read_to_end())).unwrap()
    }
}

raw_from_impl!(i8, read_i8)
raw_from_impl!(i16, read_be_i16)
raw_from_impl!(i32, read_be_i32)
raw_from_impl!(i64, read_be_i64)
raw_from_impl!(f32, read_be_f32)
raw_from_impl!(f64, read_be_f64)

impl RawFromSql for Timespec {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Timespec {
        let t = or_fail!(raw.read_be_i64());
        let mut sec = t / USEC_PER_SEC + TIME_SEC_CONVERSION;
        let mut usec = t % USEC_PER_SEC;

        if usec < 0 {
            sec -= 1;
            usec = USEC_PER_SEC + usec;
        }

        Timespec::new(sec, (usec * NSEC_PER_USEC) as i32)
    }
}

impl RawFromSql for Uuid {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Uuid {
        Uuid::from_bytes(or_fail!(raw.read_to_end())).unwrap()
    }
}

macro_rules! from_range_impl(
    ($($oid:ident)|+, $t:ty) => (
        impl RawFromSql for Range<$t> {
            fn raw_from_sql<R: Reader>(rdr: &mut R) -> Range<$t> {
                let t = or_fail!(rdr.read_i8());

                if t & RANGE_EMPTY != 0 {
                    Range::empty()
                } else {
                    let lower = match t & RANGE_LOWER_UNBOUNDED {
                        0 => {
                            let type_ = match t & RANGE_LOWER_INCLUSIVE {
                                0 => Exclusive,
                                _ => Inclusive
                            };
                            let len = or_fail!(rdr.read_be_i32()) as uint;
                            let mut limit = LimitReader::new(rdr.by_ref(), len);
                            let lower = Some(RangeBound::new(
                                    RawFromSql::raw_from_sql(&mut limit), type_));
                            assert!(limit.limit() == 0);
                            lower
                        }
                        _ => None
                    };
                    let upper = match t & RANGE_UPPER_UNBOUNDED {
                        0 => {
                            let type_ = match t & RANGE_UPPER_INCLUSIVE {
                                0 => Exclusive,
                                _ => Inclusive
                            };
                            let len = or_fail!(rdr.read_be_i32()) as uint;
                            let mut limit = LimitReader::new(rdr.by_ref(), len);
                            let upper = Some(RangeBound::new(
                                    RawFromSql::raw_from_sql(&mut limit), type_));
                            assert!(limit.limit() == 0);
                            upper
                        }
                        _ => None
                    };

                    Range::new(lower, upper)
                }
            }
        }
    )
)

from_range_impl!(PgInt4Range, i32)
from_range_impl!(PgInt8Range, i64)
from_range_impl!(PgTsRange | PgTstzRange, Timespec)

impl RawFromSql for Json {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Json {
        json::from_reader(raw as &mut Reader).unwrap()
    }
}

macro_rules! from_map_impl(
    ($($expected:pat)|+, $t:ty, $blk:expr) => (
        impl FromSql for Option<$t> {
            fn from_sql(ty: &PostgresType, raw: &Option<~[u8]>) -> Option<$t> {
                check_types!($($expected)|+, ty)
                raw.as_ref().map($blk)
            }
        }

        impl FromSql for $t {
            fn from_sql(ty: &PostgresType, raw: &Option<~[u8]>) -> $t {
                // FIXME when you can specify Self types properly
                let ret: Option<$t> = FromSql::from_sql(ty, raw);
                ret.unwrap()
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

from_raw_from_impl!(PgBool, bool)
from_raw_from_impl!(PgByteA, ~[u8])
from_raw_from_impl!(PgVarchar | PgText | PgCharN, ~str)
from_raw_from_impl!(PgChar, i8)
from_raw_from_impl!(PgInt2, i16)
from_raw_from_impl!(PgInt4, i32)
from_raw_from_impl!(PgInt8, i64)
from_raw_from_impl!(PgFloat4, f32)
from_raw_from_impl!(PgFloat8, f64)
from_raw_from_impl!(PgUuid, Uuid)
from_raw_from_impl!(PgJson, Json)

from_raw_from_impl!(PgTimestamp | PgTimestampTZ, Timespec)
from_raw_from_impl!(PgInt4Range, Range<i32>)
from_raw_from_impl!(PgInt8Range, Range<i64>)
from_raw_from_impl!(PgTsRange | PgTstzRange, Range<Timespec>)

macro_rules! from_array_impl(
    ($($oid:ident)|+, $t:ty) => (
        from_map_impl!($($oid)|+, ArrayBase<Option<$t>>, |buf| {
            let mut rdr = BufReader::new(buf.as_slice());

            let ndim = or_fail!(rdr.read_be_i32()) as uint;
            let _has_null = or_fail!(rdr.read_be_i32()) == 1;
            let _element_type: Oid = or_fail!(rdr.read_be_u32());

            let mut dim_info = vec::with_capacity(ndim);
            for _ in range(0, ndim) {
                dim_info.push(DimensionInfo {
                    len: or_fail!(rdr.read_be_i32()) as uint,
                    lower_bound: or_fail!(rdr.read_be_i32()) as int
                });
            }
            let nele = dim_info.iter().fold(1, |acc, info| acc * info.len);

            let mut elements = vec::with_capacity(nele);
            for _ in range(0, nele) {
                let len = or_fail!(rdr.read_be_i32());
                if len < 0 {
                    elements.push(None);
                } else {
                    let mut limit = LimitReader::new(rdr.by_ref(), len as uint);
                    elements.push(Some(RawFromSql::raw_from_sql(&mut limit)));
                    assert!(limit.limit() == 0);
                }
            }

            ArrayBase::from_raw(elements, dim_info)
        })
    )
)

from_array_impl!(PgBoolArray, bool)
from_array_impl!(PgByteAArray, ~[u8])
from_array_impl!(PgCharArray, i8)
from_array_impl!(PgInt2Array, i16)
from_array_impl!(PgInt4Array, i32)
from_array_impl!(PgTextArray | PgCharNArray | PgVarcharArray, ~str)
from_array_impl!(PgInt8Array, i64)
from_array_impl!(PgTimestampArray | PgTimestampTZArray, Timespec)
from_array_impl!(PgJsonArray, Json)
from_array_impl!(PgFloat4Array, f32)
from_array_impl!(PgFloat8Array, f64)
from_array_impl!(PgUuidArray, Uuid)
from_array_impl!(PgInt4RangeArray, Range<i32>)
from_array_impl!(PgTsRangeArray | PgTstzRangeArray, Range<Timespec>)
from_array_impl!(PgInt8RangeArray, Range<i64>)

from_map_impl!(PgUnknownType { name: ~"hstore", .. },
               HashMap<~str, Option<~str>>, |buf| {
    let mut rdr = BufReader::new(buf.as_slice());
    let mut map = HashMap::new();

    let count = or_fail!(rdr.read_be_i32());

    for _ in range(0, count) {
        let key_len = or_fail!(rdr.read_be_i32());
        let key = str::from_utf8_owned(or_fail!(rdr.read_bytes(key_len as uint))).unwrap();

        let val_len = or_fail!(rdr.read_be_i32());
        let val = if val_len < 0 {
            None
        } else {
            Some(str::from_utf8_owned(or_fail!(rdr.read_bytes(val_len as uint))).unwrap())
        };

        map.insert(key, val);
    }

    map
})

/// A trait for types that can be converted into Postgres values
pub trait ToSql {
    /// Converts the value of `self` into a format appropriate for the Postgres
    /// backend.
    ///
    /// # Failure
    ///
    /// Fails if this type cannot be converted into the specified Postgres
    /// type.
    fn to_sql(&self, ty: &PostgresType) -> (Format, Option<~[u8]>);
}

#[doc(hidden)]
trait RawToSql {
    fn raw_to_sql<W: Writer>(&self, w: &mut W);
}

macro_rules! raw_to_impl(
    ($t:ty, $f:ident) => (
        impl RawToSql for $t {
            fn raw_to_sql<W: Writer>(&self, w: &mut W) {
                or_fail!(w.$f(*self))
            }
        }
    )
)

impl RawToSql for bool {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) {
        or_fail!(w.write_u8(*self as u8))
    }
}

impl RawToSql for ~[u8] {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) {
        or_fail!(w.write(self.as_slice()))
    }
}

impl RawToSql for ~str {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) {
        or_fail!(w.write(self.as_bytes()))
    }
}

raw_to_impl!(i8, write_i8)
raw_to_impl!(i16, write_be_i16)
raw_to_impl!(i32, write_be_i32)
raw_to_impl!(i64, write_be_i64)
raw_to_impl!(f32, write_be_f32)
raw_to_impl!(f64, write_be_f64)

impl RawToSql for Timespec {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) {
        let t = (self.sec - TIME_SEC_CONVERSION) * USEC_PER_SEC
            + self.nsec as i64 / NSEC_PER_USEC;
        or_fail!(w.write_be_i64(t))
    }
}

impl RawToSql for Uuid {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) {
        or_fail!(w.write(self.to_bytes()))
    }
}

macro_rules! to_range_impl(
    ($($oid:ident)|+, $t:ty) => (
        impl RawToSql for Range<$t> {
            fn raw_to_sql<W: Writer>(&self, buf: &mut W) {
                let mut tag = 0;
                if self.is_empty() {
                    tag |= RANGE_EMPTY;
                } else {
                    match self.lower() {
                        None => tag |= RANGE_LOWER_UNBOUNDED,
                        Some(&RangeBound { type_: Inclusive, .. }) =>
                            tag |= RANGE_LOWER_INCLUSIVE,
                        _ => {}
                    }
                    match self.upper() {
                        None => tag |= RANGE_UPPER_UNBOUNDED,
                        Some(&RangeBound { type_: Inclusive, .. }) =>
                            tag |= RANGE_UPPER_INCLUSIVE,
                        _ => {}
                    }
                }

                or_fail!(buf.write_i8(tag));

                match self.lower() {
                    Some(bound) => {
                        let mut inner_buf = MemWriter::new();
                        bound.value.raw_to_sql(&mut inner_buf);
                        let inner_buf = inner_buf.unwrap();
                        or_fail!(buf.write_be_i32(inner_buf.len() as i32));
                        or_fail!(buf.write(inner_buf));
                    }
                    None => {}
                }
                match self.upper() {
                    Some(bound) => {
                        let mut inner_buf = MemWriter::new();
                        bound.value.raw_to_sql(&mut inner_buf);
                        let inner_buf = inner_buf.unwrap();
                        or_fail!(buf.write_be_i32(inner_buf.len() as i32));
                        or_fail!(buf.write(inner_buf));
                    }
                    None => {}
                }
            }
        }
    )
)

to_range_impl!(PgInt4Range, i32)
to_range_impl!(PgInt8Range, i64)
to_range_impl!(PgTsRange | PgTstzRange, Timespec)

impl RawToSql for Json {
    fn raw_to_sql<W: Writer>(&self, raw: &mut W) {
        or_fail!(self.to_writer(raw as &mut Writer))
    }
}

macro_rules! to_option_impl(
    ($($oid:pat)|+, $t:ty) => (
        impl ToSql for Option<$t> {
            fn to_sql(&self, ty: &PostgresType) -> (Format, Option<~[u8]>) {
                check_types!($($oid)|+, ty)

                match *self {
                    None => (Text, None),
                    Some(ref val) => val.to_sql(ty)
                }
            }
        }
    )
)

macro_rules! to_option_impl_lifetime(
    ($($oid:pat)|+, $t:ty) => (
        impl<'a> ToSql for Option<$t> {
            fn to_sql(&self, ty: &PostgresType) -> (Format, Option<~[u8]>) {
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
            fn to_sql(&self, ty: &PostgresType) -> (Format, Option<~[u8]>) {
                check_types!($($oid)|+, ty)

                let mut writer = MemWriter::new();
                self.raw_to_sql(&mut writer);
                (Binary, Some(writer.unwrap()))
            }
        }

        to_option_impl!($($oid)|+, $t)
    )
)

to_raw_to_impl!(PgBool, bool)
to_raw_to_impl!(PgByteA, ~[u8])
to_raw_to_impl!(PgVarchar | PgText | PgCharN, ~str)
to_raw_to_impl!(PgJson, Json)
to_raw_to_impl!(PgChar, i8)
to_raw_to_impl!(PgInt2, i16)
to_raw_to_impl!(PgInt4, i32)
to_raw_to_impl!(PgInt8, i64)
to_raw_to_impl!(PgFloat4, f32)
to_raw_to_impl!(PgFloat8, f64)
to_raw_to_impl!(PgInt4Range, Range<i32>)
to_raw_to_impl!(PgInt8Range, Range<i64>)
to_raw_to_impl!(PgTsRange | PgTstzRange, Range<Timespec>)

impl<'a> ToSql for &'a str {
    fn to_sql(&self, ty: &PostgresType) -> (Format, Option<~[u8]>) {
        check_types!(PgVarchar | PgText | PgCharN, ty)
        (Text, Some(self.as_bytes().to_owned()))
    }
}

to_option_impl_lifetime!(PgVarchar | PgText | PgCharN, &'a str)

impl<'a> ToSql for &'a [u8] {
    fn to_sql(&self, ty: &PostgresType) -> (Format, Option<~[u8]>) {
        check_types!(PgByteA, ty)
        (Binary, Some(self.to_owned()))
    }
}

to_option_impl_lifetime!(PgByteA, &'a [u8])

to_raw_to_impl!(PgTimestamp | PgTimestampTZ, Timespec)
to_raw_to_impl!(PgUuid, Uuid)

macro_rules! to_array_impl(
    ($($oid:ident)|+, $t:ty) => (
        impl ToSql for ArrayBase<Option<$t>> {
            fn to_sql(&self, ty: &PostgresType) -> (Format, Option<~[u8]>) {
                check_types!($($oid)|+, ty)
                let mut buf = MemWriter::new();

                or_fail!(buf.write_be_i32(self.dimension_info().len() as i32));
                or_fail!(buf.write_be_i32(1));
                or_fail!(buf.write_be_u32(ty.member_type().to_oid()));

                for info in self.dimension_info().iter() {
                    or_fail!(buf.write_be_i32(info.len as i32));
                    or_fail!(buf.write_be_i32(info.lower_bound as i32));
                }

                for v in self.values() {
                    match *v {
                        Some(ref val) => {
                            let mut inner_buf = MemWriter::new();
                            val.raw_to_sql(&mut inner_buf);
                            let inner_buf = inner_buf.unwrap();
                            or_fail!(buf.write_be_i32(inner_buf.len() as i32));
                            or_fail!(buf.write(inner_buf));
                        }
                        None => or_fail!(buf.write_be_i32(-1))
                    }
                }

                (Binary, Some(buf.unwrap()))
            }
        }

        to_option_impl!($($oid)|+, ArrayBase<Option<$t>>)
    )
)

to_array_impl!(PgBoolArray, bool)
to_array_impl!(PgByteAArray, ~[u8])
to_array_impl!(PgCharArray, i8)
to_array_impl!(PgInt2Array, i16)
to_array_impl!(PgInt4Array, i32)
to_array_impl!(PgTextArray | PgCharNArray | PgVarcharArray, ~str)
to_array_impl!(PgInt8Array, i64)
to_array_impl!(PgTimestampArray | PgTimestampTZArray, Timespec)
to_array_impl!(PgFloat4Array, f32)
to_array_impl!(PgFloat8Array, f64)
to_array_impl!(PgUuidArray, Uuid)
to_array_impl!(PgInt4RangeArray, Range<i32>)
to_array_impl!(PgTsRangeArray | PgTstzRangeArray, Range<Timespec>)
to_array_impl!(PgInt8RangeArray, Range<i64>)
to_array_impl!(PgJsonArray, Json)

impl<'a> ToSql for HashMap<~str, Option<~str>> {
    fn to_sql(&self, ty: &PostgresType) -> (Format, Option<~[u8]>) {
        check_types!(PgUnknownType { name: ~"hstore", .. }, ty)
        let mut buf = MemWriter::new();

        or_fail!(buf.write_be_i32(self.len() as i32));

        for (key, val) in self.iter() {
            or_fail!(buf.write_be_i32(key.len() as i32));
            or_fail!(buf.write(key.as_bytes()));

            match *val {
                Some(ref val) => {
                    or_fail!(buf.write_be_i32(val.len() as i32));
                    or_fail!(buf.write(val.as_bytes()));
                }
                None => or_fail!(buf.write_be_i32(-1))
            }
        }

        (Binary, Some(buf.unwrap()))
    }
}
to_option_impl!(PgUnknownType { name: ~"hstore", .. },
                HashMap<~str, Option<~str>>)

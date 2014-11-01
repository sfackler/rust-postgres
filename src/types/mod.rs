//! Traits dealing with Postgres data types
#![macro_escape]

use serialize::json;
use serialize::json::Json;
use std::collections::HashMap;
use std::io::{AsRefReader, MemWriter, BufReader};
use std::io::util::LimitReader;
use time::Timespec;

use Result;
use error::{PgWrongType, PgStreamError, PgWasNull, PgBadData};
use types::array::{Array, ArrayBase, DimensionInfo};
use types::range::{RangeBound, Inclusive, Exclusive, Range};

pub mod array;
pub mod range;

/// A Postgres OID
pub type Oid = u32;

// Values from pg_type.h
const BOOLOID: Oid = 16;
const BYTEAOID: Oid = 17;
const CHAROID: Oid = 18;
const NAMEOID: Oid = 19;
const INT8OID: Oid = 20;
const INT2OID: Oid = 21;
const INT4OID: Oid = 23;
const TEXTOID: Oid = 25;
const JSONOID: Oid = 114;
const JSONARRAYOID: Oid = 199;
const FLOAT4OID: Oid = 700;
const FLOAT8OID: Oid = 701;
const BOOLARRAYOID: Oid = 1000;
const BYTEAARRAYOID: Oid = 1001;
const CHARARRAYOID: Oid = 1002;
const NAMEARRAYOID: Oid = 1003;
const INT2ARRAYOID: Oid = 1005;
const INT4ARRAYOID: Oid = 1007;
const TEXTARRAYOID: Oid = 1009;
const BPCHARARRAYOID: Oid = 1014;
const VARCHARARRAYOID: Oid = 1015;
const INT8ARRAYOID: Oid = 1016;
const FLOAT4ARRAYOID: Oid = 1021;
const FLAOT8ARRAYOID: Oid = 1022;
const BPCHAROID: Oid = 1042;
const VARCHAROID: Oid = 1043;
const TIMESTAMPOID: Oid = 1114;
const TIMESTAMPARRAYOID: Oid = 1115;
const TIMESTAMPZOID: Oid = 1184;
const TIMESTAMPZARRAYOID: Oid = 1185;
const INT4RANGEOID: Oid = 3904;
const INT4RANGEARRAYOID: Oid = 3905;
const TSRANGEOID: Oid = 3908;
const TSRANGEARRAYOID: Oid = 3909;
const TSTZRANGEOID: Oid = 3910;
const TSTZRANGEARRAYOID: Oid = 3911;
const INT8RANGEOID: Oid = 3926;
const INT8RANGEARRAYOID: Oid = 3927;

const USEC_PER_SEC: i64 = 1_000_000;
const NSEC_PER_USEC: i64 = 1_000;

// Number of seconds from 1970-01-01 to 2000-01-01
const TIME_SEC_CONVERSION: i64 = 946684800;

const RANGE_UPPER_UNBOUNDED: i8 = 0b0001_0000;
const RANGE_LOWER_UNBOUNDED: i8 = 0b0000_1000;
const RANGE_UPPER_INCLUSIVE: i8 = 0b0000_0100;
const RANGE_LOWER_INCLUSIVE: i8 = 0b0000_0010;
const RANGE_EMPTY: i8           = 0b0000_0001;

macro_rules! make_postgres_type(
    ($(#[$doc:meta] $oid:ident => $variant:ident $(member $member:ident)*),+) => (
        /// A Postgres type
        #[deriving(PartialEq, Eq, Clone, Show)]
        pub enum PostgresType {
            $(
                #[$doc]
                $variant,
            )+
            /// An unknown type
            PgUnknownType {
                /// The name of the type
                pub name: String,
                /// The OID of the type
                pub oid: Oid
            }
        }

        impl PostgresType {
            #[doc(hidden)]
            pub fn from_oid(oid: Oid) -> PostgresType {
                match oid {
                    $($oid => $variant,)+
                    // We have to load an empty string now, it'll get filled in later
                    oid => PgUnknownType { name: String::new(), oid: oid }
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
    #[doc="NAME"]
    NAMEOID => PgName,
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
    #[doc="NAME[]"]
    NAMEARRAYOID => PgNameArray member PgName,
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

macro_rules! check_types(
    ($($expected:pat)|+, $actual:ident) => (
        match $actual {
            $(&$expected)|+ => {}
            actual => return Err(PgWrongType(actual.clone()))
        }
    )
)

/// A trait for types that can be created from a Postgres value
pub trait FromSql {
    /// Creates a new value of this type from a buffer of Postgres data.
    ///
    /// If the value was `NULL`, the buffer will be `None`.
    fn from_sql(ty: &PostgresType, raw: &Option<Vec<u8>>) -> Result<Self>;
}

#[doc(hidden)]
trait RawFromSql {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<Self>;
}

macro_rules! raw_from_impl(
    ($t:ty, $f:ident) => (
        impl RawFromSql for $t {
            fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<$t> {
                Ok(try_pg!(raw.$f()))
            }
        }
    )
)

impl RawFromSql for bool {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<bool> {
        Ok((try_pg!(raw.read_u8())) != 0)
    }
}

impl RawFromSql for Vec<u8> {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<Vec<u8>> {
        Ok(try_pg!(raw.read_to_end()))
    }
}

impl RawFromSql for String {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<String> {
        String::from_utf8(try_pg!(raw.read_to_end())).map_err(|_| PgBadData)
    }
}

raw_from_impl!(i8, read_i8)
raw_from_impl!(i16, read_be_i16)
raw_from_impl!(i32, read_be_i32)
raw_from_impl!(i64, read_be_i64)
raw_from_impl!(f32, read_be_f32)
raw_from_impl!(f64, read_be_f64)

impl RawFromSql for Timespec {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<Timespec> {
        let t = try_pg!(raw.read_be_i64());
        let mut sec = t / USEC_PER_SEC + TIME_SEC_CONVERSION;
        let mut usec = t % USEC_PER_SEC;

        if usec < 0 {
            sec -= 1;
            usec = USEC_PER_SEC + usec;
        }

        Ok(Timespec::new(sec, (usec * NSEC_PER_USEC) as i32))
    }
}

macro_rules! from_range_impl(
    ($t:ty) => (
        impl RawFromSql for Range<$t> {
            fn raw_from_sql<R: Reader>(rdr: &mut R) -> Result<Range<$t>> {
                let t = try_pg!(rdr.read_i8());

                if t & RANGE_EMPTY != 0 {
                    Ok(Range::empty())
                } else {
                    let lower = match t & RANGE_LOWER_UNBOUNDED {
                        0 => {
                            let type_ = match t & RANGE_LOWER_INCLUSIVE {
                                0 => Exclusive,
                                _ => Inclusive
                            };
                            let len = try_pg!(rdr.read_be_i32()) as uint;
                            let mut limit = LimitReader::new(rdr.by_ref(), len);
                            let lower = try!(RawFromSql::raw_from_sql(&mut limit));
                            let lower = Some(RangeBound::new(lower, type_));
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
                            let len = try_pg!(rdr.read_be_i32()) as uint;
                            let mut limit = LimitReader::new(rdr.by_ref(), len);
                            let upper = try!(RawFromSql::raw_from_sql(&mut limit));
                            let upper = Some(RangeBound::new(upper, type_));
                            assert!(limit.limit() == 0);
                            upper
                        }
                        _ => None
                    };

                    Ok(Range::new(lower, upper))
                }
            }
        }
    )
)

from_range_impl!(i32)
from_range_impl!(i64)
from_range_impl!(Timespec)

impl RawFromSql for Json {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<Json> {
        json::from_reader(raw).map_err(|_| PgBadData)
    }
}

macro_rules! from_map_impl(
    ($($expected:pat)|+, $t:ty, $blk:expr) => (
        impl FromSql for Option<$t> {
            fn from_sql(ty: &PostgresType, raw: &Option<Vec<u8>>)
                    -> Result<Option<$t>> {
                check_types!($($expected)|+, ty)
                match *raw {
                    Some(ref buf) => ($blk)(buf).map(|ok| Some(ok)),
                    None => Ok(None)
                }
            }
        }

        impl FromSql for $t {
            fn from_sql(ty: &PostgresType, raw: &Option<Vec<u8>>)
                    -> Result<$t> {
                // FIXME when you can specify Self types properly
                let ret: Result<Option<$t>> = FromSql::from_sql(ty, raw);
                match ret {
                    Ok(Some(val)) => Ok(val),
                    Ok(None) => Err(PgWasNull),
                    Err(err) => Err(err)
                }
            }
        }
    )
)

macro_rules! from_raw_from_impl(
    ($($expected:pat)|+, $t:ty) => (
        from_map_impl!($($expected)|+, $t, |buf: &Vec<u8>| {
            let mut reader = BufReader::new(buf[]);
            RawFromSql::raw_from_sql(&mut reader)
        })
    )
)

from_raw_from_impl!(PgBool, bool)
from_raw_from_impl!(PgByteA, Vec<u8>)
from_raw_from_impl!(PgVarchar | PgText | PgCharN | PgName, String)
from_raw_from_impl!(PgChar, i8)
from_raw_from_impl!(PgInt2, i16)
from_raw_from_impl!(PgInt4, i32)
from_raw_from_impl!(PgInt8, i64)
from_raw_from_impl!(PgFloat4, f32)
from_raw_from_impl!(PgFloat8, f64)
from_raw_from_impl!(PgJson, Json)

from_raw_from_impl!(PgTimestamp | PgTimestampTZ, Timespec)
from_raw_from_impl!(PgInt4Range, Range<i32>)
from_raw_from_impl!(PgInt8Range, Range<i64>)
from_raw_from_impl!(PgTsRange | PgTstzRange, Range<Timespec>)

macro_rules! from_array_impl(
    ($($oid:ident)|+, $t:ty) => (
        from_map_impl!($($oid)|+, ArrayBase<Option<$t>>, |buf: &Vec<u8>| {
            let mut rdr = BufReader::new(buf[]);

            let ndim = try_pg!(rdr.read_be_i32()) as uint;
            let _has_null = try_pg!(rdr.read_be_i32()) == 1;
            let _element_type: Oid = try_pg!(rdr.read_be_u32());

            let mut dim_info = Vec::with_capacity(ndim);
            for _ in range(0, ndim) {
                dim_info.push(DimensionInfo {
                    len: try_pg!(rdr.read_be_i32()) as uint,
                    lower_bound: try_pg!(rdr.read_be_i32()) as int
                });
            }
            let nele = dim_info.iter().fold(1, |acc, info| acc * info.len);

            let mut elements = Vec::with_capacity(nele);
            for _ in range(0, nele) {
                let len = try_pg!(rdr.read_be_i32());
                if len < 0 {
                    elements.push(None);
                } else {
                    let mut limit = LimitReader::new(rdr.by_ref(), len as uint);
                    elements.push(Some(try!(RawFromSql::raw_from_sql(&mut limit))));
                    assert!(limit.limit() == 0);
                }
            }

            Ok(ArrayBase::from_raw(elements, dim_info))
        })
    )
)

from_array_impl!(PgBoolArray, bool)
from_array_impl!(PgByteAArray, Vec<u8>)
from_array_impl!(PgCharArray, i8)
from_array_impl!(PgInt2Array, i16)
from_array_impl!(PgInt4Array, i32)
from_array_impl!(PgTextArray | PgCharNArray | PgVarcharArray | PgNameArray, String)
from_array_impl!(PgInt8Array, i64)
from_array_impl!(PgTimestampArray | PgTimestampTZArray, Timespec)
from_array_impl!(PgJsonArray, Json)
from_array_impl!(PgFloat4Array, f32)
from_array_impl!(PgFloat8Array, f64)
from_array_impl!(PgInt4RangeArray, Range<i32>)
from_array_impl!(PgTsRangeArray | PgTstzRangeArray, Range<Timespec>)
from_array_impl!(PgInt8RangeArray, Range<i64>)

impl FromSql for Option<HashMap<String, Option<String>>> {
    fn from_sql(ty: &PostgresType, raw: &Option<Vec<u8>>)
                -> Result<Option<HashMap<String, Option<String>>>> {
        match *ty {
            PgUnknownType { ref name, .. } if "hstore" == name[] => {}
            _ => return Err(PgWrongType(ty.clone()))
        }

        match *raw {
            Some(ref buf) => {
                let mut rdr = BufReader::new(buf[]);
                let mut map = HashMap::new();

                let count = try_pg!(rdr.read_be_i32());

                for _ in range(0, count) {
                    let key_len = try_pg!(rdr.read_be_i32());
                    let key = try_pg!(rdr.read_exact(key_len as uint));
                    let key = match String::from_utf8(key) {
                        Ok(key) => key,
                        Err(_) => return Err(PgBadData),
                    };

                    let val_len = try_pg!(rdr.read_be_i32());
                    let val = if val_len < 0 {
                        None
                    } else {
                        let val = try_pg!(rdr.read_exact(val_len as uint));
                        match String::from_utf8(val) {
                            Ok(val) => Some(val),
                            Err(_) => return Err(PgBadData),
                        }
                    };

                    map.insert(key, val);
                }
                Ok(Some(map))
            }
            None => Ok(None)
        }
    }
}

impl FromSql for HashMap<String, Option<String>> {
    fn from_sql(ty: &PostgresType, raw: &Option<Vec<u8>>)
                -> Result<HashMap<String, Option<String>>> {
        // FIXME when you can specify Self types properly
        let ret: Result<Option<HashMap<String, Option<String>>>> =
            FromSql::from_sql(ty, raw);
        match ret {
            Ok(Some(val)) => Ok(val),
            Ok(None) => Err(PgWasNull),
            Err(err) => Err(err)
        }
    }
}

/// A trait for types that can be converted into Postgres values
pub trait ToSql {
    /// Converts the value of `self` into the binary format appropriate for the
    /// Postgres backend.
    fn to_sql(&self, ty: &PostgresType) -> Result<Option<Vec<u8>>>;
}

#[doc(hidden)]
trait RawToSql {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()>;
}

macro_rules! raw_to_impl(
    ($t:ty, $f:ident) => (
        impl RawToSql for $t {
            fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()> {
                Ok(try_pg!(w.$f(*self)))
            }
        }
    )
)

impl RawToSql for bool {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()> {
        Ok(try_pg!(w.write_u8(*self as u8)))
    }
}

impl RawToSql for Vec<u8> {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()> {
        Ok(try_pg!(w.write(self[])))
    }
}

impl RawToSql for String {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()> {
        Ok(try_pg!(w.write(self.as_bytes())))
    }
}

raw_to_impl!(i8, write_i8)
raw_to_impl!(i16, write_be_i16)
raw_to_impl!(i32, write_be_i32)
raw_to_impl!(i64, write_be_i64)
raw_to_impl!(f32, write_be_f32)
raw_to_impl!(f64, write_be_f64)

impl RawToSql for Timespec {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()> {
        let t = (self.sec - TIME_SEC_CONVERSION) * USEC_PER_SEC + self.nsec as i64 / NSEC_PER_USEC;
        Ok(try_pg!(w.write_be_i64(t)))
    }
}

macro_rules! to_range_impl(
    ($t:ty) => (
        impl RawToSql for Range<$t> {
            fn raw_to_sql<W: Writer>(&self, buf: &mut W) -> Result<()> {
                let mut tag = 0;
                if self.is_empty() {
                    tag |= RANGE_EMPTY;
                } else {
                    match self.lower() {
                        None => tag |= RANGE_LOWER_UNBOUNDED,
                        Some(&RangeBound { type_: Inclusive, .. }) => tag |= RANGE_LOWER_INCLUSIVE,
                        _ => {}
                    }
                    match self.upper() {
                        None => tag |= RANGE_UPPER_UNBOUNDED,
                        Some(&RangeBound { type_: Inclusive, .. }) => tag |= RANGE_UPPER_INCLUSIVE,
                        _ => {}
                    }
                }

                try_pg!(buf.write_i8(tag));

                match self.lower() {
                    Some(bound) => {
                        let mut inner_buf = MemWriter::new();
                        try!(bound.value.raw_to_sql(&mut inner_buf));
                        let inner_buf = inner_buf.unwrap();
                        try_pg!(buf.write_be_i32(inner_buf.len() as i32));
                        try_pg!(buf.write(inner_buf[]));
                    }
                    None => {}
                }
                match self.upper() {
                    Some(bound) => {
                        let mut inner_buf = MemWriter::new();
                        try!(bound.value.raw_to_sql(&mut inner_buf));
                        let inner_buf = inner_buf.unwrap();
                        try_pg!(buf.write_be_i32(inner_buf.len() as i32));
                        try_pg!(buf.write(inner_buf[]));
                    }
                    None => {}
                }

                Ok(())
            }
        }
    )
)

to_range_impl!(i32)
to_range_impl!(i64)
to_range_impl!(Timespec)

impl RawToSql for Json {
    fn raw_to_sql<W: Writer>(&self, raw: &mut W) -> Result<()> {
        Ok(try_pg!(self.to_writer(raw as &mut Writer)))
    }
}

macro_rules! to_option_impl(
    ($($oid:pat)|+, $t:ty) => (
        impl ToSql for Option<$t> {
            fn to_sql(&self, ty: &PostgresType) -> Result<Option<Vec<u8>>> {
                check_types!($($oid)|+, ty)

                match *self {
                    None => Ok(None),
                    Some(ref val) => val.to_sql(ty)
                }
            }
        }
    )
)

macro_rules! to_option_impl_lifetime(
    ($($oid:pat)|+, $t:ty) => (
        impl<'a> ToSql for Option<$t> {
            fn to_sql(&self, ty: &PostgresType) -> Result<Option<Vec<u8>>> {
                check_types!($($oid)|+, ty)

                match *self {
                    None => Ok(None),
                    Some(ref val) => val.to_sql(ty)
                }
            }
        }
    )
)

macro_rules! to_raw_to_impl(
    ($($oid:ident)|+, $t:ty) => (
        impl ToSql for $t {
            fn to_sql(&self, ty: &PostgresType) -> Result<Option<Vec<u8>>> {
                check_types!($($oid)|+, ty)

                let mut writer = MemWriter::new();
                try!(self.raw_to_sql(&mut writer));
                Ok(Some(writer.unwrap()))
            }
        }

        to_option_impl!($($oid)|+, $t)
    )
)

to_raw_to_impl!(PgBool, bool)
to_raw_to_impl!(PgByteA, Vec<u8>)
to_raw_to_impl!(PgVarchar | PgText | PgCharN | PgName, String)
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
    fn to_sql(&self, ty: &PostgresType) -> Result<Option<Vec<u8>>> {
        check_types!(PgVarchar | PgText | PgCharN | PgName, ty)
        Ok(Some(self.as_bytes().to_vec()))
    }
}

to_option_impl_lifetime!(PgVarchar | PgText | PgCharN | PgName, &'a str)

impl<'a> ToSql for &'a [u8] {
    fn to_sql(&self, ty: &PostgresType) -> Result<Option<Vec<u8>>> {
        check_types!(PgByteA, ty)
        Ok(Some(self.to_vec()))
    }
}

to_option_impl_lifetime!(PgByteA, &'a [u8])

to_raw_to_impl!(PgTimestamp | PgTimestampTZ, Timespec)

macro_rules! to_array_impl(
    ($($oid:ident)|+, $t:ty) => (
        impl ToSql for ArrayBase<Option<$t>> {
            fn to_sql(&self, ty: &PostgresType) -> Result<Option<Vec<u8>>> {
                check_types!($($oid)|+, ty)
                let mut buf = MemWriter::new();

                try_pg!(buf.write_be_i32(self.dimension_info().len() as i32));
                try_pg!(buf.write_be_i32(1));
                try_pg!(buf.write_be_u32(ty.member_type().to_oid()));

                for info in self.dimension_info().iter() {
                    try_pg!(buf.write_be_i32(info.len as i32));
                    try_pg!(buf.write_be_i32(info.lower_bound as i32));
                }

                for v in self.values() {
                    match *v {
                        Some(ref val) => {
                            let mut inner_buf = MemWriter::new();
                            try!(val.raw_to_sql(&mut inner_buf));
                            let inner_buf = inner_buf.unwrap();
                            try_pg!(buf.write_be_i32(inner_buf.len() as i32));
                            try_pg!(buf.write(inner_buf[]));
                        }
                        None => try_pg!(buf.write_be_i32(-1))
                    }
                }

                Ok(Some(buf.unwrap()))
            }
        }

        to_option_impl!($($oid)|+, ArrayBase<Option<$t>>)
    )
)

to_array_impl!(PgBoolArray, bool)
to_array_impl!(PgByteAArray, Vec<u8>)
to_array_impl!(PgCharArray, i8)
to_array_impl!(PgInt2Array, i16)
to_array_impl!(PgInt4Array, i32)
to_array_impl!(PgInt8Array, i64)
to_array_impl!(PgTextArray | PgCharNArray | PgVarcharArray | PgNameArray, String)
to_array_impl!(PgTimestampArray | PgTimestampTZArray, Timespec)
to_array_impl!(PgFloat4Array, f32)
to_array_impl!(PgFloat8Array, f64)
to_array_impl!(PgInt4RangeArray, Range<i32>)
to_array_impl!(PgTsRangeArray | PgTstzRangeArray, Range<Timespec>)
to_array_impl!(PgInt8RangeArray, Range<i64>)
to_array_impl!(PgJsonArray, Json)

impl ToSql for HashMap<String, Option<String>> {
    fn to_sql(&self, ty: &PostgresType) -> Result<Option<Vec<u8>>> {
        match *ty {
            PgUnknownType { ref name, .. } if "hstore" == name[] => {}
            _ => return Err(PgWrongType(ty.clone()))
        }

        let mut buf = MemWriter::new();

        try_pg!(buf.write_be_i32(self.len() as i32));

        for (key, val) in self.iter() {
            try_pg!(buf.write_be_i32(key.len() as i32));
            try_pg!(buf.write(key.as_bytes()));

            match *val {
                Some(ref val) => {
                    try_pg!(buf.write_be_i32(val.len() as i32));
                    try_pg!(buf.write(val.as_bytes()));
                }
                None => try_pg!(buf.write_be_i32(-1))
            }
        }

        Ok(Some(buf.unwrap()))
    }
}

impl ToSql for Option<HashMap<String, Option<String>>> {
    fn to_sql(&self, ty: &PostgresType) -> Result<Option<Vec<u8>>> {
        match *ty {
            PgUnknownType { ref name, .. } if "hstore" == name[] => {}
            _ => return Err(PgWrongType(ty.clone()))
        }

        match *self {
            Some(ref inner) => inner.to_sql(ty),
            None => Ok(None)
        }
    }
}

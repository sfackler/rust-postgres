//! Traits dealing with Postgres data types
use serialize::json;
use std::collections::HashMap;
use std::io::{ByRefReader, BufReader};

use self::Type::*;
use Result;
use error::Error;
use types::range::Range;
use types::range::BoundType::{Inclusive, Exclusive};

macro_rules! check_types(
    ($($expected:pat)|+, $actual:ident) => (
        match $actual {
            $(&$expected)|+ => {}
            actual => return Err(::Error::WrongType(actual.clone()))
        }
    )
)

macro_rules! raw_from_impl(
    ($t:ty, $f:ident) => (
        impl RawFromSql for $t {
            fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<$t> {
                Ok(try!(raw.$f()))
            }
        }
    )
)

macro_rules! from_range_impl(
    ($t:ty) => (
        impl ::types::RawFromSql for ::types::range::Range<$t> {
            fn raw_from_sql<R: Reader>(rdr: &mut R) -> Result<Range<$t>> {
                use std::io::ByRefReader;
                use std::io::util::LimitReader;
                use types::{RANGE_EMPTY, RANGE_LOWER_UNBOUNDED, RANGE_LOWER_INCLUSIVE,
                            RANGE_UPPER_UNBOUNDED, RANGE_UPPER_INCLUSIVE};
                use types::range::{BoundType, Range, RangeBound, BoundSided};
                use Error;

                let t = try!(rdr.read_i8());

                if t & RANGE_EMPTY != 0 {
                    return Ok(Range::empty());
                }

                fn make_bound<T, R>(rdr: &mut R, tag: i8, bound_flag: i8, inclusive_flag: i8)
                        -> Result<Option<RangeBound<T, $t>>> where T: BoundSided, R: Reader {
                    match tag & bound_flag {
                        0 => {
                            let type_ = match tag & inclusive_flag {
                                0 => BoundType::Exclusive,
                                _ => BoundType::Inclusive,
                            };
                            let len = try!(rdr.read_be_i32()) as uint;
                            let mut limit = LimitReader::new(rdr.by_ref(), len);
                            let bound = try!(RawFromSql::raw_from_sql(&mut limit));
                            if limit.limit() != 0 {
                                return Err(Error::BadData);
                            }
                            Ok(Some(RangeBound::new(bound, type_)))
                        }
                        _ => Ok(None)
                    }
                }

                let lower = try!(make_bound(rdr, t, RANGE_LOWER_UNBOUNDED, RANGE_LOWER_INCLUSIVE));
                let upper = try!(make_bound(rdr, t, RANGE_UPPER_UNBOUNDED, RANGE_UPPER_INCLUSIVE));
                Ok(Range::new(lower, upper))
            }
        }
    )
)

macro_rules! from_map_impl(
    ($($expected:pat)|+, $t:ty, $blk:expr $(, $a:meta)*) => (
        $(#[$a])*
        impl ::types::FromSql for Option<$t> {
            fn from_sql(ty: &Type, raw: &Option<Vec<u8>>) -> Result<Option<$t>> {
                check_types!($($expected)|+, ty)
                match *raw {
                    Some(ref buf) => ($blk)(buf).map(|ok| Some(ok)),
                    None => Ok(None)
                }
            }
        }

        $(#[$a])*
        impl ::types::FromSql for $t {
            fn from_sql(ty: &Type, raw: &Option<Vec<u8>>) -> Result<$t> {
                use Error;
                use types::FromSql;

                // FIXME when you can specify Self types properly
                let ret: Result<Option<$t>> = FromSql::from_sql(ty, raw);
                match ret {
                    Ok(Some(val)) => Ok(val),
                    Ok(None) => Err(Error::WasNull),
                    Err(err) => Err(err)
                }
            }
        }
    )
)

macro_rules! from_raw_from_impl(
    ($($expected:pat)|+, $t:ty $(, $a:meta)*) => (
        from_map_impl!($($expected)|+, $t, |buf: &Vec<u8>| {
            use std::io::BufReader;
            use types::RawFromSql;

            let mut reader = BufReader::new(&**buf);
            RawFromSql::raw_from_sql(&mut reader)
        } $(, $a)*)
    )
)

macro_rules! from_array_impl(
    ($($oid:pat)|+, $t:ty $(, $a:meta)*) => (
        from_map_impl!($($oid)|+, ::types::array::ArrayBase<Option<$t>>, |buf: &Vec<u8>| {
            use std::io::{BufReader, ByRefReader};
            use std::io::util::LimitReader;
            use std::iter::MultiplicativeIterator;
            use types::{Oid, RawFromSql};
            use types::array::{ArrayBase, DimensionInfo};
            use Error;

            let mut rdr = BufReader::new(&**buf);

            let ndim = try!(rdr.read_be_i32()) as uint;
            let _has_null = try!(rdr.read_be_i32()) == 1;
            let _element_type: Oid = try!(rdr.read_be_u32());

            let mut dim_info = Vec::with_capacity(ndim);
            for _ in range(0, ndim) {
                dim_info.push(DimensionInfo {
                    len: try!(rdr.read_be_i32()) as uint,
                    lower_bound: try!(rdr.read_be_i32()) as int
                });
            }
            let nele = dim_info.iter().map(|info| info.len).product();

            let mut elements = Vec::with_capacity(nele);
            for _ in range(0, nele) {
                let len = try!(rdr.read_be_i32());
                if len < 0 {
                    elements.push(None);
                } else {
                    let mut limit = LimitReader::new(rdr.by_ref(), len as uint);
                    elements.push(Some(try!(RawFromSql::raw_from_sql(&mut limit))));
                    if limit.limit() != 0 {
                        return Err(Error::BadData);
                    }
                }
            }

            Ok(ArrayBase::from_raw(elements, dim_info))
        } $(, $a)*)
    )
)

macro_rules! raw_to_impl(
    ($t:ty, $f:ident) => (
        impl RawToSql for $t {
            fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()> {
                Ok(try!(w.$f(*self)))
            }
        }
    )
)

macro_rules! to_range_impl(
    ($t:ty) => (
        impl ::types::RawToSql for ::types::range::Range<$t> {
            fn raw_to_sql<W: Writer>(&self, buf: &mut W) -> Result<()> {
                use types::{RANGE_EMPTY, RANGE_LOWER_UNBOUNDED, RANGE_LOWER_INCLUSIVE,
                            RANGE_UPPER_UNBOUNDED, RANGE_UPPER_INCLUSIVE};
                use types::range::{BoundType, RangeBound, BoundSided};

                let mut tag = 0;
                if self.is_empty() {
                    tag |= RANGE_EMPTY;
                } else {
                    fn make_tag<T>(bound: Option<&RangeBound<T, $t>>, unbounded_tag: i8,
                                   inclusive_tag: i8) -> i8 where T: BoundSided {
                        match bound {
                            None => unbounded_tag,
                            Some(&RangeBound { type_: BoundType::Inclusive, .. }) => inclusive_tag,
                            _ => 0
                        }
                    }
                    tag |= make_tag(self.lower(), RANGE_LOWER_UNBOUNDED, RANGE_LOWER_INCLUSIVE);
                    tag |= make_tag(self.upper(), RANGE_UPPER_UNBOUNDED, RANGE_UPPER_INCLUSIVE);
                }

                try!(buf.write_i8(tag));

                fn write_value<T, W>(buf: &mut W, v: Option<&RangeBound<T, $t>>) -> Result<()>
                        where T: BoundSided, W: Writer {
                    if let Some(bound) = v {
                        let mut inner_buf = vec![];
                        try!(bound.value.raw_to_sql(&mut inner_buf));
                        try!(buf.write_be_u32(inner_buf.len() as u32));
                        try!(buf.write(&*inner_buf));
                    }
                    Ok(())
                }

                try!(write_value(buf, self.lower()));
                try!(write_value(buf, self.upper()));

                Ok(())
            }
        }
    )
)

macro_rules! to_option_impl(
    ($($oid:pat)|+, $t:ty $(,$a:meta)*) => (
        $(#[$a])*
        impl ::types::ToSql for Option<$t> {
            fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
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
            fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
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
    ($($oid:pat)|+, $t:ty $(, $a:meta)*) => (
        $(#[$a])*
        impl ::types::ToSql for $t {
            fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
                check_types!($($oid)|+, ty)

                let mut writer = vec![];
                try!(self.raw_to_sql(&mut writer));
                Ok(Some(writer))
            }
        }

        to_option_impl!($($oid)|+, $t $(, $a)*)
    )
)

macro_rules! to_array_impl(
    ($($oid:pat)|+, $t:ty $(, $a:meta)*) => (
        $(#[$a])*
        impl ::types::ToSql for ::types::array::ArrayBase<Option<$t>> {
            fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
                use types::array::Array;

                check_types!($($oid)|+, ty)
                let mut buf = vec![];

                try!(buf.write_be_i32(self.dimension_info().len() as i32));
                try!(buf.write_be_i32(1));
                try!(buf.write_be_u32(ty.member_type().to_oid()));

                for info in self.dimension_info().iter() {
                    try!(buf.write_be_i32(info.len as i32));
                    try!(buf.write_be_i32(info.lower_bound as i32));
                }

                for v in self.values() {
                    match *v {
                        Some(ref val) => {
                            let mut inner_buf = vec![];
                            try!(val.raw_to_sql(&mut inner_buf));
                            try!(buf.write_be_i32(inner_buf.len() as i32));
                            try!(buf.write(&*inner_buf));
                        }
                        None => try!(buf.write_be_i32(-1))
                    }
                }

                Ok(Some(buf))
            }
        }

        to_option_impl!($($oid)|+, ::types::array::ArrayBase<Option<$t>> $(, $a)*)
    )
)

pub mod array;
pub mod range;
#[cfg(feature = "uuid")]
mod uuid;
mod time;

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
const UUIDOID: Oid = 2950;
const UUIDARRAYOID: Oid = 2951;
const INT4RANGEOID: Oid = 3904;
const INT4RANGEARRAYOID: Oid = 3905;
const TSRANGEOID: Oid = 3908;
const TSRANGEARRAYOID: Oid = 3909;
const TSTZRANGEOID: Oid = 3910;
const TSTZRANGEARRAYOID: Oid = 3911;
const INT8RANGEOID: Oid = 3926;
const INT8RANGEARRAYOID: Oid = 3927;

const RANGE_UPPER_UNBOUNDED: i8 = 0b0001_0000;
const RANGE_LOWER_UNBOUNDED: i8 = 0b0000_1000;
const RANGE_UPPER_INCLUSIVE: i8 = 0b0000_0100;
const RANGE_LOWER_INCLUSIVE: i8 = 0b0000_0010;
const RANGE_EMPTY: i8           = 0b0000_0001;

macro_rules! make_postgres_type(
    ($(#[$doc:meta] $oid:ident => $variant:ident $(member $member:ident)*),+) => (
        /// A Postgres type
        #[deriving(PartialEq, Eq, Clone, Show)]
        pub enum Type {
            $(
                #[$doc]
                $variant,
            )+
            /// An unknown type
            Unknown {
                /// The name of the type
                name: String,
                /// The OID of the type
                oid: Oid
            }
        }

        impl Type {
            #[doc(hidden)]
            pub fn from_oid(oid: Oid) -> Type {
                match oid {
                    $($oid => Type::$variant,)+
                    // We have to load an empty string now, it'll get filled in later
                    oid => Type::Unknown { name: String::new(), oid: oid }
                }
            }

            #[doc(hidden)]
            pub fn to_oid(&self) -> Oid {
                match *self {
                    $(Type::$variant => $oid,)+
                    Type::Unknown { oid, .. } => oid
                }
            }

            fn member_type(&self) -> Type {
                match *self {
                    $(
                        $(Type::$variant => Type::$member,)*
                    )+
                    _ => unreachable!()
                }
            }
        }
    )
)

make_postgres_type!(
    #[doc="BOOL"]
    BOOLOID => Bool,
    #[doc="BYTEA"]
    BYTEAOID => ByteA,
    #[doc="\"char\""]
    CHAROID => Char,
    #[doc="NAME"]
    NAMEOID => Name,
    #[doc="INT8/BIGINT"]
    INT8OID => Int8,
    #[doc="INT2/SMALLINT"]
    INT2OID => Int2,
    #[doc="INT4/INT"]
    INT4OID => Int4,
    #[doc="TEXT"]
    TEXTOID => Text,
    #[doc="JSON"]
    JSONOID => Json,
    #[doc="JSON[]"]
    JSONARRAYOID => JsonArray member Json,
    #[doc="FLOAT4/REAL"]
    FLOAT4OID => Float4,
    #[doc="FLOAT8/DOUBLE PRECISION"]
    FLOAT8OID => Float8,
    #[doc="BOOL[]"]
    BOOLARRAYOID => BoolArray member Bool,
    #[doc="BYTEA[]"]
    BYTEAARRAYOID => ByteAArray member ByteA,
    #[doc="\"char\"[]"]
    CHARARRAYOID => CharArray member Char,
    #[doc="NAME[]"]
    NAMEARRAYOID => NameArray member Name,
    #[doc="INT2[]"]
    INT2ARRAYOID => Int2Array member Int2,
    #[doc="INT4[]"]
    INT4ARRAYOID => Int4Array member Int4,
    #[doc="TEXT[]"]
    TEXTARRAYOID => TextArray member Text,
    #[doc="CHAR(n)[]"]
    BPCHARARRAYOID => CharNArray member CharN,
    #[doc="VARCHAR[]"]
    VARCHARARRAYOID => VarcharArray member Varchar,
    #[doc="INT8[]"]
    INT8ARRAYOID => Int8Array member Int8,
    #[doc="FLOAT4[]"]
    FLOAT4ARRAYOID => Float4Array member Float4,
    #[doc="FLOAT8[]"]
    FLAOT8ARRAYOID => Float8Array member Float8,
    #[doc="TIMESTAMP"]
    TIMESTAMPOID => Timestamp,
    #[doc="TIMESTAMP[]"]
    TIMESTAMPARRAYOID => TimestampArray member Timestamp,
    #[doc="TIMESTAMP WITH TIME ZONE"]
    TIMESTAMPZOID => TimestampTZ,
    #[doc="TIMESTAMP WITH TIME ZONE[]"]
    TIMESTAMPZARRAYOID => TimestampTZArray member TimestampTZ,
    #[doc="UUID"]
    UUIDOID => Uuid,
    #[doc="UUID[]"]
    UUIDARRAYOID => UuidArray member Uuid,
    #[doc="CHAR(n)/CHARACTER(n)"]
    BPCHAROID => CharN,
    #[doc="VARCHAR/CHARACTER VARYING"]
    VARCHAROID => Varchar,
    #[doc="INT4RANGE"]
    INT4RANGEOID => Int4Range,
    #[doc="INT4RANGE[]"]
    INT4RANGEARRAYOID => Int4RangeArray member Int4Range,
    #[doc="TSRANGE"]
    TSRANGEOID => TsRange,
    #[doc="TSRANGE[]"]
    TSRANGEARRAYOID => TsRangeArray member TsRange,
    #[doc="TSTZRANGE"]
    TSTZRANGEOID => TstzRange,
    #[doc="TSTZRANGE[]"]
    TSTZRANGEARRAYOID => TstzRangeArray member TstzRange,
    #[doc="INT8RANGE"]
    INT8RANGEOID => Int8Range,
    #[doc="INT8RANGE[]"]
    INT8RANGEARRAYOID => Int8RangeArray member Int8Range
)

/// A trait for types that can be created from a Postgres value
pub trait FromSql {
    /// Creates a new value of this type from a buffer of Postgres data.
    ///
    /// If the value was `NULL`, the buffer will be `None`.
    fn from_sql(ty: &Type, raw: &Option<Vec<u8>>) -> Result<Self>;
}

#[doc(hidden)]
trait RawFromSql {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<Self>;
}

impl RawFromSql for bool {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<bool> {
        Ok((try!(raw.read_u8())) != 0)
    }
}

impl RawFromSql for Vec<u8> {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<Vec<u8>> {
        Ok(try!(raw.read_to_end()))
    }
}

impl RawFromSql for String {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<String> {
        String::from_utf8(try!(raw.read_to_end())).map_err(|_| Error::BadData)
    }
}

raw_from_impl!(i8, read_i8)
raw_from_impl!(i16, read_be_i16)
raw_from_impl!(i32, read_be_i32)
raw_from_impl!(i64, read_be_i64)
raw_from_impl!(f32, read_be_f32)
raw_from_impl!(f64, read_be_f64)

from_range_impl!(i32)
from_range_impl!(i64)

impl RawFromSql for json::Json {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<json::Json> {
        json::from_reader(raw).map_err(|_| Error::BadData)
    }
}

from_raw_from_impl!(Bool, bool)
from_raw_from_impl!(ByteA, Vec<u8>)
from_raw_from_impl!(Varchar | Text | CharN | Name, String)
from_raw_from_impl!(Char, i8)
from_raw_from_impl!(Int2, i16)
from_raw_from_impl!(Int4, i32)
from_raw_from_impl!(Int8, i64)
from_raw_from_impl!(Float4, f32)
from_raw_from_impl!(Float8, f64)
from_raw_from_impl!(Json, json::Json)

from_raw_from_impl!(Int4Range, Range<i32>)
from_raw_from_impl!(Int8Range, Range<i64>)

from_array_impl!(BoolArray, bool)
from_array_impl!(ByteAArray, Vec<u8>)
from_array_impl!(CharArray, i8)
from_array_impl!(Int2Array, i16)
from_array_impl!(Int4Array, i32)
from_array_impl!(TextArray | CharNArray | VarcharArray | NameArray, String)
from_array_impl!(Int8Array, i64)
from_array_impl!(JsonArray, json::Json)
from_array_impl!(Float4Array, f32)
from_array_impl!(Float8Array, f64)
from_array_impl!(Int4RangeArray, Range<i32>)
from_array_impl!(Int8RangeArray, Range<i64>)

impl FromSql for Option<HashMap<String, Option<String>>> {
    fn from_sql(ty: &Type, raw: &Option<Vec<u8>>)
                -> Result<Option<HashMap<String, Option<String>>>> {
        match *ty {
            Type::Unknown { ref name, .. } if "hstore" == *name => {}
            _ => return Err(Error::WrongType(ty.clone()))
        }

        match *raw {
            Some(ref buf) => {
                let mut rdr = BufReader::new(&**buf);
                let mut map = HashMap::new();

                let count = try!(rdr.read_be_i32());

                for _ in range(0, count) {
                    let key_len = try!(rdr.read_be_i32());
                    let key = try!(rdr.read_exact(key_len as uint));
                    let key = match String::from_utf8(key) {
                        Ok(key) => key,
                        Err(_) => return Err(Error::BadData),
                    };

                    let val_len = try!(rdr.read_be_i32());
                    let val = if val_len < 0 {
                        None
                    } else {
                        let val = try!(rdr.read_exact(val_len as uint));
                        match String::from_utf8(val) {
                            Ok(val) => Some(val),
                            Err(_) => return Err(Error::BadData),
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
    fn from_sql(ty: &Type, raw: &Option<Vec<u8>>)
                -> Result<HashMap<String, Option<String>>> {
        // FIXME when you can specify Self types properly
        let ret: Result<Option<HashMap<String, Option<String>>>> =
            FromSql::from_sql(ty, raw);
        match ret {
            Ok(Some(val)) => Ok(val),
            Ok(None) => Err(Error::WasNull),
            Err(err) => Err(err)
        }
    }
}

/// A trait for types that can be converted into Postgres values
pub trait ToSql {
    /// Converts the value of `self` into the binary format appropriate for the
    /// Postgres backend.
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>>;
}

#[doc(hidden)]
trait RawToSql {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()>;
}

impl RawToSql for bool {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()> {
        Ok(try!(w.write_u8(*self as u8)))
    }
}

impl RawToSql for Vec<u8> {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()> {
        Ok(try!(w.write(&**self)))
    }
}

impl RawToSql for String {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()> {
        Ok(try!(w.write(self.as_bytes())))
    }
}

raw_to_impl!(i8, write_i8)
raw_to_impl!(i16, write_be_i16)
raw_to_impl!(i32, write_be_i32)
raw_to_impl!(i64, write_be_i64)
raw_to_impl!(f32, write_be_f32)
raw_to_impl!(f64, write_be_f64)

to_range_impl!(i32)
to_range_impl!(i64)

impl RawToSql for json::Json {
    fn raw_to_sql<W: Writer>(&self, raw: &mut W) -> Result<()> {
        Ok(try!(self.to_writer(raw as &mut Writer)))
    }
}

to_raw_to_impl!(Bool, bool)
to_raw_to_impl!(ByteA, Vec<u8>)
to_raw_to_impl!(Varchar | Text | CharN | Name, String)
to_raw_to_impl!(Json, json::Json)
to_raw_to_impl!(Char, i8)
to_raw_to_impl!(Int2, i16)
to_raw_to_impl!(Int4, i32)
to_raw_to_impl!(Int8, i64)
to_raw_to_impl!(Float4, f32)
to_raw_to_impl!(Float8, f64)
to_raw_to_impl!(Int4Range, Range<i32>)
to_raw_to_impl!(Int8Range, Range<i64>)

impl<'a> ToSql for &'a str {
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
        check_types!(Varchar | Text | CharN | Name, ty)
        Ok(Some(self.as_bytes().to_vec()))
    }
}

to_option_impl_lifetime!(Varchar | Text | CharN | Name, &'a str)

impl<'a> ToSql for &'a [u8] {
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
        check_types!(ByteA, ty)
        Ok(Some(self.to_vec()))
    }
}

to_option_impl_lifetime!(ByteA, &'a [u8])

to_array_impl!(BoolArray, bool)
to_array_impl!(ByteAArray, Vec<u8>)
to_array_impl!(CharArray, i8)
to_array_impl!(Int2Array, i16)
to_array_impl!(Int4Array, i32)
to_array_impl!(Int8Array, i64)
to_array_impl!(TextArray | CharNArray | VarcharArray | NameArray, String)
to_array_impl!(Float4Array, f32)
to_array_impl!(Float8Array, f64)
to_array_impl!(Int4RangeArray, Range<i32>)
to_array_impl!(Int8RangeArray, Range<i64>)
to_array_impl!(JsonArray, json::Json)

impl ToSql for HashMap<String, Option<String>> {
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
        match *ty {
            Type::Unknown { ref name, .. } if "hstore" == *name => {}
            _ => return Err(Error::WrongType(ty.clone()))
        }

        let mut buf = vec![];

        try!(buf.write_be_i32(self.len() as i32));

        for (key, val) in self.iter() {
            try!(buf.write_be_i32(key.len() as i32));
            try!(buf.write(key.as_bytes()));

            match *val {
                Some(ref val) => {
                    try!(buf.write_be_i32(val.len() as i32));
                    try!(buf.write(val.as_bytes()));
                }
                None => try!(buf.write_be_i32(-1))
            }
        }

        Ok(Some(buf))
    }
}

impl ToSql for Option<HashMap<String, Option<String>>> {
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
        match *ty {
            Type::Unknown { ref name, .. } if "hstore" == *name => {}
            _ => return Err(Error::WrongType(ty.clone()))
        }

        match *self {
            Some(ref inner) => inner.to_sql(ty),
            None => Ok(None)
        }
    }
}

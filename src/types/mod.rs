//! Traits dealing with Postgres data types
pub use self::slice::Slice;

use std::collections::HashMap;
use std::old_io::net::ip::IpAddr;
use std::fmt;

use Result;
use error::Error;

pub use ugh_privacy::Unknown;

macro_rules! accepts {
    ($($expected:pat),+) => (
        fn accepts(ty: &::types::Type) -> bool {
            match *ty {
                $($expected)|+ => true,
                _ => false
            }
        }
    )
}

macro_rules! check_types {
    ($($expected:pat),+; $actual:ident) => (
        match $actual {
            $(&$expected)|+ => {}
            actual => return Err(::Error::WrongType(actual.clone()))
        }
    )
}

macro_rules! raw_from_impl {
    ($t:ty, $f:ident) => (
        impl RawFromSql for $t {
            fn raw_from_sql<R: Reader>(_: &Type, raw: &mut R) -> Result<$t> {
                Ok(try!(raw.$f()))
            }
        }
    )
}

macro_rules! from_option_impl {
    ($t:ty) => {
        impl ::types::FromSql for $t {
            fn from_sql(ty: &Type, raw: Option<&[u8]>) -> Result<$t> {
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
    }
}

macro_rules! from_map_impl {
    ($($expected:pat),+; $t:ty, $blk:expr) => (
        impl ::types::FromSql for Option<$t> {
            fn from_sql(ty: &Type, raw: Option<&[u8]>) -> Result<Option<$t>> {
                check_types!($($expected),+; ty);
                match raw {
                    Some(buf) => ($blk)(ty, buf).map(|ok| Some(ok)),
                    None => Ok(None)
                }
            }
        }

        from_option_impl!($t);
    )
}

macro_rules! from_raw_from_impl {
    ($($expected:pat),+; $t:ty) => (
        from_map_impl!($($expected),+; $t, |ty, mut buf: &[u8]| {
            use types::RawFromSql;

            RawFromSql::raw_from_sql(ty, &mut buf)
        });
    )
}

macro_rules! raw_to_impl {
    ($t:ty, $f:ident) => (
        impl RawToSql for $t {
            fn raw_to_sql<W: Writer>(&self, _: &Type, w: &mut W) -> Result<()> {
                Ok(try!(w.$f(*self)))
            }
        }
    )
}

macro_rules! to_option_impl {
    ($($oid:pat),+; $t:ty) => (
        impl ::types::ToSql for Option<$t> {
            fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
                check_types!($($oid),+; ty);

                match *self {
                    None => Ok(None),
                    Some(ref val) => val.to_sql(ty)
                }
            }
        }
    )
}

macro_rules! to_option_impl_lifetime {
    ($($oid:pat),+; $t:ty) => (
        impl<'a> ToSql for Option<$t> {
            fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
                check_types!($($oid),+; ty);

                match *self {
                    None => Ok(None),
                    Some(ref val) => val.to_sql(ty)
                }
            }
        }
    )
}

macro_rules! to_raw_to_impl {
    ($($oid:pat),+; $t:ty) => (
        impl ::types::ToSql for $t {
            fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
                check_types!($($oid),+; ty);

                let mut writer = vec![];
                try!(self.raw_to_sql(ty, &mut writer));
                Ok(Some(writer))
            }
        }

        to_option_impl!($($oid),+; $t);
    )
}

#[cfg(feature = "uuid")]
mod uuid;
#[cfg(feature = "time")]
mod time;
mod slice;
#[cfg(feature = "rustc-serialize")]
mod json;

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
const OIDOID: Oid = 26;
const JSONOID: Oid = 114;
const JSONARRAYOID: Oid = 199;
const CIDROID: Oid = 650;
const FLOAT4OID: Oid = 700;
const FLOAT8OID: Oid = 701;
const INETOID: Oid = 869;
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
const JSONBOID: Oid = 3802;
const JSONBARRAYOID: Oid = 3807;
const INT4RANGEOID: Oid = 3904;
const INT4RANGEARRAYOID: Oid = 3905;
const TSRANGEOID: Oid = 3908;
const TSRANGEARRAYOID: Oid = 3909;
const TSTZRANGEOID: Oid = 3910;
const TSTZRANGEARRAYOID: Oid = 3911;
const INT8RANGEOID: Oid = 3926;
const INT8RANGEARRAYOID: Oid = 3927;

/// Represents the kind of a Postgres type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// A simple type like `VARCHAR` or `INTEGER`.
    Simple,
    /// An array type along with the type of its elements.
    Array(Type),
    /// A range type along with the type of its elements.
    Range(Type),
}

macro_rules! make_postgres_type {
    ($(#[$doc:meta] $oid:ident => $variant:ident: $kind:expr),+) => (
        /// A Postgres type
        #[derive(PartialEq, Eq, Clone)]
        pub enum Type {
            $(
                #[$doc]
                $variant,
            )+
            /// An unknown type
            Unknown(Box<Unknown>),
        }

        impl fmt::Debug for Type {
            fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
                let s = match *self {
                    $(Type::$variant => stringify!($variant),)+
                    Type::Unknown(ref u) => return fmt::Debug::fmt(u, fmt),
                };
                fmt.write_str(s)
            }
        }

        impl Type {
            /// Creates a `Type` from an OID.
            ///
            /// If the OID is unknown, `None` is returned.
            pub fn from_oid(oid: Oid) -> Option<Type> {
                match oid {
                    $($oid => Some(Type::$variant),)+
                    _ => None
                }
            }

            /// Returns the OID of the `Type`.
            pub fn to_oid(&self) -> Oid {
                match *self {
                    $(Type::$variant => $oid,)+
                    Type::Unknown(ref u) => u.oid(),
                }
            }

            /// The kind of this type
            pub fn kind(&self) -> Kind {
                match *self {
                    $(
                        Type::$variant => $kind,
                    )+
                    Type::Unknown(ref u) => u.kind().clone(),
                }
            }
        }
    )
}

make_postgres_type! {
    #[doc="BOOL"]
    BOOLOID => Bool: Kind::Simple,
    #[doc="BYTEA"]
    BYTEAOID => ByteA: Kind::Simple,
    #[doc="\"char\""]
    CHAROID => Char: Kind::Simple,
    #[doc="NAME"]
    NAMEOID => Name: Kind::Simple,
    #[doc="INT8/BIGINT"]
    INT8OID => Int8: Kind::Simple,
    #[doc="INT2/SMALLINT"]
    INT2OID => Int2: Kind::Simple,
    #[doc="INT4/INT"]
    INT4OID => Int4: Kind::Simple,
    #[doc="TEXT"]
    TEXTOID => Text: Kind::Simple,
    #[doc="OID"]
    OIDOID => Oid: Kind::Simple,
    #[doc="JSON"]
    JSONOID => Json: Kind::Simple,
    #[doc="CIDR"]
    CIDROID => Cidr: Kind::Simple,
    #[doc="JSON[]"]
    JSONARRAYOID => JsonArray: Kind::Array(Type::Json),
    #[doc="FLOAT4/REAL"]
    FLOAT4OID => Float4: Kind::Simple,
    #[doc="FLOAT8/DOUBLE PRECISION"]
    FLOAT8OID => Float8: Kind::Simple,
    #[doc="INET"]
    INETOID => Inet: Kind::Simple,
    #[doc="BOOL[]"]
    BOOLARRAYOID => BoolArray: Kind::Array(Type::Bool),
    #[doc="BYTEA[]"]
    BYTEAARRAYOID => ByteAArray: Kind::Array(Type::ByteA),
    #[doc="\"char\"[]"]
    CHARARRAYOID => CharArray: Kind::Array(Type::Char),
    #[doc="NAME[]"]
    NAMEARRAYOID => NameArray: Kind::Array(Type::Name),
    #[doc="INT2[]"]
    INT2ARRAYOID => Int2Array: Kind::Array(Type::Int2),
    #[doc="INT4[]"]
    INT4ARRAYOID => Int4Array: Kind::Array(Type::Int4),
    #[doc="TEXT[]"]
    TEXTARRAYOID => TextArray: Kind::Array(Type::Text),
    #[doc="CHAR(n)[]"]
    BPCHARARRAYOID => CharNArray: Kind::Array(Type::CharN),
    #[doc="VARCHAR[]"]
    VARCHARARRAYOID => VarcharArray: Kind::Array(Type::Varchar),
    #[doc="INT8[]"]
    INT8ARRAYOID => Int8Array: Kind::Array(Type::Int8),
    #[doc="FLOAT4[]"]
    FLOAT4ARRAYOID => Float4Array: Kind::Array(Type::Float4),
    #[doc="FLOAT8[]"]
    FLAOT8ARRAYOID => Float8Array: Kind::Array(Type::Float8),
    #[doc="TIMESTAMP"]
    TIMESTAMPOID => Timestamp: Kind::Simple,
    #[doc="TIMESTAMP[]"]
    TIMESTAMPARRAYOID => TimestampArray: Kind::Array(Type::Timestamp),
    #[doc="TIMESTAMP WITH TIME ZONE"]
    TIMESTAMPZOID => TimestampTZ: Kind::Simple,
    #[doc="TIMESTAMP WITH TIME ZONE[]"]
    TIMESTAMPZARRAYOID => TimestampTZArray: Kind::Array(Type::TimestampTZ),
    #[doc="UUID"]
    UUIDOID => Uuid: Kind::Simple,
    #[doc="UUID[]"]
    UUIDARRAYOID => UuidArray: Kind::Array(Type::Uuid),
    #[doc="JSONB"]
    JSONBOID => Jsonb: Kind::Simple,
    #[doc="JSONB[]"]
    JSONBARRAYOID => JsonbArray: Kind::Array(Type::Jsonb),
    #[doc="CHAR(n)/CHARACTER(n)"]
    BPCHAROID => CharN: Kind::Simple,
    #[doc="VARCHAR/CHARACTER VARYING"]
    VARCHAROID => Varchar: Kind::Simple,
    #[doc="INT4RANGE"]
    INT4RANGEOID => Int4Range: Kind::Range(Type::Int4),
    #[doc="INT4RANGE[]"]
    INT4RANGEARRAYOID => Int4RangeArray: Kind::Array(Type::Int4Range),
    #[doc="TSRANGE"]
    TSRANGEOID => TsRange: Kind::Range(Type::Timestamp),
    #[doc="TSRANGE[]"]
    TSRANGEARRAYOID => TsRangeArray: Kind::Array(Type::TsRange),
    #[doc="TSTZRANGE"]
    TSTZRANGEOID => TstzRange: Kind::Range(Type::TimestampTZ),
    #[doc="TSTZRANGE[]"]
    TSTZRANGEARRAYOID => TstzRangeArray: Kind::Array(Type::TstzRange),
    #[doc="INT8RANGE"]
    INT8RANGEOID => Int8Range: Kind::Range(Type::Int8),
    #[doc="INT8RANGE[]"]
    INT8RANGEARRAYOID => Int8RangeArray: Kind::Array(Type::Int8Range)
}

/// A trait for types that can be created from a Postgres value.
pub trait FromSql: Sized {
    /// Creates a new value of this type from a `Reader` of Postgres data.
    ///
    /// If the value was `NULL`, the `Reader` will be `None`.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    ///
    /// The default implementation calls `FromSql::from_sql` when `raw` is
    /// `Some` and returns `Err(Error::WasNull)` when `raw` is `None`. It does
    /// not typically need to be overridden.
    fn from_sql_nullable<R: Reader>(ty: &Type, raw: Option<&mut R>) -> Result<Self> {
        match raw {
            Some(raw) => FromSql::from_sql(ty, raw),
            None => Err(Error::WasNull),
        }
    }

    /// Creates a new value of this type from a `Reader` of Postgres data.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    fn from_sql<R: Reader>(ty: &Type, raw: &mut R) -> Result<Self>;

    /// Determines if a value of this type can be created from the specified
    /// Postgres `Type`.
    fn accepts(ty: &Type) -> bool;
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_sql_nullable<R: Reader>(ty: &Type, raw: Option<&mut R>) -> Result<Option<T>> {
        match raw {
            Some(raw) => <T as FromSql>::from_sql(ty, raw).map(|e| Some(e)),
            None => Ok(None),
        }
    }

    fn from_sql<R: Reader>(ty: &Type, raw: &mut R) -> Result<Option<T>> {
        <T as FromSql>::from_sql(ty, raw).map(|e| Some(e))
    }

    fn accepts(ty: &Type) -> bool {
        <T as FromSql>::accepts(ty)
    }
}

impl FromSql for bool {
    fn from_sql<R: Reader>(_: &Type, raw: &mut R) -> Result<bool> {
        Ok(try!(raw.read_u8()) != 0)
    }

    accepts!(Type::Bool);
}

impl FromSql for Vec<u8> {
    fn from_sql<R: Reader>(_: &Type, raw: &mut R) -> Result<Vec<u8>> {
        Ok(try!(raw.read_to_end()))
    }

    accepts!(Type::ByteA);
}

impl FromSql for String {
    fn from_sql<R: Reader>(_: &Type, raw: &mut R) -> Result<String> {
        String::from_utf8(try!(raw.read_to_end())).map_err(|_| Error::BadResponse)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::Varchar | Type::Text | Type::CharN | Type::Name => true,
            Type::Unknown(ref u) if u.name() == "citext" => true,
            _ => false,
        }
    }
}

macro_rules! primitive_from {
    ($t:ty, $f:ident, $($expected:pat),+) => {
        impl FromSql for $t {
            fn from_sql<R: Reader>(_: &Type, raw: &mut R) -> Result<$t> {
                Ok(try!(raw.$f()))
            }

            accepts!($($expected),+);
        }
    }
}

primitive_from!(i8, read_i8, Type::Char);
primitive_from!(i16, read_be_i16, Type::Int2);
primitive_from!(i32, read_be_i32, Type::Int4);
primitive_from!(u32, read_be_u32, Type::Oid);
primitive_from!(i64, read_be_i64, Type::Int8);
primitive_from!(f32, read_be_f32, Type::Float4);
primitive_from!(f64, read_be_f64, Type::Float8);

impl FromSql for IpAddr {
    fn from_sql<R: Reader>(_: &Type, raw: &mut R) -> Result<IpAddr> {
        let family = try!(raw.read_u8());
        let _bits = try!(raw.read_u8());
        let _is_cidr = try!(raw.read_u8());
        let nb = try!(raw.read_u8());
        if nb > 16 {
            return Err(Error::BadResponse);
        }
        let mut buf = [0u8; 16];
        try!(raw.read_at_least(nb as usize, &mut buf));
        let mut buf: &[u8] = &buf;

        match family {
            2 if nb == 4 => Ok(IpAddr::Ipv4Addr(buf[0], buf[1], buf[2], buf[3])),
            3 if nb == 16 => Ok(IpAddr::Ipv6Addr(try!(buf.read_be_u16()),
                                                 try!(buf.read_be_u16()),
                                                 try!(buf.read_be_u16()),
                                                 try!(buf.read_be_u16()),
                                                 try!(buf.read_be_u16()),
                                                 try!(buf.read_be_u16()),
                                                 try!(buf.read_be_u16()),
                                                 try!(buf.read_be_u16()))),
            _ => Err(Error::BadResponse),
        }
    }

    accepts!(Type::Inet, Type::Cidr);
}

impl FromSql for HashMap<String, Option<String>> {
    fn from_sql<R: Reader>(_: &Type, raw: &mut R)
            -> Result<HashMap<String, Option<String>>> {
        let mut map = HashMap::new();

        let count = try!(raw.read_be_i32());

        for _ in range(0, count) {
            let key_len = try!(raw.read_be_i32());
            let key = try!(raw.read_exact(key_len as usize));
            let key = match String::from_utf8(key) {
                Ok(key) => key,
                Err(_) => return Err(Error::BadResponse),
            };

            let val_len = try!(raw.read_be_i32());
            let val = if val_len < 0 {
                None
            } else {
                let val = try!(raw.read_exact(val_len as usize));
                match String::from_utf8(val) {
                    Ok(val) => Some(val),
                    Err(_) => return Err(Error::BadResponse),
                }
            };

            map.insert(key, val);
        }

        Ok(map)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::Unknown(ref u) if u.name() == "hstore" => true,
            _ => false
        }
    }
}

/// A trait for types that can be converted into Postgres values
pub trait ToSql {
    /// Converts the value of `self` into the binary format appropriate for the
    /// Postgres backend.
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>>;
}

/// A utility trait used by `ToSql` implementations.
pub trait RawToSql {
    /// Converts the value of `self` into the binary format of the specified
    /// Postgres type, writing it to `w`.
    ///
    /// It is the caller's responsibility to make sure that this type can be
    /// converted to the specified Postgres type.
    fn raw_to_sql<W: Writer>(&self, ty: &Type, w: &mut W) -> Result<()>;
}

impl RawToSql for bool {
    fn raw_to_sql<W: Writer>(&self, _: &Type, w: &mut W) -> Result<()> {
        Ok(try!(w.write_u8(*self as u8)))
    }
}

impl RawToSql for Vec<u8> {
    fn raw_to_sql<W: Writer>(&self, _: &Type, w: &mut W) -> Result<()> {
        Ok(try!(w.write_all(&**self)))
    }
}

impl RawToSql for String {
    fn raw_to_sql<W: Writer>(&self, _: &Type, w: &mut W) -> Result<()> {
        Ok(try!(w.write_all(self.as_bytes())))
    }
}

raw_to_impl!(i8, write_i8);
raw_to_impl!(i16, write_be_i16);
raw_to_impl!(i32, write_be_i32);
raw_to_impl!(u32, write_be_u32);
raw_to_impl!(i64, write_be_i64);
raw_to_impl!(f32, write_be_f32);
raw_to_impl!(f64, write_be_f64);

impl RawToSql for IpAddr {
    fn raw_to_sql<W: Writer>(&self, _: &Type, raw: &mut W) -> Result<()> {
        match *self {
            IpAddr::Ipv4Addr(a, b, c, d) => {
                try!(raw.write_all(&[2, // family
                                 32, // bits
                                 0, // is_cidr
                                 4, // nb
                                 a, b, c, d // addr
                                ]));
            }
            IpAddr::Ipv6Addr(a, b, c, d, e, f, g, h) => {
                try!(raw.write_all(&[3, // family
                                 128, // bits
                                 0, // is_cidr
                                 16, // nb
                                ]));
                try!(raw.write_be_u16(a));
                try!(raw.write_be_u16(b));
                try!(raw.write_be_u16(c));
                try!(raw.write_be_u16(d));
                try!(raw.write_be_u16(e));
                try!(raw.write_be_u16(f));
                try!(raw.write_be_u16(g));
                try!(raw.write_be_u16(h));
            }
        }
        Ok(())
    }
}

to_raw_to_impl!(Type::Bool; bool);
to_raw_to_impl!(Type::ByteA; Vec<u8>);
to_raw_to_impl!(Type::Inet, Type::Cidr; IpAddr);
to_raw_to_impl!(Type::Char; i8);
to_raw_to_impl!(Type::Int2; i16);
to_raw_to_impl!(Type::Int4; i32);
to_raw_to_impl!(Type::Oid; u32);
to_raw_to_impl!(Type::Int8; i64);
to_raw_to_impl!(Type::Float4; f32);
to_raw_to_impl!(Type::Float8; f64);

impl ToSql for String {
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
        (&**self).to_sql(ty)
    }
}

impl ToSql for Option<String> {
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
        self.as_ref().map(|s| &**s).to_sql(ty)
    }
}

impl<'a> ToSql for &'a str {
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
        match *ty {
            Type::Varchar | Type::Text | Type::CharN | Type::Name => {}
            Type::Unknown(ref u) if u.name() == "citext" => {}
            _ => return Err(Error::WrongType(ty.clone()))
        }
        Ok(Some(self.as_bytes().to_vec()))
    }
}

impl<'a> ToSql for Option<&'a str> {
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
        match *ty {
            Type::Varchar | Type::Text | Type::CharN | Type::Name => {}
            Type::Unknown(ref u) if u.name() == "citext" => {}
            _ => return Err(Error::WrongType(ty.clone()))
        }
        match *self {
            Some(ref val) => val.to_sql(ty),
            None => Ok(None)
        }
    }
}

impl<'a> ToSql for &'a [u8] {
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
        check_types!(Type::ByteA; ty);
        Ok(Some(self.to_vec()))
    }
}

to_option_impl_lifetime!(Type::ByteA; &'a [u8]);

impl ToSql for HashMap<String, Option<String>> {
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
        match *ty {
            Type::Unknown(ref u) if u.name() == "hstore" => {}
            _ => return Err(Error::WrongType(ty.clone()))
        }

        let mut buf = vec![];

        try!(buf.write_be_i32(self.len() as i32));

        for (key, val) in self.iter() {
            try!(buf.write_be_i32(key.len() as i32));
            try!(buf.write_all(key.as_bytes()));

            match *val {
                Some(ref val) => {
                    try!(buf.write_be_i32(val.len() as i32));
                    try!(buf.write_all(val.as_bytes()));
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
            Type::Unknown(ref u) if u.name() == "hstore" => {}
            _ => return Err(Error::WrongType(ty.clone()))
        }

        match *self {
            Some(ref inner) => inner.to_sql(ty),
            None => Ok(None)
        }
    }
}

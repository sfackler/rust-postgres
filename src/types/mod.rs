//! Traits dealing with Postgres data types
pub use self::slice::Slice;

use std::option::Option::{self, Some, None};
use std::result::Result::{Ok, Err};
use std::boxed::Box;
use std::vec::Vec;
use std::clone::Clone;
use std::string::String;
use std::iter::ExactSizeIterator;
use std::marker::Sized;

use std::collections::HashMap;
use std::fmt;
use std::io::prelude::*;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

use Result;
use error::Error;

pub use ugh_privacy::Other;

/// Generates a simple implementation of `ToSql::accepts` which accepts the
/// types passed to it.
#[macro_export]
macro_rules! accepts {
    ($($expected:pat),+) => (
        fn accepts(ty: &$crate::types::Type) -> bool {
            match *ty {
                $($expected)|+ => true,
                _ => false
            }
        }
    )
}

/// Generates an implementation of `ToSql::to_sql_checked`.
///
/// All `ToSql` implementations should use this macro.
#[macro_export]
macro_rules! to_sql_checked {
    () => {
        fn to_sql_checked(&self, ty: &$crate::types::Type, out: &mut ::std::io::Write)
                          -> $crate::Result<$crate::types::IsNull> {
            if !<Self as $crate::types::ToSql>::accepts(ty) {
                return Err($crate::Error::WrongType(ty.clone()));
            }
            self.to_sql(ty, out)
        }
    }
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

macro_rules! as_pat {
    ($p:pat) => ($p)
}

macro_rules! as_expr {
    ($e:expr) => ($e)
}

macro_rules! make_postgres_type {
    ($(#[$doc:meta] $oid:tt => $variant:ident: $kind:expr),+) => (
        /// A Postgres type
        #[derive(PartialEq, Eq, Clone)]
        pub enum Type {
            $(
                #[$doc]
                $variant,
            )+
            /// An unknown type
            Other(Box<Other>),
        }

        impl fmt::Debug for Type {
            fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
                let s = match *self {
                    $(Type::$variant => stringify!($variant),)+
                    Type::Other(ref u) => return fmt::Debug::fmt(u, fmt),
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
                    $(as_pat!($oid) => Some(Type::$variant),)+
                    _ => None
                }
            }

            /// Returns the OID of the `Type`.
            pub fn to_oid(&self) -> Oid {
                match *self {
                    $(Type::$variant => as_expr!($oid),)+
                    Type::Other(ref u) => u.oid(),
                }
            }

            /// The kind of this type
            pub fn kind(&self) -> &Kind {
                match *self {
                    $(
                        Type::$variant => {
                            const V: &'static Kind = &$kind;
                            V
                        }
                    )+
                    Type::Other(ref u) => u.kind(),
                }
            }
        }
    )
}

// Values from pg_type.h
make_postgres_type! {
    #[doc="BOOL - boolean, 'true'/'false'"]
    16 => Bool: Kind::Simple,
    #[doc="BYTEA - variable-length string, binary values escaped"]
    17 => Bytea: Kind::Simple,
    #[doc="\"char\" - single character"]
    18 => Char: Kind::Simple,
    #[doc="NAME - 63-byte type for storing system identifiers"]
    19 => Name: Kind::Simple,
    #[doc="INT8/BIGINT - ~18 digit integer, 8-byte storage"]
    20 => Int8: Kind::Simple,
    #[doc="INT2/SMALLINT - -32 thousand to 32 thousand, 2-byte storage"]
    21 => Int2: Kind::Simple,
    #[doc="INT2VECTOR - array of int2, used in system tables"]
    22 => Int2Vector: Kind::Array(Type::Int2),
    #[doc="INT4/INT - -2 billion to 2 billion integer, 4-byte storage"]
    23 => Int4: Kind::Simple,
    #[doc="REGPROC - registered procedure"]
    24 => Regproc: Kind::Simple,
    #[doc="TEXT - variable-length string, no limit specified"]
    25 => Text: Kind::Simple,
    #[doc="OID - object identifier(oid), maximum 4 billion"]
    26 => Oid: Kind::Simple,
    #[doc="TID - (block, offset), physical location of tuple"]
    27 => Tid: Kind::Simple,
    #[doc="XID - transaction id"]
    28 => Xid: Kind::Simple,
    #[doc="CID - command identifier type, sequence in transaction id"]
    29 => Cid: Kind::Simple,
    #[doc="OIDVECTOR - array of oids, used in system tables"]
    30 => OidVector: Kind::Array(Type::Oid),
    #[doc="PG_TYPE"]
    71 => PgType: Kind::Simple,
    #[doc="PG_ATTRIBUTE"]
    75 => PgAttribute: Kind::Simple,
    #[doc="PG_PROC"]
    81 => PgProc: Kind::Simple,
    #[doc="PG_CLASS"]
    83 => PgClass: Kind::Simple,
    #[doc="JSON"]
    114 => Json: Kind::Simple,
    #[doc="XML - XML content"]
    142 => Xml: Kind::Simple,
    #[doc="XML[]"]
    143 => XmlArray: Kind::Array(Type::Xml),
    #[doc="PG_NODE_TREE - string representing an internal node tree"]
    194 => PgNodeTree: Kind::Simple,
    #[doc="JSON[]"]
    199 => JsonArray: Kind::Array(Type::Json),
    #[doc="SMGR - storage manager"]
    210 => Smgr: Kind::Simple,
    #[doc="POINT - geometric point '(x, y)'"]
    600 => Point: Kind::Simple,
    #[doc="LSEG - geometric line segment '(pt1,pt2)'"]
    601 => Lseg: Kind::Simple,
    #[doc="PATH - geometric path '(pt1,...)'"]
    602 => Path: Kind::Simple,
    #[doc="BOX - geometric box '(lower left,upper right)'"]
    603 => Box: Kind::Simple,
    #[doc="POLYGON - geometric polygon '(pt1,...)'"]
    604 => Polygon: Kind::Simple,
    #[doc="LINE - geometric line"]
    628 => Line: Kind::Simple,
    #[doc="LINE[]"]
    629 => LineArray: Kind::Array(Type::Line),
    #[doc="CIDR - network IP address/netmask, network address"]
    650 => Cidr: Kind::Simple,
    #[doc="CIDR[]"]
    651 => CidrArray: Kind::Array(Type::Cidr),
    #[doc="FLOAT4/REAL - single-precision floating point number, 4-byte storage"]
    700 => Float4: Kind::Simple,
    #[doc="FLOAT8/DOUBLE PRECISION - double-precision floating point number, 8-byte storage"]
    701 => Float8: Kind::Simple,
    #[doc="ABSTIME - absolute, limited-range date and time (Unix system time)"]
    702 => Abstime: Kind::Simple,
    #[doc="RELTIME - relative, limited-range date and time (Unix delta time)"]
    703 => Reltime: Kind::Simple,
    #[doc="TINTERVAL - (abstime,abstime), time interval"]
    704 => Tinterval: Kind::Simple,
    #[doc="UNKNOWN"]
    705 => Unknown: Kind::Simple,
    #[doc="CIRCLE - geometric circle '(center,radius)'"]
    718 => Circle: Kind::Simple,
    #[doc="CIRCLE[]"]
    719 => CircleArray: Kind::Array(Type::Circle),
    #[doc="MONEY - monetary amounts, $d,ddd.cc"]
    790 => Money: Kind::Simple,
    #[doc="MONEY[]"]
    791 => MoneyArray: Kind::Array(Type::Money),
    #[doc="MACADDR - XX:XX:XX:XX:XX:XX, MAC address"]
    829 => Macaddr: Kind::Simple,
    #[doc="INET - IP address/netmask, host address, netmask optional"]
    869 => Inet: Kind::Simple,
    #[doc="BOOL[]"]
    1000 => BoolArray: Kind::Array(Type::Bool),
    #[doc="BYTEA[]"]
    1001 => ByteaArray: Kind::Array(Type::Bytea),
    #[doc="\"char\"[]"]
    1002 => CharArray: Kind::Array(Type::Char),
    #[doc="NAME[]"]
    1003 => NameArray: Kind::Array(Type::Name),
    #[doc="INT2[]"]
    1005 => Int2Array: Kind::Array(Type::Int2),
    #[doc="INT2VECTOR[]"]
    1006 => Int2VectorArray: Kind::Array(Type::Int2Vector),
    #[doc="INT4[]"]
    1007 => Int4Array: Kind::Array(Type::Int4),
    #[doc="REGPROC[]"]
    1008 => RegprocArray: Kind::Array(Type::Regproc),
    #[doc="TEXT[]"]
    1009 => TextArray: Kind::Array(Type::Text),
    #[doc="TID[]"]
    1010 => TidArray: Kind::Array(Type::Tid),
    #[doc="XID[]"]
    1011 => XidArray: Kind::Array(Type::Xid),
    #[doc="CID[]"]
    1012 => CidArray: Kind::Array(Type::Cid),
    #[doc="OIDVECTOR[]"]
    1013 => OidVectorArray: Kind::Array(Type::OidVector),
    #[doc="BPCHAR[]"]
    1014 => BpcharArray: Kind::Array(Type::Bpchar),
    #[doc="VARCHAR[]"]
    1015 => VarcharArray: Kind::Array(Type::Varchar),
    #[doc="INT8[]"]
    1016 => Int8Array: Kind::Array(Type::Int8),
    #[doc="POINT[]"]
    1017 => PointArray: Kind::Array(Type::Point),
    #[doc="LSEG[]"]
    1018 => LsegArray: Kind::Array(Type::Lseg),
    #[doc="PATH[]"]
    1019 => PathArray: Kind::Array(Type::Path),
    #[doc="BOX[]"]
    1020 => BoxArray: Kind::Array(Type::Box),
    #[doc="FLOAT4[]"]
    1021 => Float4Array: Kind::Array(Type::Float4),
    #[doc="FLOAT8[]"]
    1022 => Float8Array: Kind::Array(Type::Float8),
    #[doc="ABSTIME[]"]
    1023 => AbstimeArray: Kind::Array(Type::Abstime),
    #[doc="RELTIME[]"]
    1024 => ReltimeArray: Kind::Array(Type::Reltime),
    #[doc="TINTERVAL[]"]
    1025 => TintervalArray: Kind::Array(Type::Tinterval),
    #[doc="POLYGON[]"]
    1027 => PolygonArray: Kind::Array(Type::Polygon),
    #[doc="OID[]"]
    1028 => OidArray: Kind::Array(Type::Oid),
    #[doc="ACLITEM - access control list"]
    1033 => Aclitem: Kind::Simple,
    #[doc="ACLITEM[]"]
    1034 => AclitemArray: Kind::Array(Type::Aclitem),
    #[doc="MACADDR[]"]
    1040 => MacaddrArray: Kind::Array(Type::Macaddr),
    #[doc="INET[]"]
    1041 => InetArray: Kind::Array(Type::Inet),
    #[doc="BPCHAR - char(length), blank-padded string, fixed storage length"]
    1042 => Bpchar: Kind::Simple,
    #[doc="VARCHAR - varchar(length), non-blank-padded string, variable storage length"]
    1043 => Varchar: Kind::Simple,
    #[doc="DATE - date"]
    1082 => Date: Kind::Simple,
    #[doc="TIME - time of day"]
    1083 => Time: Kind::Simple,
    #[doc="TIMESTAMP - date and time"]
    1114 => Timestamp: Kind::Simple,
    #[doc="TIMESTAMP[]"]
    1115 => TimestampArray: Kind::Array(Type::Timestamp),
    #[doc="DATE[]"]
    1182 => DateArray: Kind::Array(Type::Date),
    #[doc="TIME[]"]
    1183 => TimeArray: Kind::Array(Type::Time),
    #[doc="TIMESTAMPTZ - date and time with time zone"]
    1184 => TimestampTZ: Kind::Simple,
    #[doc="TIMESTAMPTZ[]"]
    1185 => TimestampTZArray: Kind::Array(Type::TimestampTZ),
    #[doc="INTERVAL - @ &lt;number&gt; &lt;units&gt;, time interval"]
    1186 => Interval: Kind::Simple,
    #[doc="INTERVAL[]"]
    1187 => IntervalArray: Kind::Array(Type::Interval),
    #[doc="NUMERIC[]"]
    1231 => NumericArray: Kind::Array(Type::Numeric),
    #[doc="CSTRING[]"]
    1263 => CstringArray: Kind::Array(Type::Cstring),
    #[doc="TIMETZ - time of day with time zone"]
    1266 => Timetz: Kind::Simple,
    #[doc="TIMETZ[]"]
    1270 => TimetzArray: Kind::Array(Type::Timetz),
    #[doc="BIT - fixed-length bit string"]
    1560 => Bit: Kind::Simple,
    #[doc="BIT[]"]
    1561 => BitArray: Kind::Array(Type::Bit),
    #[doc="VARBIT - variable-length bit string"]
    1562 => Varbit: Kind::Simple,
    #[doc="VARBIT[]"]
    1563 => VarbitArray: Kind::Array(Type::Varbit),
    #[doc="NUMERIC - numeric(precision, decimal), arbitrary precision number"]
    1700 => Numeric: Kind::Simple,
    #[doc="REFCURSOR - reference to cursor (portal name)"]
    1790 => Refcursor: Kind::Simple,
    #[doc="REFCURSOR[]"]
    2201 => RefcursorArray: Kind::Array(Type::Refcursor),
    #[doc="REGPROCEDURE - registered procedure (with args)"]
    2202 => Regprocedure: Kind::Simple,
    #[doc="REGOPER - registered operator"]
    2203 => Regoper: Kind::Simple,
    #[doc="REGOPERATOR - registered operator (with args)"]
    2204 => Regoperator: Kind::Simple,
    #[doc="REGCLASS - registered class"]
    2205 => Regclass: Kind::Simple,
    #[doc="REGTYPE - registered type"]
    2206 => Regtype: Kind::Simple,
    #[doc="REGPROCEDURE[]"]
    2207 => RegprocedureArray: Kind::Array(Type::Regprocedure),
    #[doc="REGOPER[]"]
    2208 => RegoperArray: Kind::Array(Type::Regoper),
    #[doc="REGOPERATOR[]"]
    2209 => RegoperatorArray: Kind::Array(Type::Regoperator),
    #[doc="REGCLASS[]"]
    2210 => RegclassArray: Kind::Array(Type::Regclass),
    #[doc="REGTYPE[]"]
    2211 => RegtypeArray: Kind::Array(Type::Regtype),
    #[doc="RECORD"]
    2249 => Record: Kind::Simple,
    #[doc="CSTRING"]
    2275 => Cstring: Kind::Simple,
    #[doc="ANY"]
    2276 => Any: Kind::Simple,
    #[doc="ANY[]"]
    2277 => AnyArray: Kind::Array(Type::Any),
    #[doc="VOID"]
    2278 => Void: Kind::Simple,
    #[doc="TRIGGER"]
    2279 => Trigger: Kind::Simple,
    #[doc="LANGUAGE_HANDLER"]
    2280 => LanguageHandler: Kind::Simple,
    #[doc="INTERNAL"]
    2281 => Internal: Kind::Simple,
    #[doc="OPAQUE"]
    2282 => Opaque: Kind::Simple,
    #[doc="ANYELEMENT"]
    2283 => Anyelement: Kind::Simple,
    #[doc="RECORD[]"]
    2287 => RecordArray: Kind::Array(Type::Record),
    #[doc="ANYNONARRAY"]
    2776 => Anynonarray: Kind::Simple,
    #[doc="TXID_SNAPSHOT[]"]
    2949 => TxidSnapshotArray: Kind::Array(Type::TxidSnapshot),
    #[doc="UUID - UUID datatype"]
    2950 => Uuid: Kind::Simple,
    #[doc="TXID_SNAPSHOT - txid snapshot"]
    2970 => TxidSnapshot: Kind::Simple,
    #[doc="UUID[]"]
    2951 => UuidArray: Kind::Array(Type::Uuid),
    #[doc="FDW_HANDLER"]
    3115 => FdwHandler: Kind::Simple,
    #[doc="PG_LSN - PostgreSQL LSN datatype"]
    3320 => PgLsn: Kind::Simple,
    #[doc="PG_LSN[]"]
    3321 => PgLsnArray: Kind::Array(Type::PgLsn),
    #[doc="ANYENUM"]
    3500 => Anyenum: Kind::Simple,
    #[doc="TSVECTOR - text representation for text search"]
    3614 => Tsvector: Kind::Simple,
    #[doc="TSQUERY - query representation for text search"]
    3615 => Tsquery: Kind::Simple,
    #[doc="GTSVECTOR - GiST index internal text representation for text search"]
    3642 => Gtsvector: Kind::Simple,
    #[doc="TSVECTOR[]"]
    3643 => TsvectorArray: Kind::Array(Type::Tsvector),
    #[doc="GTSVECTOR[]"]
    3644 => GtsvectorArray: Kind::Array(Type::Gtsvector),
    #[doc="TSQUERY[]"]
    3645 => TsqueryArray: Kind::Array(Type::Tsquery),
    #[doc="REGCONFIG - registered text search configuration"]
    3734 => Regconfig: Kind::Simple,
    #[doc="REGCONFIG[]"]
    3735 => RegconfigArray: Kind::Array(Type::Regconfig),
    #[doc="REGDICTIONARY - registered text search dictionary"]
    3769 => Regdictionary: Kind::Simple,
    #[doc="REGDICTIONARY[]"]
    3770 => RegdictionaryArray: Kind::Array(Type::Regdictionary),
    #[doc="JSONB"]
    3802 => Jsonb: Kind::Simple,
    #[doc="ANYRANGE"]
    3831 => Anyrange: Kind::Simple,
    #[doc="JSONB[]"]
    3807 => JsonbArray: Kind::Array(Type::Jsonb),
    #[doc="INT4RANGE - range of integers"]
    3904 => Int4Range: Kind::Range(Type::Int4),
    #[doc="INT4RANGE[]"]
    3905 => Int4RangeArray: Kind::Array(Type::Int4Range),
    #[doc="NUMRANGE - range of numerics"]
    3906 => NumRange: Kind::Range(Type::Numeric),
    #[doc="NUMRANGE[]"]
    3907 => NumRangeArray: Kind::Array(Type::NumRange),
    #[doc="TSRANGE - range of timestamps without time zone"]
    3908 => TsRange: Kind::Range(Type::Timestamp),
    #[doc="TSRANGE[]"]
    3909 => TsRangeArray: Kind::Array(Type::TsRange),
    #[doc="TSTZRANGE - range of timestamps with time zone"]
    3910 => TstzRange: Kind::Range(Type::TimestampTZ),
    #[doc="TSTZRANGE[]"]
    3911 => TstzRangeArray: Kind::Array(Type::TstzRange),
    #[doc="DATERANGE - range of dates"]
    3912 => DateRange: Kind::Range(Type::Date),
    #[doc="DATERANGE[]"]
    3913 => DateRangeArray: Kind::Array(Type::DateRange),
    #[doc="INT8RANGE - range of bigints"]
    3926 => Int8Range: Kind::Range(Type::Int8),
    #[doc="INT8RANGE[]"]
    3927 => Int8RangeArray: Kind::Array(Type::Int8Range),
    #[doc="EVENT_TRIGGER"]
    3838 => EventTrigger: Kind::Simple
}

/// A trait for types that can be created from a Postgres value.
pub trait FromSql: Sized {
    /// Creates a new value of this type from a `Read` of Postgres data.
    ///
    /// If the value was `NULL`, the `Read` will be `None`.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    ///
    /// The default implementation calls `FromSql::from_sql` when `raw` is
    /// `Some` and returns `Err(Error::WasNull)` when `raw` is `None`. It does
    /// not typically need to be overridden.
    fn from_sql_nullable<R: Read>(ty: &Type, raw: Option<&mut R>) -> Result<Self> {
        match raw {
            Some(raw) => FromSql::from_sql(ty, raw),
            None => Err(Error::WasNull),
        }
    }

    /// Creates a new value of this type from a `Read` of the binary format
    /// of the specified Postgres `Type`.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    fn from_sql<R: Read>(ty: &Type, raw: &mut R) -> Result<Self>;

    /// Determines if a value of this type can be created from the specified
    /// Postgres `Type`.
    fn accepts(ty: &Type) -> bool;
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_sql_nullable<R: Read>(ty: &Type, raw: Option<&mut R>) -> Result<Option<T>> {
        match raw {
            Some(raw) => <T as FromSql>::from_sql(ty, raw).map(|e| Some(e)),
            None => Ok(None),
        }
    }

    fn from_sql<R: Read>(ty: &Type, raw: &mut R) -> Result<Option<T>> {
        <T as FromSql>::from_sql(ty, raw).map(|e| Some(e))
    }

    fn accepts(ty: &Type) -> bool {
        <T as FromSql>::accepts(ty)
    }
}

impl FromSql for bool {
    fn from_sql<R: Read>(_: &Type, raw: &mut R) -> Result<bool> {
        Ok(try!(raw.read_u8()) != 0)
    }

    accepts!(Type::Bool);
}

impl FromSql for Vec<u8> {
    fn from_sql<R: Read>(_: &Type, raw: &mut R) -> Result<Vec<u8>> {
        let mut buf = vec![];
        try!(raw.read_to_end(&mut buf));
        Ok(buf)
    }

    accepts!(Type::Bytea);
}

impl FromSql for String {
    fn from_sql<R: Read>(_: &Type, raw: &mut R) -> Result<String> {
        let mut buf = vec![];
        try!(raw.read_to_end(&mut buf));
        String::from_utf8(buf).map_err(|_| Error::BadResponse)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::Varchar | Type::Text | Type::Bpchar | Type::Name => true,
            Type::Other(ref u) if u.name() == "citext" => true,
            _ => false,
        }
    }
}

macro_rules! primitive_from {
    ($t:ty, $f:ident, $($expected:pat),+) => {
        impl FromSql for $t {
            fn from_sql<R: Read>(_: &Type, raw: &mut R) -> Result<$t> {
                Ok(try!(raw.$f::<BigEndian>()))
            }

            accepts!($($expected),+);
        }
    }
}

impl FromSql for i8 {
    fn from_sql<R: Read>(_: &Type, raw: &mut R) -> Result<i8> {
        Ok(try!(raw.read_i8()))
    }

    accepts!(Type::Char);
}

primitive_from!(i16, read_i16, Type::Int2);
primitive_from!(i32, read_i32, Type::Int4);
primitive_from!(u32, read_u32, Type::Oid);
primitive_from!(i64, read_i64, Type::Int8);
primitive_from!(f32, read_f32, Type::Float4);
primitive_from!(f64, read_f64, Type::Float8);

impl FromSql for HashMap<String, Option<String>> {
    fn from_sql<R: Read>(_: &Type, raw: &mut R)
            -> Result<HashMap<String, Option<String>>> {
        let mut map = HashMap::new();

        let count = try!(raw.read_i32::<BigEndian>());

        for _ in 0..count {
            let key_len = try!(raw.read_i32::<BigEndian>());
            let mut key = vec![];
            try!(raw.take(key_len as u64).read_to_end(&mut key));
            let key = match String::from_utf8(key) {
                Ok(key) => key,
                Err(_) => return Err(Error::BadResponse),
            };

            let val_len = try!(raw.read_i32::<BigEndian>());
            let val = if val_len < 0 {
                None
            } else {
                let mut val = vec![];
                try!(raw.take(val_len as u64).read_to_end(&mut val));
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
            Type::Other(ref u) if u.name() == "hstore" => true,
            _ => false
        }
    }
}

/// An enum representing the nullability of a Postgres value.
pub enum IsNull {
    /// The value is NULL.
    Yes,
    /// The value is not NULL.
    No,
}

/// A trait for types that can be converted into Postgres values.
pub trait ToSql {
    /// Converts the value of `self` into the binary format of the specified
    /// Postgres `Type`, writing it to `out`.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    ///
    /// The return value indicates if this value should be represented as
    /// `NULL`. If this is the case, implementations **must not** write
    /// anything to `out`.
    fn to_sql<W: ?Sized>(&self, ty: &Type, out: &mut W) -> Result<IsNull>
            where Self: Sized, W: Write;

    /// Determines if a value of this type can be converted to the specified
    /// Postgres `Type`.
    fn accepts(ty: &Type) -> bool where Self: Sized;

    /// An adaptor method used internally by Rust-Postgres.
    ///
    /// *All* implementations of this method should be generated by the
    /// `to_sql_checked!()` macro.
    fn to_sql_checked(&self, ty: &Type, out: &mut Write) -> Result<IsNull>;
}

impl<T: ToSql> ToSql for Option<T> {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, ty: &Type, out: &mut W) -> Result<IsNull> {
        match *self {
            Some(ref val) => val.to_sql(ty, out),
            None => Ok(IsNull::Yes),
        }
    }

    fn accepts(ty: &Type) -> bool {
        <T as ToSql>::accepts(ty)
    }
}

impl ToSql for bool {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W) -> Result<IsNull> {
        try!(w.write_u8(*self as u8));
        Ok(IsNull::No)
    }

    accepts!(Type::Bool);
}

impl<'a> ToSql for &'a [u8] {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, _: &Type, w: &mut W) -> Result<IsNull> {
        try!(w.write_all(*self));
        Ok(IsNull::No)
    }

    accepts!(Type::Bytea);
}

impl ToSql for Vec<u8> {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, ty: &Type, w: &mut W) -> Result<IsNull> {
        (&**self).to_sql(ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&[u8] as ToSql>::accepts(ty)
    }
}

impl<'a> ToSql for &'a str {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, _: &Type, w: &mut W) -> Result<IsNull> {
        try!(w.write_all(self.as_bytes()));
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::Varchar | Type::Text | Type::Bpchar | Type::Name => true,
            Type::Other(ref u) if u.name() == "citext" => true,
            _ => false,
        }
    }
}

impl ToSql for String {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, ty: &Type, w: &mut W) -> Result<IsNull> {
        (&**self).to_sql(ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty)
    }
}

macro_rules! to_primitive {
    ($t:ty, $f:ident, $($expected:pat),+) => {
        impl ToSql for $t {
            to_sql_checked!();

            fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W) -> Result<IsNull> {
                try!(w.$f::<BigEndian>(*self));
                Ok(IsNull::No)
            }

            accepts!($($expected),+);
        }
    }
}

impl ToSql for i8 {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W) -> Result<IsNull> {
        try!(w.write_i8(*self));
        Ok(IsNull::No)
    }

    accepts!(Type::Char);
}

to_primitive!(i16, write_i16, Type::Int2);
to_primitive!(i32, write_i32, Type::Int4);
to_primitive!(u32, write_u32, Type::Oid);
to_primitive!(i64, write_i64, Type::Int8);
to_primitive!(f32, write_f32, Type::Float4);
to_primitive!(f64, write_f64, Type::Float8);

impl ToSql for HashMap<String, Option<String>> {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W) -> Result<IsNull> {
        try!(w.write_i32::<BigEndian>(self.len() as i32));

        for (key, val) in self.iter() {
            try!(w.write_i32::<BigEndian>(key.len() as i32));
            try!(w.write_all(key.as_bytes()));

            match *val {
                Some(ref val) => {
                    try!(w.write_i32::<BigEndian>(val.len() as i32));
                    try!(w.write_all(val.as_bytes()));
                }
                None => try!(w.write_i32::<BigEndian>(-1))
            }
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::Other(ref u) if u.name() == "hstore" => true,
            _ => false,
        }
    }
}

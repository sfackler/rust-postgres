//! Traits dealing with Postgres data types

use std::collections::HashMap;
use std::error;
use std::fmt;
use std::io::prelude::*;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

pub use self::slice::Slice;
use {Result, SessionInfoNew, InnerConnection, OtherNew};
use error::Error;
use util;

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
        fn to_sql_checked(&self, ty: &$crate::types::Type, out: &mut ::std::io::Write,
                          ctx: &$crate::types::SessionInfo)
                          -> $crate::Result<$crate::types::IsNull> {
            if !<Self as $crate::types::ToSql>::accepts(ty) {
                return Err($crate::error::Error::WrongType(ty.clone()));
            }
            self.to_sql(ty, out, ctx)
        }
    }
}

#[cfg(feature = "uuid")]
mod uuid;
#[cfg(feature = "time")]
mod time;
mod slice;
#[cfg(feature = "rustc-serialize")]
mod rustc_serialize;
#[cfg(feature = "serde_json")]
mod serde_json;
#[cfg(feature = "serde")]
mod serde;
#[cfg(feature = "chrono")]
mod chrono;

/// A structure providing information for conversion methods.
pub struct SessionInfo<'a> {
    conn: &'a InnerConnection,
}

impl<'a> SessionInfoNew<'a> for SessionInfo<'a> {
    fn new(conn: &'a InnerConnection) -> SessionInfo<'a> {
        SessionInfo {
            conn: conn
        }
    }
}

impl<'a> SessionInfo<'a> {
    /// Returns the value of the specified Postgres backend parameter, such
    /// as `timezone` or `server_version`.
    pub fn parameter(&self, param: &str) -> Option<&'a str> {
        self.conn.parameters.get(param).map(|s| &**s)
    }
}

/// A Postgres OID.
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
    ($(#[$doc:meta] $oid:tt: $name:expr => $variant:ident: $kind:expr),+) => (
        /// A Postgres type.
        #[derive(PartialEq, Eq, Clone)]
        pub enum Type {
            $(
                #[$doc]
                $variant,
            )+
            /// An unknown type.
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
            /// Returns the `Type` corresponding to the provided `Oid` if it
            /// corresponds to a built-in type.
            pub fn from_oid(oid: Oid) -> Option<Type> {
                match oid {
                    $(as_pat!($oid) => Some(Type::$variant),)+
                    _ => None
                }
            }

            /// Returns the OID of the `Type`.
            pub fn oid(&self) -> Oid {
                match *self {
                    $(Type::$variant => as_expr!($oid),)+
                    Type::Other(ref u) => u.oid(),
                }
            }

            /// Returns the kind of this type.
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

            /// Returns the schema of this type.
            pub fn schema(&self) -> &str {
                match *self {
                    Type::Other(ref u) => u.schema(),
                    _ => "pg_catalog",
                }
            }

            /// Returns the name of this type.
            pub fn name(&self) -> &str {
                match *self {
                    $(
                        Type::$variant => $name,
                    )+
                    Type::Other(ref u) => u.name(),
                }
            }
        }
    )
}

// Values from pg_type.h
make_postgres_type! {
    #[doc="BOOL - boolean, 'true'/'false'"]
    16: "bool" => Bool: Kind::Simple,
    #[doc="BYTEA - variable-length string, binary values escaped"]
    17: "bytea" => Bytea: Kind::Simple,
    #[doc="\"char\" - single character"]
    18: "char" => Char: Kind::Simple,
    #[doc="NAME - 63-byte type for storing system identifiers"]
    19: "name" => Name: Kind::Simple,
    #[doc="INT8/BIGINT - ~18 digit integer, 8-byte storage"]
    20: "int8" => Int8: Kind::Simple,
    #[doc="INT2/SMALLINT - -32 thousand to 32 thousand, 2-byte storage"]
    21: "int2" => Int2: Kind::Simple,
    #[doc="INT2VECTOR - array of int2, used in system tables"]
    22: "int2vector" => Int2Vector: Kind::Array(Type::Int2),
    #[doc="INT4/INT - -2 billion to 2 billion integer, 4-byte storage"]
    23: "int4" => Int4: Kind::Simple,
    #[doc="REGPROC - registered procedure"]
    24: "regproc" => Regproc: Kind::Simple,
    #[doc="TEXT - variable-length string, no limit specified"]
    25: "text" => Text: Kind::Simple,
    #[doc="OID - object identifier(oid), maximum 4 billion"]
    26: "oid" => Oid: Kind::Simple,
    #[doc="TID - (block, offset), physical location of tuple"]
    27: "tid" => Tid: Kind::Simple,
    #[doc="XID - transaction id"]
    28: "xid" => Xid: Kind::Simple,
    #[doc="CID - command identifier type, sequence in transaction id"]
    29: "cid" => Cid: Kind::Simple,
    #[doc="OIDVECTOR - array of oids, used in system tables"]
    30: "oidvector" => OidVector: Kind::Array(Type::Oid),
    #[doc="PG_TYPE"]
    71: "pg_type" => PgType: Kind::Simple,
    #[doc="PG_ATTRIBUTE"]
    75: "pg_attribute" => PgAttribute: Kind::Simple,
    #[doc="PG_PROC"]
    81: "pg_proc" => PgProc: Kind::Simple,
    #[doc="PG_CLASS"]
    83: "pg_class" => PgClass: Kind::Simple,
    #[doc="JSON"]
    114: "json" => Json: Kind::Simple,
    #[doc="XML - XML content"]
    142: "xml" => Xml: Kind::Simple,
    #[doc="XML[]"]
    143: "_xml" => XmlArray: Kind::Array(Type::Xml),
    #[doc="PG_NODE_TREE - string representing an internal node tree"]
    194: "pg_node_tree" => PgNodeTree: Kind::Simple,
    #[doc="JSON[]"]
    199: "_json" => JsonArray: Kind::Array(Type::Json),
    #[doc="SMGR - storage manager"]
    210: "smgr" => Smgr: Kind::Simple,
    #[doc="POINT - geometric point '(x, y)'"]
    600: "point" => Point: Kind::Simple,
    #[doc="LSEG - geometric line segment '(pt1,pt2)'"]
    601: "lseg" => Lseg: Kind::Simple,
    #[doc="PATH - geometric path '(pt1,...)'"]
    602: "path" => Path: Kind::Simple,
    #[doc="BOX - geometric box '(lower left,upper right)'"]
    603: "box" => Box: Kind::Simple,
    #[doc="POLYGON - geometric polygon '(pt1,...)'"]
    604: "polygon" => Polygon: Kind::Simple,
    #[doc="LINE - geometric line"]
    628: "line" => Line: Kind::Simple,
    #[doc="LINE[]"]
    629: "_line" => LineArray: Kind::Array(Type::Line),
    #[doc="CIDR - network IP address/netmask, network address"]
    650: "cidr" => Cidr: Kind::Simple,
    #[doc="CIDR[]"]
    651: "_cidr" => CidrArray: Kind::Array(Type::Cidr),
    #[doc="FLOAT4/REAL - single-precision floating point number, 4-byte storage"]
    700: "float4" => Float4: Kind::Simple,
    #[doc="FLOAT8/DOUBLE PRECISION - double-precision floating point number, 8-byte storage"]
    701: "float8" => Float8: Kind::Simple,
    #[doc="ABSTIME - absolute, limited-range date and time (Unix system time)"]
    702: "abstime" => Abstime: Kind::Simple,
    #[doc="RELTIME - relative, limited-range date and time (Unix delta time)"]
    703: "reltime" => Reltime: Kind::Simple,
    #[doc="TINTERVAL - (abstime,abstime), time interval"]
    704: "tinterval" => Tinterval: Kind::Simple,
    #[doc="UNKNOWN"]
    705: "unknown" => Unknown: Kind::Simple,
    #[doc="CIRCLE - geometric circle '(center,radius)'"]
    718: "circle" => Circle: Kind::Simple,
    #[doc="CIRCLE[]"]
    719: "_circle" => CircleArray: Kind::Array(Type::Circle),
    #[doc="MONEY - monetary amounts, $d,ddd.cc"]
    790: "money" => Money: Kind::Simple,
    #[doc="MONEY[]"]
    791: "_money" => MoneyArray: Kind::Array(Type::Money),
    #[doc="MACADDR - XX:XX:XX:XX:XX:XX, MAC address"]
    829: "macaddr" => Macaddr: Kind::Simple,
    #[doc="INET - IP address/netmask, host address, netmask optional"]
    869: "inet" => Inet: Kind::Simple,
    #[doc="BOOL[]"]
    1000: "_bool" => BoolArray: Kind::Array(Type::Bool),
    #[doc="BYTEA[]"]
    1001: "_bytea" => ByteaArray: Kind::Array(Type::Bytea),
    #[doc="\"char\"[]"]
    1002: "_char" => CharArray: Kind::Array(Type::Char),
    #[doc="NAME[]"]
    1003: "_name" => NameArray: Kind::Array(Type::Name),
    #[doc="INT2[]"]
    1005: "_int2" => Int2Array: Kind::Array(Type::Int2),
    #[doc="INT2VECTOR[]"]
    1006: "_int2vector" => Int2VectorArray: Kind::Array(Type::Int2Vector),
    #[doc="INT4[]"]
    1007: "_int4" => Int4Array: Kind::Array(Type::Int4),
    #[doc="REGPROC[]"]
    1008: "_regproc" => RegprocArray: Kind::Array(Type::Regproc),
    #[doc="TEXT[]"]
    1009: "_text" => TextArray: Kind::Array(Type::Text),
    #[doc="TID[]"]
    1010: "_tid" => TidArray: Kind::Array(Type::Tid),
    #[doc="XID[]"]
    1011: "_xid" => XidArray: Kind::Array(Type::Xid),
    #[doc="CID[]"]
    1012: "_cid" => CidArray: Kind::Array(Type::Cid),
    #[doc="OIDVECTOR[]"]
    1013: "_oidvector" => OidVectorArray: Kind::Array(Type::OidVector),
    #[doc="BPCHAR[]"]
    1014: "_bpchar" => BpcharArray: Kind::Array(Type::Bpchar),
    #[doc="VARCHAR[]"]
    1015: "_varchar" => VarcharArray: Kind::Array(Type::Varchar),
    #[doc="INT8[]"]
    1016: "_int8" => Int8Array: Kind::Array(Type::Int8),
    #[doc="POINT[]"]
    1017: "_point" => PointArray: Kind::Array(Type::Point),
    #[doc="LSEG[]"]
    1018: "_lseg" => LsegArray: Kind::Array(Type::Lseg),
    #[doc="PATH[]"]
    1019: "_path" => PathArray: Kind::Array(Type::Path),
    #[doc="BOX[]"]
    1020: "_box" => BoxArray: Kind::Array(Type::Box),
    #[doc="FLOAT4[]"]
    1021: "_float4" => Float4Array: Kind::Array(Type::Float4),
    #[doc="FLOAT8[]"]
    1022: "_float8" => Float8Array: Kind::Array(Type::Float8),
    #[doc="ABSTIME[]"]
    1023: "_abstime" => AbstimeArray: Kind::Array(Type::Abstime),
    #[doc="RELTIME[]"]
    1024: "_reltime" => ReltimeArray: Kind::Array(Type::Reltime),
    #[doc="TINTERVAL[]"]
    1025: "_tinterval" => TintervalArray: Kind::Array(Type::Tinterval),
    #[doc="POLYGON[]"]
    1027: "_polygon" => PolygonArray: Kind::Array(Type::Polygon),
    #[doc="OID[]"]
    1028: "_oid" => OidArray: Kind::Array(Type::Oid),
    #[doc="ACLITEM - access control list"]
    1033: "aclitem" => Aclitem: Kind::Simple,
    #[doc="ACLITEM[]"]
    1034: "_aclitem" => AclitemArray: Kind::Array(Type::Aclitem),
    #[doc="MACADDR[]"]
    1040: "_macaddr" => MacaddrArray: Kind::Array(Type::Macaddr),
    #[doc="INET[]"]
    1041: "_inet" => InetArray: Kind::Array(Type::Inet),
    #[doc="BPCHAR - char(length), blank-padded string, fixed storage length"]
    1042: "bpchar" => Bpchar: Kind::Simple,
    #[doc="VARCHAR - varchar(length), non-blank-padded string, variable storage length"]
    1043: "varchar" => Varchar: Kind::Simple,
    #[doc="DATE - date"]
    1082: "date" => Date: Kind::Simple,
    #[doc="TIME - time of day"]
    1083: "time" => Time: Kind::Simple,
    #[doc="TIMESTAMP - date and time"]
    1114: "timestamp" => Timestamp: Kind::Simple,
    #[doc="TIMESTAMP[]"]
    1115: "_timestamp" => TimestampArray: Kind::Array(Type::Timestamp),
    #[doc="DATE[]"]
    1182: "_date" => DateArray: Kind::Array(Type::Date),
    #[doc="TIME[]"]
    1183: "_time" => TimeArray: Kind::Array(Type::Time),
    #[doc="TIMESTAMPTZ - date and time with time zone"]
    1184: "timestamptz" => TimestampTZ: Kind::Simple,
    #[doc="TIMESTAMPTZ[]"]
    1185: "_timestamptz" => TimestampTZArray: Kind::Array(Type::TimestampTZ),
    #[doc="INTERVAL - @ &lt;number&gt; &lt;units&gt;, time interval"]
    1186: "interval" => Interval: Kind::Simple,
    #[doc="INTERVAL[]"]
    1187: "_interval" => IntervalArray: Kind::Array(Type::Interval),
    #[doc="NUMERIC[]"]
    1231: "_numeric" => NumericArray: Kind::Array(Type::Numeric),
    #[doc="CSTRING[]"]
    1263: "_cstring" => CstringArray: Kind::Array(Type::Cstring),
    #[doc="TIMETZ - time of day with time zone"]
    1266: "timetz" => Timetz: Kind::Simple,
    #[doc="TIMETZ[]"]
    1270: "_timetz" => TimetzArray: Kind::Array(Type::Timetz),
    #[doc="BIT - fixed-length bit string"]
    1560: "bit" => Bit: Kind::Simple,
    #[doc="BIT[]"]
    1561: "_bit" => BitArray: Kind::Array(Type::Bit),
    #[doc="VARBIT - variable-length bit string"]
    1562: "varbit" => Varbit: Kind::Simple,
    #[doc="VARBIT[]"]
    1563: "_varbit" => VarbitArray: Kind::Array(Type::Varbit),
    #[doc="NUMERIC - numeric(precision, decimal), arbitrary precision number"]
    1700: "numeric" => Numeric: Kind::Simple,
    #[doc="REFCURSOR - reference to cursor (portal name)"]
    1790: "refcursor" => Refcursor: Kind::Simple,
    #[doc="REFCURSOR[]"]
    2201: "_refcursor" => RefcursorArray: Kind::Array(Type::Refcursor),
    #[doc="REGPROCEDURE - registered procedure (with args)"]
    2202: "regprocedure" => Regprocedure: Kind::Simple,
    #[doc="REGOPER - registered operator"]
    2203: "regoper" => Regoper: Kind::Simple,
    #[doc="REGOPERATOR - registered operator (with args)"]
    2204: "regoperator" => Regoperator: Kind::Simple,
    #[doc="REGCLASS - registered class"]
    2205: "regclass" => Regclass: Kind::Simple,
    #[doc="REGTYPE - registered type"]
    2206: "regtype" => Regtype: Kind::Simple,
    #[doc="REGPROCEDURE[]"]
    2207: "_regprocedure" => RegprocedureArray: Kind::Array(Type::Regprocedure),
    #[doc="REGOPER[]"]
    2208: "_regoper" => RegoperArray: Kind::Array(Type::Regoper),
    #[doc="REGOPERATOR[]"]
    2209: "_regoperator" => RegoperatorArray: Kind::Array(Type::Regoperator),
    #[doc="REGCLASS[]"]
    2210: "_regclass" => RegclassArray: Kind::Array(Type::Regclass),
    #[doc="REGTYPE[]"]
    2211: "_regtype" => RegtypeArray: Kind::Array(Type::Regtype),
    #[doc="RECORD"]
    2249: "record" => Record: Kind::Simple,
    #[doc="CSTRING"]
    2275: "cstring" => Cstring: Kind::Simple,
    #[doc="ANY"]
    2276: "any" => Any: Kind::Simple,
    #[doc="ANYARRAY"]
    2277: "anyarray" => AnyArray: Kind::Array(Type::Any),
    #[doc="VOID"]
    2278: "void" => Void: Kind::Simple,
    #[doc="TRIGGER"]
    2279: "trigger" => Trigger: Kind::Simple,
    #[doc="LANGUAGE_HANDLER"]
    2280: "language_handler" => LanguageHandler: Kind::Simple,
    #[doc="INTERNAL"]
    2281: "internal" => Internal: Kind::Simple,
    #[doc="OPAQUE"]
    2282: "opaque" => Opaque: Kind::Simple,
    #[doc="ANYELEMENT"]
    2283: "anyelement" => Anyelement: Kind::Simple,
    #[doc="RECORD[]"]
    2287: "_record" => RecordArray: Kind::Array(Type::Record),
    #[doc="ANYNONARRAY"]
    2776: "anynonarray" => Anynonarray: Kind::Simple,
    #[doc="TXID_SNAPSHOT[]"]
    2949: "_txid_snapshot" => TxidSnapshotArray: Kind::Array(Type::TxidSnapshot),
    #[doc="UUID - UUID datatype"]
    2950: "uuid" => Uuid: Kind::Simple,
    #[doc="TXID_SNAPSHOT - txid snapshot"]
    2970: "txid_snapshot" => TxidSnapshot: Kind::Simple,
    #[doc="UUID[]"]
    2951: "_uuid" => UuidArray: Kind::Array(Type::Uuid),
    #[doc="FDW_HANDLER"]
    3115: "fdw_handler" => FdwHandler: Kind::Simple,
    #[doc="PG_LSN - PostgreSQL LSN datatype"]
    3220: "pg_lsn" => PgLsn: Kind::Simple,
    #[doc="PG_LSN[]"]
    3221: "_pg_lsn" => PgLsnArray: Kind::Array(Type::PgLsn),
    #[doc="ANYENUM"]
    3500: "anyenum" => Anyenum: Kind::Simple,
    #[doc="TSVECTOR - text representation for text search"]
    3614: "tsvector" => Tsvector: Kind::Simple,
    #[doc="TSQUERY - query representation for text search"]
    3615: "tsquery" => Tsquery: Kind::Simple,
    #[doc="GTSVECTOR - GiST index internal text representation for text search"]
    3642: "gtsvector" => Gtsvector: Kind::Simple,
    #[doc="TSVECTOR[]"]
    3643: "_tsvector" => TsvectorArray: Kind::Array(Type::Tsvector),
    #[doc="GTSVECTOR[]"]
    3644: "_gtsvector" => GtsvectorArray: Kind::Array(Type::Gtsvector),
    #[doc="TSQUERY[]"]
    3645: "_tsquery" => TsqueryArray: Kind::Array(Type::Tsquery),
    #[doc="REGCONFIG - registered text search configuration"]
    3734: "regconfig" => Regconfig: Kind::Simple,
    #[doc="REGCONFIG[]"]
    3735: "_regconfig" => RegconfigArray: Kind::Array(Type::Regconfig),
    #[doc="REGDICTIONARY - registered text search dictionary"]
    3769: "regdictionary" => Regdictionary: Kind::Simple,
    #[doc="REGDICTIONARY[]"]
    3770: "_regdictionary" => RegdictionaryArray: Kind::Array(Type::Regdictionary),
    #[doc="JSONB"]
    3802: "jsonb" => Jsonb: Kind::Simple,
    #[doc="ANYRANGE"]
    3831: "anyrange" => Anyrange: Kind::Simple,
    #[doc="JSONB[]"]
    3807: "_jsonb" => JsonbArray: Kind::Array(Type::Jsonb),
    #[doc="INT4RANGE - range of integers"]
    3904: "int4range" => Int4Range: Kind::Range(Type::Int4),
    #[doc="INT4RANGE[]"]
    3905: "_int4range" => Int4RangeArray: Kind::Array(Type::Int4Range),
    #[doc="NUMRANGE - range of numerics"]
    3906: "numrange" => NumRange: Kind::Range(Type::Numeric),
    #[doc="NUMRANGE[]"]
    3907: "_numrange" => NumRangeArray: Kind::Array(Type::NumRange),
    #[doc="TSRANGE - range of timestamps without time zone"]
    3908: "tsrange" => TsRange: Kind::Range(Type::Timestamp),
    #[doc="TSRANGE[]"]
    3909: "_tsrange" => TsRangeArray: Kind::Array(Type::TsRange),
    #[doc="TSTZRANGE - range of timestamps with time zone"]
    3910: "tstzrange" => TstzRange: Kind::Range(Type::TimestampTZ),
    #[doc="TSTZRANGE[]"]
    3911: "_tstzrange" => TstzRangeArray: Kind::Array(Type::TstzRange),
    #[doc="DATERANGE - range of dates"]
    3912: "daterange" => DateRange: Kind::Range(Type::Date),
    #[doc="DATERANGE[]"]
    3913: "_daterange" => DateRangeArray: Kind::Array(Type::DateRange),
    #[doc="INT8RANGE - range of bigints"]
    3926: "int8range" => Int8Range: Kind::Range(Type::Int8),
    #[doc="INT8RANGE[]"]
    3927: "_int8range" => Int8RangeArray: Kind::Array(Type::Int8Range),
    #[doc="EVENT_TRIGGER"]
    3838: "event_trigger" => EventTrigger: Kind::Simple
}

/// Information about an unknown type.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Other {
    name: String,
    oid: Oid,
    kind: Kind,
    schema: String,
}

impl OtherNew for Other {
    fn new(name: String, oid: Oid, kind: Kind, schema: String) -> Other {
        Other {
            name: name,
            oid: oid,
            kind: kind,
            schema: schema,
        }
    }
}

impl Other {
    /// The name of the type.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The OID of this type.
    pub fn oid(&self) -> Oid {
        self.oid
    }

    /// The kind of this type.
    pub fn kind(&self) -> &Kind {
        &self.kind
    }

    /// The schema of this type.
    pub fn schema(&self) -> &str {
        &self.schema
    }
}

/// An error indicating that a `NULL` Postgres value was passed to a `FromSql`
/// implementation that does not support `NULL` values.
#[derive(Debug, Clone, Copy)]
pub struct WasNull;

impl fmt::Display for WasNull {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.write_str(error::Error::description(self))
    }
}

impl error::Error for WasNull {
    fn description(&self) -> &str {
        "a Postgres value was `NULL`"
    }
}

/// A trait for types that can be created from a Postgres value.
///
/// # Types
///
/// The following implementations are provided by this crate, along with the
/// corresponding Postgres types:
///
/// | Rust type                                   | Postgres type(s)               |
/// |---------------------------------------------|--------------------------------|
/// | bool                                        | BOOL                           |
/// | i8                                          | "char"                         |
/// | i16                                         | SMALLINT, SMALLSERIAL          |
/// | i32                                         | INT, SERIAL                    |
/// | u32                                         | OID                            |
/// | i64                                         | BIGINT, BIGSERIAL              |
/// | f32                                         | REAL                           |
/// | f64                                         | DOUBLE PRECISION               |
/// | String                                      | VARCHAR, CHAR(n), TEXT, CITEXT |
/// | Vec&lt;u8&gt;                               | BYTEA                          |
/// | HashMap&lt;String, Option&lt;String&gt;&gt; | HSTORE                         |
///
/// In addition, some implementations are provided for types in third party
/// crates. These are disabled by default; to opt into one of these
/// implementations, activate the Cargo feature corresponding to the crate's
/// name. For example, the `serde_json` feature enables the implementation for
/// the `serde_json::Value` type.
///
/// | Rust type                           | Postgres type(s)                    |
/// |-------------------------------------|-------------------------------------|
/// | serialize::json::Json               | JSON, JSONB                         |
/// | serde_json::Value                   | JSON, JSONB                         |
/// | time::Timespec                      | TIMESTAMP, TIMESTAMP WITH TIME ZONE |
/// | chrono::NaiveDateTime               | TIMESTAMP                           |
/// | chrono::DateTime&lt;UTC&gt;         | TIMESTAMP WITH TIME ZONE            |
/// | chrono::DateTime&lt;Local&gt;       | TIMESTAMP WITH TIME ZONE            |
/// | chrono::DateTime&lt;FixedOffset&gt; | TIMESTAMP WITH TIME ZONE            |
/// | chrono::NaiveDate                   | DATE                                |
/// | chrono::NaiveTime                   | TIME                                |
/// | uuid::Uuid                          | UUID                                |
///
/// # Nullability
///
/// In addition to the types listed above, `FromSql` is implemented for
/// `Option<T>` where `T` implements `FromSql`. An `Option<T>` represents a
/// nullable Postgres value.
pub trait FromSql: Sized {
    /// ### Deprecated
    fn from_sql_nullable<R: Read>(ty: &Type, raw: Option<&mut R>, ctx: &SessionInfo)
                                  -> Result<Self> {
        match raw {
            Some(raw) => FromSql::from_sql(ty, raw, ctx),
            None => FromSql::from_sql_null(ty, ctx),
        }
    }

    /// Creates a new value of this type from a `Read`er of the binary format
    /// of the specified Postgres `Type`.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, ctx: &SessionInfo) -> Result<Self>;

    /// Creates a new value of this type from a `NULL` SQL value.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    ///
    /// The default implementation returns
    /// `Err(Error::Conversion(Box::new(WasNull))`.
    #[allow(unused_variables)]
    fn from_sql_null(ty: &Type, ctx: &SessionInfo) -> Result<Self> {
        Err(Error::Conversion(Box::new(WasNull)))
    }

    /// Determines if a value of this type can be created from the specified
    /// Postgres `Type`.
    fn accepts(ty: &Type) -> bool;
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, ctx: &SessionInfo) -> Result<Option<T>> {
        <T as FromSql>::from_sql(ty, raw, ctx).map(Some)
    }

    fn from_sql_null(_: &Type, _: &SessionInfo) -> Result<Option<T>> {
        Ok(None)
    }

    fn accepts(ty: &Type) -> bool {
        <T as FromSql>::accepts(ty)
    }
}

impl FromSql for bool {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo) -> Result<bool> {
        Ok(try!(raw.read_u8()) != 0)
    }

    accepts!(Type::Bool);
}

impl FromSql for Vec<u8> {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo) -> Result<Vec<u8>> {
        let mut buf = vec![];
        try!(raw.read_to_end(&mut buf));
        Ok(buf)
    }

    accepts!(Type::Bytea);
}

impl FromSql for String {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo) -> Result<String> {
        let mut buf = vec![];
        try!(raw.read_to_end(&mut buf));
        String::from_utf8(buf).map_err(|err| Error::Conversion(Box::new(err)))
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::Varchar | Type::Text | Type::Bpchar | Type::Name => true,
            Type::Other(ref u) if u.name() == "citext" => true,
            _ => false,
        }
    }
}

impl FromSql for i8 {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo) -> Result<i8> {
        Ok(try!(raw.read_i8()))
    }

    accepts!(Type::Char);
}

macro_rules! primitive_from {
    ($t:ty, $f:ident, $($expected:pat),+) => {
        impl FromSql for $t {
            fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo) -> Result<$t> {
                Ok(try!(raw.$f::<BigEndian>()))
            }

            accepts!($($expected),+);
        }
    }
}

primitive_from!(i16, read_i16, Type::Int2);
primitive_from!(i32, read_i32, Type::Int4);
primitive_from!(u32, read_u32, Type::Oid);
primitive_from!(i64, read_i64, Type::Int8);
primitive_from!(f32, read_f32, Type::Float4);
primitive_from!(f64, read_f64, Type::Float8);

impl FromSql for HashMap<String, Option<String>> {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo)
                         -> Result<HashMap<String, Option<String>>> {
        let mut map = HashMap::new();

        let count = try!(raw.read_i32::<BigEndian>());

        for _ in 0..count {
            let key_len = try!(raw.read_i32::<BigEndian>());
            let mut key = vec![0; key_len as usize];
            try!(util::read_all(raw, &mut key));
            let key = match String::from_utf8(key) {
                Ok(key) => key,
                Err(err) => return Err(Error::Conversion(Box::new(err))),
            };

            let val_len = try!(raw.read_i32::<BigEndian>());
            let val = if val_len < 0 {
                None
            } else {
                let mut val = vec![0; val_len as usize];
                try!(util::read_all(raw, &mut val));
                match String::from_utf8(val) {
                    Ok(val) => Some(val),
                    Err(err) => return Err(Error::Conversion(Box::new(err))),
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
///
/// # Types
///
/// The following implementations are provided by this crate, along with the
/// corresponding Postgres types:
///
/// | Rust type                                   | Postgres type(s)               |
/// |---------------------------------------------|--------------------------------|
/// | bool                                        | BOOL                           |
/// | i8                                          | "char"                         |
/// | i16                                         | SMALLINT, SMALLSERIAL          |
/// | i32                                         | INT, SERIAL                    |
/// | u32                                         | OID                            |
/// | i64                                         | BIGINT, BIGSERIAL              |
/// | f32                                         | REAL                           |
/// | f64                                         | DOUBLE PRECISION               |
/// | String                                      | VARCHAR, CHAR(n), TEXT, CITEXT |
/// | &str                                        | VARCHAR, CHAR(n), TEXT, CITEXT |
/// | Vec&lt;u8&gt;                               | BYTEA                          |
/// | &[u8]                                       | BYTEA                          |
/// | HashMap&lt;String, Option&lt;String&gt;&gt; | HSTORE                         |
///
/// In addition, some implementations are provided for types in third party
/// crates. These are disabled by default; to opt into one of these
/// implementations, activate the Cargo feature corresponding to the crate's
/// name. For example, the `serde_json` feature enables the implementation for
/// the `serde_json::Value` type.
///
/// | Rust type                           | Postgres type(s)                    |
/// |-------------------------------------|-------------------------------------|
/// | serialize::json::Json               | JSON, JSONB                         |
/// | serde_json::Value                   | JSON, JSONB                         |
/// | time::Timespec                      | TIMESTAMP, TIMESTAMP WITH TIME ZONE |
/// | chrono::NaiveDateTime               | TIMESTAMP                           |
/// | chrono::DateTime&lt;UTC&gt;         | TIMESTAMP WITH TIME ZONE            |
/// | chrono::DateTime&lt;Local&gt;       | TIMESTAMP WITH TIME ZONE            |
/// | chrono::DateTime&lt;FixedOffset&gt; | TIMESTAMP WITH TIME ZONE            |
/// | chrono::NaiveDate                   | DATE                                |
/// | chrono::NaiveTime                   | TIME                                |
/// | uuid::Uuid                          | UUID                                |
///
/// # Nullability
///
/// In addition to the types listed above, `ToSql` is implemented for
/// `Option<T>` where `T` implements `ToSql`. An `Option<T>` represents a
/// nullable Postgres value.
pub trait ToSql: fmt::Debug {
    /// Converts the value of `self` into the binary format of the specified
    /// Postgres `Type`, writing it to `out`.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    ///
    /// The return value indicates if this value should be represented as
    /// `NULL`. If this is the case, implementations **must not** write
    /// anything to `out`.
    fn to_sql<W: ?Sized>(&self, ty: &Type, out: &mut W, ctx: &SessionInfo) -> Result<IsNull>
            where Self: Sized, W: Write;

    /// Determines if a value of this type can be converted to the specified
    /// Postgres `Type`.
    fn accepts(ty: &Type) -> bool where Self: Sized;

    /// An adaptor method used internally by Rust-Postgres.
    ///
    /// *All* implementations of this method should be generated by the
    /// `to_sql_checked!()` macro.
    fn to_sql_checked(&self, ty: &Type, out: &mut Write, ctx: &SessionInfo)
                      -> Result<IsNull>;
}

impl<'a, T> ToSql for &'a T where T: ToSql {
    fn to_sql_checked(&self, ty: &Type, out: &mut Write, ctx: &SessionInfo)
                      -> Result<IsNull> {
        if !<&'a T as ToSql>::accepts(ty) {
            return Err(Error::WrongType(ty.clone()));
        }
        self.to_sql(ty, out, ctx)
    }


    fn to_sql<W: Write + ?Sized>(&self, ty: &Type, out: &mut W, ctx: &SessionInfo) -> Result<IsNull> {
        (*self).to_sql(ty, out, ctx)
    }

    fn accepts(ty: &Type) -> bool { T::accepts(ty) }
}

impl<T: ToSql> ToSql for Option<T> {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, ty: &Type, out: &mut W, ctx: &SessionInfo)
                               -> Result<IsNull> {
        match *self {
            Some(ref val) => val.to_sql(ty, out, ctx),
            None => Ok(IsNull::Yes),
        }
    }

    fn accepts(ty: &Type) -> bool {
        <T as ToSql>::accepts(ty)
    }
}

impl ToSql for bool {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W, _: &SessionInfo)
                               -> Result<IsNull> {
        try!(w.write_u8(*self as u8));
        Ok(IsNull::No)
    }

    accepts!(Type::Bool);
}

impl<'a> ToSql for &'a [u8] {
    // FIXME should use to_sql_checked!() but blocked on rust-lang/rust#24308
    fn to_sql_checked(&self, ty: &Type, out: &mut Write, ctx: &SessionInfo)
                      -> Result<IsNull> {
        if !<&'a [u8] as ToSql>::accepts(ty) {
            return Err(Error::WrongType(ty.clone()));
        }
        self.to_sql(ty, out, ctx)
    }

    fn to_sql<W: Write+?Sized>(&self, _: &Type, w: &mut W, _: &SessionInfo)
                               -> Result<IsNull> {
        try!(w.write_all(*self));
        Ok(IsNull::No)
    }

    accepts!(Type::Bytea);
}

impl ToSql for Vec<u8> {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, ty: &Type, w: &mut W, ctx: &SessionInfo)
                               -> Result<IsNull> {
        <&[u8] as ToSql>::to_sql(&&**self, ty, w, ctx)
    }

    fn accepts(ty: &Type) -> bool {
        <&[u8] as ToSql>::accepts(ty)
    }
}

impl<'a> ToSql for &'a str {
    // FIXME should use to_sql_checked!() but blocked on rust-lang/rust#24308
    fn to_sql_checked(&self, ty: &Type, out: &mut Write, ctx: &SessionInfo)
                      -> Result<IsNull> {
        if !<&'a str as ToSql>::accepts(ty) {
            return Err(Error::WrongType(ty.clone()));
        }
        self.to_sql(ty, out, ctx)
    }

    fn to_sql<W: Write+?Sized>(&self, _: &Type, w: &mut W, _: &SessionInfo)
                               -> Result<IsNull> {
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

    fn to_sql<W: Write+?Sized>(&self, ty: &Type, w: &mut W, ctx: &SessionInfo)
                               -> Result<IsNull> {
        <&str as ToSql>::to_sql(&&**self, ty, w, ctx)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty)
    }
}

impl ToSql for i8 {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W, _: &SessionInfo)
                               -> Result<IsNull> {
        try!(w.write_i8(*self));
        Ok(IsNull::No)
    }

    accepts!(Type::Char);
}

macro_rules! to_primitive {
    ($t:ty, $f:ident, $($expected:pat),+) => {
        impl ToSql for $t {
            to_sql_checked!();

            fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W, _: &SessionInfo)
                                       -> Result<IsNull> {
                try!(w.$f::<BigEndian>(*self));
                Ok(IsNull::No)
            }

            accepts!($($expected),+);
        }
    }
}

to_primitive!(i16, write_i16, Type::Int2);
to_primitive!(i32, write_i32, Type::Int4);
to_primitive!(u32, write_u32, Type::Oid);
to_primitive!(i64, write_i64, Type::Int8);
to_primitive!(f32, write_f32, Type::Float4);
to_primitive!(f64, write_f64, Type::Float8);

impl ToSql for HashMap<String, Option<String>> {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, _: &Type, mut w: &mut W, _: &SessionInfo)
                               -> Result<IsNull> {
        try!(w.write_i32::<BigEndian>(try!(downcast(self.len()))));

        for (key, val) in self {
            try!(w.write_i32::<BigEndian>(try!(downcast(key.len()))));
            try!(w.write_all(key.as_bytes()));

            match *val {
                Some(ref val) => {
                    try!(w.write_i32::<BigEndian>(try!(downcast(val.len()))));
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

fn downcast(len: usize) -> Result<i32> {
    if len > i32::max_value() as usize {
        let err: Box<error::Error+Sync+Send> = "value too large to transmit".into();
        Err(Error::Conversion(err))
    } else {
        Ok(len as i32)
    }
}

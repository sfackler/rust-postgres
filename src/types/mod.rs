//! Traits dealing with Postgres data types

use std::collections::HashMap;
use std::error;
use std::fmt;
use std::io::prelude::*;
use std::sync::Arc;
use byteorder::{ReadBytesExt, WriteBytesExt, BigEndian};

pub use self::types::Type;
pub use self::special::{Date, Timestamp};
use {Result, SessionInfoNew, InnerConnection, OtherNew, WrongTypeNew, FieldNew};
use error::Error;

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
        fn to_sql_checked(&self,
                          ty: &$crate::types::Type,
                          out: &mut ::std::io::Write,
                          ctx: &$crate::types::SessionInfo)
                          -> $crate::Result<$crate::types::IsNull> {
            $crate::types::__to_sql_checked(self, ty, out, ctx)
        }
    }
}

// WARNING: this function is not considered part of this crate's public API.
// It is subject to change at any time.
#[doc(hidden)]
pub fn __to_sql_checked<T>(v: &T, ty: &Type, out: &mut Write, ctx: &SessionInfo) -> Result<IsNull>
    where T: ToSql
{
    if !T::accepts(ty) {
        return Err(Error::Conversion(Box::new(WrongType(ty.clone()))));
    }
    v.to_sql(ty, out, ctx)
}

#[cfg(feature = "with-bit-vec")]
mod bit_vec;
#[cfg(feature = "with-uuid")]
mod uuid;
#[cfg(feature = "with-time")]
mod time;
#[cfg(feature = "with-rustc-serialize")]
mod rustc_serialize;
#[cfg(feature = "with-serde_json")]
mod serde_json;
#[cfg(feature = "with-chrono")]
mod chrono;
#[cfg(feature = "with-eui48")]
mod eui48;

mod special;
mod types;

/// A structure providing information for conversion methods.
pub struct SessionInfo<'a> {
    conn: &'a InnerConnection,
}

impl<'a> SessionInfoNew<'a> for SessionInfo<'a> {
    fn new(conn: &'a InnerConnection) -> SessionInfo<'a> {
        SessionInfo { conn: conn }
    }
}

impl<'a> SessionInfo<'a> {
    /// Returns the value of the specified Postgres backend parameter, such
    /// as `timezone` or `server_version`.
    pub fn parameter(&self, param: &str) -> Option<&'a str> {
        self.conn.parameters.get(param).map(|s| &**s)
    }
}

impl<'a> fmt::Debug for SessionInfo<'a> {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("SessionInfo")
           .field("parameters", &self.conn.parameters)
           .finish()
    }
}

/// A Postgres OID.
pub type Oid = u32;

/// Represents the kind of a Postgres type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Kind {
    /// A simple type like `VARCHAR` or `INTEGER`.
    Simple,
    /// An enumerated type along with its variants.
    Enum(Vec<String>),
    /// A pseudo-type.
    Pseudo,
    /// An array type along with the type of its elements.
    Array(Type),
    /// A range type along with the type of its elements.
    Range(Type),
    /// A domain type along with its underlying type.
    Domain(Type),
    /// A composite type along with information about its fields.
    Composite(Vec<Field>),
    #[doc(hidden)]
    __PseudoPrivateForExtensibility,
}

/// Information about a field of a composite type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    name: String,
    type_: Type,
}

impl Field {
    /// Returns the name of the field.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the field.
    pub fn type_(&self) -> &Type {
        &self.type_
    }
}

impl FieldNew for Field {
    fn new(name: String, type_: Type) -> Field {
        Field {
            name: name,
            type_: type_,
        }
    }
}

/// Information about an unknown type.
#[derive(PartialEq, Eq, Clone)]
pub struct Other(Arc<OtherInner>);

impl fmt::Debug for Other {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        fmt.debug_struct("Other")
           .field("name", &self.0.name)
           .field("oid", &self.0.oid)
           .field("kind", &self.0.kind)
           .field("schema", &self.0.schema)
           .finish()
    }
}

#[derive(PartialEq, Eq)]
struct OtherInner {
    name: String,
    oid: Oid,
    kind: Kind,
    schema: String,
}

impl OtherNew for Other {
    fn new(name: String, oid: Oid, kind: Kind, schema: String) -> Other {
        Other(Arc::new(OtherInner {
            name: name,
            oid: oid,
            kind: kind,
            schema: schema,
        }))
    }
}

impl Other {
    /// The name of the type.
    pub fn name(&self) -> &str {
        &self.0.name
    }

    /// The OID of this type.
    pub fn oid(&self) -> Oid {
        self.0.oid
    }

    /// The kind of this type.
    pub fn kind(&self) -> &Kind {
        &self.0.kind
    }

    /// The schema of this type.
    pub fn schema(&self) -> &str {
        &self.0.schema
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

/// An error indicating that a conversion was attempted between incompatible
/// Rust and Postgres types.
#[derive(Debug)]
pub struct WrongType(Type);

impl fmt::Display for WrongType {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        write!(fmt,
               "cannot convert to or from a Postgres value of type `{}`",
               self.0)
    }
}

impl error::Error for WrongType {
    fn description(&self) -> &str {
        "cannot convert to or from a Postgres value"
    }
}

impl WrongTypeNew for WrongType {
    fn new(ty: Type) -> WrongType {
        WrongType(ty)
    }
}

/// A trait for types that can be created from a Postgres value.
///
/// # Types
///
/// The following implementations are provided by this crate, along with the
/// corresponding Postgres types:
///
/// | Rust type                         | Postgres type(s)               |
/// |-----------------------------------|--------------------------------|
/// | `bool`                            | BOOL                           |
/// | `i8`                              | "char"                         |
/// | `i16`                             | SMALLINT, SMALLSERIAL          |
/// | `i32`                             | INT, SERIAL                    |
/// | `u32`                             | OID                            |
/// | `i64`                             | BIGINT, BIGSERIAL              |
/// | `f32`                             | REAL                           |
/// | `f64`                             | DOUBLE PRECISION               |
/// | `String`                          | VARCHAR, CHAR(n), TEXT, CITEXT |
/// | `Vec<u8>`                         | BYTEA                          |
/// | `HashMap<String, Option<String>>` | HSTORE                         |
///
/// In addition, some implementations are provided for types in third party
/// crates. These are disabled by default; to opt into one of these
/// implementations, activate the Cargo feature corresponding to the crate's
/// name prefixed by `with-`. For example, the `with-serde_json` feature enables
/// the implementation for the `serde_json::Value` type.
///
/// | Rust type                       | Postgres type(s)                    |
/// |---------------------------------|-------------------------------------|
/// | `serialize::json::Json`         | JSON, JSONB                         |
/// | `serde_json::Value`             | JSON, JSONB                         |
/// | `time::Timespec`                | TIMESTAMP, TIMESTAMP WITH TIME ZONE |
/// | `chrono::NaiveDateTime`         | TIMESTAMP                           |
/// | `chrono::DateTime<UTC>`         | TIMESTAMP WITH TIME ZONE            |
/// | `chrono::DateTime<Local>`       | TIMESTAMP WITH TIME ZONE            |
/// | `chrono::DateTime<FixedOffset>` | TIMESTAMP WITH TIME ZONE            |
/// | `chrono::NaiveDate`             | DATE                                |
/// | `chrono::NaiveTime`             | TIME                                |
/// | `eui48::MacAddress`             | MACADDR                             |
/// | `uuid::Uuid`                    | UUID                                |
/// | `bit_vec::BitVec`               | BIT, VARBIT                         |
/// | `eui48::MacAddress`             | MACADDR                             |
///
/// # Nullability
///
/// In addition to the types listed above, `FromSql` is implemented for
/// `Option<T>` where `T` implements `FromSql`. An `Option<T>` represents a
/// nullable Postgres value.
///
/// # Arrays
///
/// `FromSql` is implemented for `Vec<T>` where `T` implements `FromSql`, and
/// corresponds to one-dimensional Postgres arrays.
pub trait FromSql: Sized {
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

impl<T: FromSql> FromSql for Vec<T> {
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, info: &SessionInfo) -> Result<Vec<T>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        let dimensions = try!(raw.read_i32::<BigEndian>());
        if dimensions > 1 {
            return Err(Error::Conversion("array contains too many dimensions".into()));
        }

        let _has_nulls = try!(raw.read_i32::<BigEndian>());
        let _member_oid = try!(raw.read_u32::<BigEndian>());

        if dimensions == 0 {
            return Ok(vec![]);
        }

        let count = try!(raw.read_i32::<BigEndian>());
        let _index_offset = try!(raw.read_i32::<BigEndian>());

        let mut out = Vec::with_capacity(count as usize);
        for _ in 0..count {
            let len = try!(raw.read_i32::<BigEndian>());
            let value = if len < 0 {
                try!(T::from_sql_null(&member_type, info))
            } else {
                let mut raw = raw.take(len as u64);
                try!(T::from_sql(&member_type, &mut raw, info))
            };
            out.push(value)
        }

        Ok(out)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Array(ref inner) => T::accepts(inner),
            _ => false,
        }
    }
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
    fn from_sql<R: Read>(_: &Type,
                         raw: &mut R,
                         _: &SessionInfo)
                         -> Result<HashMap<String, Option<String>>> {
        let mut map = HashMap::new();

        let count = try!(raw.read_i32::<BigEndian>());

        for _ in 0..count {
            let key_len = try!(raw.read_i32::<BigEndian>());
            let mut key = vec![0; key_len as usize];
            try!(raw.read_exact(&mut key));
            let key = match String::from_utf8(key) {
                Ok(key) => key,
                Err(err) => return Err(Error::Conversion(Box::new(err))),
            };

            let val_len = try!(raw.read_i32::<BigEndian>());
            let val = if val_len < 0 {
                None
            } else {
                let mut val = vec![0; val_len as usize];
                try!(raw.read_exact(&mut val));
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
            _ => false,
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
/// | Rust type                         | Postgres type(s)               |
/// |-----------------------------------|--------------------------------|
/// | `bool`                            | BOOL                           |
/// | `i8`                              | "char"                         |
/// | `i16`                             | SMALLINT, SMALLSERIAL          |
/// | `i32`                             | INT, SERIAL                    |
/// | `u32`                             | OID                            |
/// | `i64`                             | BIGINT, BIGSERIAL              |
/// | `f32`                             | REAL                           |
/// | `f64`                             | DOUBLE PRECISION               |
/// | `String`                          | VARCHAR, CHAR(n), TEXT, CITEXT |
/// | `&str`                            | VARCHAR, CHAR(n), TEXT, CITEXT |
/// | `Vec<u8>`                         | BYTEA                          |
/// | `&[u8]`                           | BYTEA                          |
/// | `HashMap<String, Option<String>>` | HSTORE                         |
///
/// In addition, some implementations are provided for types in third party
/// crates. These are disabled by default; to opt into one of these
/// implementations, activate the Cargo feature corresponding to the crate's
/// name prefixed by `with-`. For example, the `with-serde_json` feature enables
/// the implementation for the `serde_json::Value` type.
///
/// | Rust type                       | Postgres type(s)                    |
/// |---------------------------------|-------------------------------------|
/// | `serialize::json::Json`         | JSON, JSONB                         |
/// | `serde_json::Value`             | JSON, JSONB                         |
/// | `time::Timespec`                | TIMESTAMP, TIMESTAMP WITH TIME ZONE |
/// | `chrono::NaiveDateTime`         | TIMESTAMP                           |
/// | `chrono::DateTime<UTC>`         | TIMESTAMP WITH TIME ZONE            |
/// | `chrono::DateTime<Local>`       | TIMESTAMP WITH TIME ZONE            |
/// | `chrono::DateTime<FixedOffset>` | TIMESTAMP WITH TIME ZONE            |
/// | `chrono::NaiveDate`             | DATE                                |
/// | `chrono::NaiveTime`             | TIME                                |
/// | `uuid::Uuid`                    | UUID                                |
/// | `bit_vec::BitVec`               | BIT, VARBIT                         |
/// | `eui48::MacAddress`             | MACADDR                             |
///
/// # Nullability
///
/// In addition to the types listed above, `ToSql` is implemented for
/// `Option<T>` where `T` implements `ToSql`. An `Option<T>` represents a
/// nullable Postgres value.
///
/// # Arrays
///
/// `ToSql` is implemented for `Vec<T>` and `&[T]` where `T` implements `ToSql`,
/// and corresponds to one-dimentional Postgres arrays with an index offset of
/// 1.
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
        where Self: Sized,
              W: Write;

    /// Determines if a value of this type can be converted to the specified
    /// Postgres `Type`.
    fn accepts(ty: &Type) -> bool where Self: Sized;

    /// An adaptor method used internally by Rust-Postgres.
    ///
    /// *All* implementations of this method should be generated by the
    /// `to_sql_checked!()` macro.
    fn to_sql_checked(&self, ty: &Type, out: &mut Write, ctx: &SessionInfo) -> Result<IsNull>;
}

impl<'a, T> ToSql for &'a T
    where T: ToSql
{
    to_sql_checked!();

    fn to_sql<W: Write + ?Sized>(&self,
                                 ty: &Type,
                                 out: &mut W,
                                 ctx: &SessionInfo)
                                 -> Result<IsNull> {
        (*self).to_sql(ty, out, ctx)
    }

    fn accepts(ty: &Type) -> bool {
        T::accepts(ty)
    }
}

impl<T: ToSql> ToSql for Option<T> {
    to_sql_checked!();

    fn to_sql<W: Write + ?Sized>(&self,
                                 ty: &Type,
                                 out: &mut W,
                                 ctx: &SessionInfo)
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

    fn to_sql<W: Write + ?Sized>(&self,
                                 _: &Type,
                                 mut w: &mut W,
                                 _: &SessionInfo)
                                 -> Result<IsNull> {
        try!(w.write_u8(*self as u8));
        Ok(IsNull::No)
    }

    accepts!(Type::Bool);
}

impl<'a, T: ToSql> ToSql for &'a [T] {
    to_sql_checked!();

    fn to_sql<W: Write + ?Sized>(&self, ty: &Type,
                                 mut w: &mut W,
                                 ctx: &SessionInfo)
                                 -> Result<IsNull> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        try!(w.write_i32::<BigEndian>(1)); // number of dimensions
        try!(w.write_i32::<BigEndian>(1)); // has nulls
        try!(w.write_u32::<BigEndian>(member_type.oid()));

        try!(w.write_i32::<BigEndian>(try!(downcast(self.len()))));
        try!(w.write_i32::<BigEndian>(1)); // index offset

        let mut inner_buf = vec![];
        for e in *self {
            match try!(e.to_sql(&member_type, &mut inner_buf, ctx)) {
                IsNull::No => {
                    try!(w.write_i32::<BigEndian>(try!(downcast(inner_buf.len()))));
                    try!(w.write_all(&inner_buf));
                }
                IsNull::Yes => try!(w.write_i32::<BigEndian>(-1)),
            }
            inner_buf.clear();
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Array(ref member) => T::accepts(member),
            _ => false,
        }
    }
}

impl<'a> ToSql for &'a [u8] {
    to_sql_checked!();

    fn to_sql<W: Write + ?Sized>(&self, _: &Type, w: &mut W, _: &SessionInfo) -> Result<IsNull> {
        try!(w.write_all(*self));
        Ok(IsNull::No)
    }

    accepts!(Type::Bytea);
}

impl<T: ToSql> ToSql for Vec<T> {
    to_sql_checked!();

    fn to_sql<W: Write + ?Sized>(&self, ty: &Type, w: &mut W, ctx: &SessionInfo) -> Result<IsNull> {
        <&[T] as ToSql>::to_sql(&&**self, ty, w, ctx)
    }

    fn accepts(ty: &Type) -> bool {
        <&[T] as ToSql>::accepts(ty)
    }
}

impl ToSql for Vec<u8> {
    to_sql_checked!();

    fn to_sql<W: Write + ?Sized>(&self, ty: &Type, w: &mut W, ctx: &SessionInfo) -> Result<IsNull> {
        <&[u8] as ToSql>::to_sql(&&**self, ty, w, ctx)
    }

    fn accepts(ty: &Type) -> bool {
        <&[u8] as ToSql>::accepts(ty)
    }
}

impl<'a> ToSql for &'a str {
    to_sql_checked!();

    fn to_sql<W: Write + ?Sized>(&self, _: &Type, w: &mut W, _: &SessionInfo) -> Result<IsNull> {
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

    fn to_sql<W: Write + ?Sized>(&self, ty: &Type, w: &mut W, ctx: &SessionInfo) -> Result<IsNull> {
        <&str as ToSql>::to_sql(&&**self, ty, w, ctx)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty)
    }
}

impl ToSql for i8 {
    to_sql_checked!();

    fn to_sql<W: Write + ?Sized>(&self,
                                 _: &Type,
                                 mut w: &mut W,
                                 _: &SessionInfo)
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

    fn to_sql<W: Write + ?Sized>(&self,
                                 _: &Type,
                                 mut w: &mut W,
                                 _: &SessionInfo)
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
                None => try!(w.write_i32::<BigEndian>(-1)),
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
        Err(Error::Conversion("value too large to transmit".into()))
    } else {
        Ok(len as i32)
    }
}

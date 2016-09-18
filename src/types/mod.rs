//! Traits dealing with Postgres data types

use fallible_iterator::FallibleIterator;
use postgres_protocol::types::{self, ArrayDimension};
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

#[doc(inline)]
pub use postgres_protocol::Oid;

pub use self::type_gen::Type;
pub use self::special::{Date, Timestamp};
use {SessionInfoNew, InnerConnection, OtherNew, WrongTypeNew, FieldNew};

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
                          out: &mut ::std::vec::Vec<u8>,
                          ctx: &$crate::types::SessionInfo)
                          -> ::std::result::Result<$crate::types::IsNull,
                                                   Box<::std::error::Error +
                                                       ::std::marker::Sync +
                                                       ::std::marker::Send>> {
            $crate::types::__to_sql_checked(self, ty, out, ctx)
        }
    }
}

// WARNING: this function is not considered part of this crate's public API.
// It is subject to change at any time.
#[doc(hidden)]
pub fn __to_sql_checked<T>(v: &T,
                           ty: &Type,
                           out: &mut Vec<u8>,
                           ctx: &SessionInfo)
                           -> Result<IsNull, Box<Error + Sync + Send>>
    where T: ToSql
{
    if !T::accepts(ty) {
        return Err(Box::new(WrongType(ty.clone())));
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
mod type_gen;

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
        fmt.write_str(self.description())
    }
}

impl Error for WasNull {
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

impl Error for WrongType {
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
    /// Creates a new value of this type from a buffer of data of the specified
    /// Postgres `Type` in its binary format.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    fn from_sql(ty: &Type,
                raw: &[u8],
                ctx: &SessionInfo)
                -> Result<Self, Box<Error + Sync + Send>>;

    /// Creates a new value of this type from a `NULL` SQL value.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    ///
    /// The default implementation returns
    /// `Err(Box::new(WasNull))`.
    #[allow(unused_variables)]
    fn from_sql_null(ty: &Type, ctx: &SessionInfo) -> Result<Self, Box<Error + Sync + Send>> {
        Err(Box::new(WasNull))
    }

    /// Determines if a value of this type can be created from the specified
    /// Postgres `Type`.
    fn accepts(ty: &Type) -> bool;
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_sql(ty: &Type,
                raw: &[u8],
                ctx: &SessionInfo)
                -> Result<Option<T>, Box<Error + Sync + Send>> {
        <T as FromSql>::from_sql(ty, raw, ctx).map(Some)
    }

    fn from_sql_null(_: &Type, _: &SessionInfo) -> Result<Option<T>, Box<Error + Sync + Send>> {
        Ok(None)
    }

    fn accepts(ty: &Type) -> bool {
        <T as FromSql>::accepts(ty)
    }
}

impl<T: FromSql> FromSql for Vec<T> {
    fn from_sql(ty: &Type,
                raw: &[u8],
                info: &SessionInfo)
                -> Result<Vec<T>, Box<Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        let array = try!(types::array_from_sql(raw));
        if try!(array.dimensions().count()) > 1 {
            return Err("array contains too many dimensions".into());
        }

        array.values()
            .and_then(|v| {
                match v {
                    Some(v) => T::from_sql(&member_type, v, info),
                    None => T::from_sql_null(&member_type, info),
                }
            })
            .collect()
    }

    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Array(ref inner) => T::accepts(inner),
            _ => false,
        }
    }
}

impl FromSql for Vec<u8> {
    fn from_sql(_: &Type,
                raw: &[u8],
                _: &SessionInfo)
                -> Result<Vec<u8>, Box<Error + Sync + Send>> {
        Ok(types::bytea_from_sql(raw).to_owned())
    }

    accepts!(Type::Bytea);
}

impl FromSql for String {
    fn from_sql(_: &Type, raw: &[u8], _: &SessionInfo) -> Result<String, Box<Error + Sync + Send>> {
        types::text_from_sql(raw).map(|b| b.to_owned())
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::Varchar | Type::Text | Type::Bpchar | Type::Name => true,
            Type::Other(ref u) if u.name() == "citext" => true,
            _ => false,
        }
    }
}

macro_rules! simple_from {
    ($t:ty, $f:ident, $($expected:pat),+) => {
        impl FromSql for $t {
            fn from_sql(_: &Type,
                        raw: &[u8],
                        _: &SessionInfo)
                        -> Result<$t, Box<Error + Sync + Send>> {
                types::$f(raw)
            }

            accepts!($($expected),+);
        }
    }
}

simple_from!(bool, bool_from_sql, Type::Bool);
simple_from!(i8, char_from_sql, Type::Char);
simple_from!(i16, int2_from_sql, Type::Int2);
simple_from!(i32, int4_from_sql, Type::Int4);
simple_from!(u32, oid_from_sql, Type::Oid);
simple_from!(i64, int8_from_sql, Type::Int8);
simple_from!(f32, float4_from_sql, Type::Float4);
simple_from!(f64, float8_from_sql, Type::Float8);

impl FromSql for HashMap<String, Option<String>> {
    fn from_sql(_: &Type,
                raw: &[u8],
                _: &SessionInfo)
                -> Result<HashMap<String, Option<String>>, Box<Error + Sync + Send>> {
        try!(types::hstore_from_sql(raw))
            .map(|(k, v)| (k.to_owned(), v.map(|v| v.to_owned())))
            .collect()
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
    /// Postgres `Type`, appending it to `out`.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    ///
    /// The return value indicates if this value should be represented as
    /// `NULL`. If this is the case, implementations **must not** write
    /// anything to `out`.
    fn to_sql(&self,
              ty: &Type,
              out: &mut Vec<u8>,
              ctx: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>>
        where Self: Sized;

    /// Determines if a value of this type can be converted to the specified
    /// Postgres `Type`.
    fn accepts(ty: &Type) -> bool where Self: Sized;

    /// An adaptor method used internally by Rust-Postgres.
    ///
    /// *All* implementations of this method should be generated by the
    /// `to_sql_checked!()` macro.
    fn to_sql_checked(&self,
                      ty: &Type,
                      out: &mut Vec<u8>,
                      ctx: &SessionInfo)
                      -> Result<IsNull, Box<Error + Sync + Send>>;
}

impl<'a, T> ToSql for &'a T
    where T: ToSql
{
    fn to_sql(&self,
              ty: &Type,
              out: &mut Vec<u8>,
              ctx: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        (*self).to_sql(ty, out, ctx)
    }

    fn accepts(ty: &Type) -> bool {
        T::accepts(ty)
    }

    to_sql_checked!();
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(&self,
              ty: &Type,
              out: &mut Vec<u8>,
              ctx: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        match *self {
            Some(ref val) => val.to_sql(ty, out, ctx),
            None => Ok(IsNull::Yes),
        }
    }

    fn accepts(ty: &Type) -> bool {
        <T as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl<'a, T: ToSql> ToSql for &'a [T] {
    fn to_sql(&self,
              ty: &Type,
              w: &mut Vec<u8>,
              ctx: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        let dimensions = [ArrayDimension {
                              len: try!(downcast(self.len())),
                              lower_bound: 1,
                          }];

        try!(types::array_to_sql(dimensions.iter().cloned(),
                                 true,
                                 member_type.oid(),
                                 self.iter(),
                                 |e, w| {
                                     match try!(e.to_sql(member_type, w, ctx)) {
                                         IsNull::No => Ok(types::IsNull::No),
                                         IsNull::Yes => Ok(types::IsNull::Yes),
                                     }
                                 },
                                 w));
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty.kind() {
            Kind::Array(ref member) => T::accepts(member),
            _ => false,
        }
    }

    to_sql_checked!();
}

impl<'a> ToSql for &'a [u8] {
    fn to_sql(&self,
              _: &Type,
              w: &mut Vec<u8>,
              _: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        types::bytea_to_sql(*self, w);
        Ok(IsNull::No)
    }

    accepts!(Type::Bytea);

    to_sql_checked!();
}

impl<T: ToSql> ToSql for Vec<T> {
    fn to_sql(&self,
              ty: &Type,
              w: &mut Vec<u8>,
              ctx: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        <&[T] as ToSql>::to_sql(&&**self, ty, w, ctx)
    }

    fn accepts(ty: &Type) -> bool {
        <&[T] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl ToSql for Vec<u8> {
    fn to_sql(&self,
              ty: &Type,
              w: &mut Vec<u8>,
              ctx: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        <&[u8] as ToSql>::to_sql(&&**self, ty, w, ctx)
    }

    fn accepts(ty: &Type) -> bool {
        <&[u8] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl<'a> ToSql for &'a str {
    fn to_sql(&self,
              _: &Type,
              w: &mut Vec<u8>,
              _: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        types::text_to_sql(*self, w);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::Varchar | Type::Text | Type::Bpchar | Type::Name => true,
            Type::Other(ref u) if u.name() == "citext" => true,
            _ => false,
        }
    }

    to_sql_checked!();
}

impl ToSql for String {
    fn to_sql(&self,
              ty: &Type,
              w: &mut Vec<u8>,
              ctx: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        <&str as ToSql>::to_sql(&&**self, ty, w, ctx)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

macro_rules! simple_to {
    ($t:ty, $f:ident, $($expected:pat),+) => {
        impl ToSql for $t {
            fn to_sql(&self,
                      _: &Type,
                      w: &mut Vec<u8>,
                      _: &SessionInfo)
                      -> Result<IsNull, Box<Error + Sync + Send>> {
                types::$f(*self, w);
                Ok(IsNull::No)
            }

            accepts!($($expected),+);

            to_sql_checked!();
        }
    }
}

simple_to!(bool, bool_to_sql, Type::Bool);
simple_to!(i8, char_to_sql, Type::Char);
simple_to!(i16, int2_to_sql, Type::Int2);
simple_to!(i32, int4_to_sql, Type::Int4);
simple_to!(u32, oid_to_sql, Type::Oid);
simple_to!(i64, int8_to_sql, Type::Int8);
simple_to!(f32, float4_to_sql, Type::Float4);
simple_to!(f64, float8_to_sql, Type::Float8);

impl ToSql for HashMap<String, Option<String>> {
    fn to_sql(&self,
              _: &Type,
              w: &mut Vec<u8>,
              _: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        try!(types::hstore_to_sql(self.iter().map(|(k, v)| (&**k, v.as_ref().map(|v| &**v))),
                                  w));
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            Type::Other(ref u) if u.name() == "hstore" => true,
            _ => false,
        }
    }

    to_sql_checked!();
}

fn downcast(len: usize) -> Result<i32, Box<Error + Sync + Send>> {
    if len > i32::max_value() as usize {
        Err("value too large to transmit".into())
    } else {
        Ok(len as i32)
    }
}

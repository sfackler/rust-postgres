//! Types.

use fallible_iterator::FallibleIterator;
use postgres_protocol;
use postgres_protocol::types::{self, ArrayDimension};
use std::borrow::Cow;
use std::collections::HashMap;
use std::error::Error;
use std::fmt;
use std::sync::Arc;

use types::type_gen::{Inner, Other};

#[doc(inline)]
pub use postgres_protocol::Oid;

pub use types::type_gen::consts::*;
pub use types::special::{Date, Timestamp};

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
                          out: &mut ::std::vec::Vec<u8>)
                          -> ::std::result::Result<$crate::types::IsNull,
                                                   Box<::std::error::Error +
                                                       ::std::marker::Sync +
                                                       ::std::marker::Send>> {
            $crate::types::__to_sql_checked(self, ty, out)
        }
    }
}

// WARNING: this function is not considered part of this crate's public API.
// It is subject to change at any time.
#[doc(hidden)]
pub fn __to_sql_checked<T>(
    v: &T,
    ty: &Type,
    out: &mut Vec<u8>,
) -> Result<IsNull, Box<Error + Sync + Send>>
where
    T: ToSql,
{
    if !T::accepts(ty) {
        return Err(Box::new(WrongType(ty.clone())));
    }
    v.to_sql(ty, out)
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
#[cfg(feature = "with-geo")]
mod geo;

mod special;
mod type_gen;

/// A Postgres type.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Type(Inner);

impl fmt::Display for Type {
    fn fmt(&self, fmt: &mut fmt::Formatter) -> fmt::Result {
        match self.schema() {
            "public" | "pg_catalog" => {}
            schema => write!(fmt, "{}.", schema)?,
        }
        fmt.write_str(self.name())
    }
}

impl Type {
    // WARNING: this is not considered public API
    #[doc(hidden)]
    pub fn _new(name: String, oid: Oid, kind: Kind, schema: String) -> Type {
        Type(Inner::Other(Arc::new(Other {
            name: name,
            oid: oid,
            kind: kind,
            schema: schema,
        })))
    }

    /// Returns the `Type` corresponding to the provided `Oid` if it
    /// corresponds to a built-in type.
    pub fn from_oid(oid: Oid) -> Option<Type> {
        Inner::from_oid(oid).map(Type)
    }

    /// Returns the OID of the `Type`.
    pub fn oid(&self) -> Oid {
        self.0.oid()
    }

    /// Returns the kind of this type.
    pub fn kind(&self) -> &Kind {
        self.0.kind()
    }

    /// Returns the schema of this type.
    pub fn schema(&self) -> &str {
        match self.0 {
            Inner::Other(ref u) => &u.schema,
            _ => "pg_catalog",
        }
    }

    /// Returns the name of this type.
    pub fn name(&self) -> &str {
        self.0.name()
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

impl Field {
    #[doc(hidden)]
    pub fn new(name: String, type_: Type) -> Field {
        Field {
            name: name,
            type_: type_,
        }
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
        write!(
            fmt,
            "cannot convert to or from a Postgres value of type `{}`",
            self.0
        )
    }
}

impl Error for WrongType {
    fn description(&self) -> &str {
        "cannot convert to or from a Postgres value"
    }
}

impl WrongType {
    #[doc(hidden)]
    pub fn new(ty: Type) -> WrongType {
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
/// | Rust type                         | Postgres type(s)                              |
/// |-----------------------------------|-----------------------------------------------|
/// | `bool`                            | BOOL                                          |
/// | `i8`                              | "char"                                        |
/// | `i16`                             | SMALLINT, SMALLSERIAL                         |
/// | `i32`                             | INT, SERIAL                                   |
/// | `u32`                             | OID                                           |
/// | `i64`                             | BIGINT, BIGSERIAL                             |
/// | `f32`                             | REAL                                          |
/// | `f64`                             | DOUBLE PRECISION                              |
/// | `String`                          | VARCHAR, CHAR(n), TEXT, CITEXT, NAME, UNKNOWN |
/// | `Vec<u8>`                         | BYTEA                                         |
/// | `HashMap<String, Option<String>>` | HSTORE                                        |
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
/// | `chrono::DateTime<Utc>`         | TIMESTAMP WITH TIME ZONE            |
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
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Self, Box<Error + Sync + Send>>;

    /// Creates a new value of this type from a `NULL` SQL value.
    ///
    /// The caller of this method is responsible for ensuring that this type
    /// is compatible with the Postgres `Type`.
    ///
    /// The default implementation returns
    /// `Err(Box::new(WasNull))`.
    #[allow(unused_variables)]
    fn from_sql_null(ty: &Type) -> Result<Self, Box<Error + Sync + Send>> {
        Err(Box::new(WasNull))
    }

    /// A convenience function that delegates to `from_sql` and `from_sql_null` depending on the
    /// value of `raw`.
    fn from_sql_nullable(ty: &Type, raw: Option<&[u8]>) -> Result<Self, Box<Error + Sync + Send>> {
        match raw {
            Some(raw) => Self::from_sql(ty, raw),
            None => Self::from_sql_null(ty),
        }
    }

    /// Determines if a value of this type can be created from the specified
    /// Postgres `Type`.
    fn accepts(ty: &Type) -> bool;
}

impl<T: FromSql> FromSql for Option<T> {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Option<T>, Box<Error + Sync + Send>> {
        <T as FromSql>::from_sql(ty, raw).map(Some)
    }

    fn from_sql_null(_: &Type) -> Result<Option<T>, Box<Error + Sync + Send>> {
        Ok(None)
    }

    fn accepts(ty: &Type) -> bool {
        <T as FromSql>::accepts(ty)
    }
}

impl<T: FromSql> FromSql for Vec<T> {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Vec<T>, Box<Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        let array = types::array_from_sql(raw)?;
        if array.dimensions().count()? > 1 {
            return Err("array contains too many dimensions".into());
        }

        array
            .values()
            .and_then(|v| T::from_sql_nullable(member_type, v))
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
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Vec<u8>, Box<Error + Sync + Send>> {
        Ok(types::bytea_from_sql(raw).to_owned())
    }

    accepts!(BYTEA);
}

impl FromSql for String {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<String, Box<Error + Sync + Send>> {
        types::text_from_sql(raw).map(|b| b.to_owned())
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            VARCHAR | TEXT | BPCHAR | NAME | UNKNOWN => true,
            ref ty if ty.name() == "citext" => true,
            _ => false,
        }
    }
}

macro_rules! simple_from {
    ($t:ty, $f:ident, $($expected:pat),+) => {
        impl FromSql for $t {
            fn from_sql(_: &Type,
                        raw: &[u8])
                        -> Result<$t, Box<Error + Sync + Send>> {
                types::$f(raw)
            }

            accepts!($($expected),+);
        }
    }
}

simple_from!(bool, bool_from_sql, BOOL);
simple_from!(i8, char_from_sql, CHAR);
simple_from!(i16, int2_from_sql, INT2);
simple_from!(i32, int4_from_sql, INT4);
simple_from!(u32, oid_from_sql, OID);
simple_from!(i64, int8_from_sql, INT8);
simple_from!(f32, float4_from_sql, FLOAT4);
simple_from!(f64, float8_from_sql, FLOAT8);

impl FromSql for HashMap<String, Option<String>> {
    fn from_sql(
        _: &Type,
        raw: &[u8],
    ) -> Result<HashMap<String, Option<String>>, Box<Error + Sync + Send>> {
        types::hstore_from_sql(raw)?
            .map(|(k, v)| (k.to_owned(), v.map(str::to_owned)))
            .collect()
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "hstore"
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
/// | Rust type                         | Postgres type(s)                     |
/// |-----------------------------------|--------------------------------------|
/// | `bool`                            | BOOL                                 |
/// | `i8`                              | "char"                               |
/// | `i16`                             | SMALLINT, SMALLSERIAL                |
/// | `i32`                             | INT, SERIAL                          |
/// | `u32`                             | OID                                  |
/// | `i64`                             | BIGINT, BIGSERIAL                    |
/// | `f32`                             | REAL                                 |
/// | `f64`                             | DOUBLE PRECISION                     |
/// | `String`                          | VARCHAR, CHAR(n), TEXT, CITEXT, NAME |
/// | `&str`                            | VARCHAR, CHAR(n), TEXT, CITEXT, NAME |
/// | `Vec<u8>`                         | BYTEA                                |
/// | `&[u8]`                           | BYTEA                                |
/// | `HashMap<String, Option<String>>` | HSTORE                               |
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
/// | `chrono::DateTime<Utc>`         | TIMESTAMP WITH TIME ZONE            |
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
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>>
    where
        Self: Sized;

    /// Determines if a value of this type can be converted to the specified
    /// Postgres `Type`.
    fn accepts(ty: &Type) -> bool
    where
        Self: Sized;

    /// An adaptor method used internally by Rust-Postgres.
    ///
    /// *All* implementations of this method should be generated by the
    /// `to_sql_checked!()` macro.
    fn to_sql_checked(
        &self,
        ty: &Type,
        out: &mut Vec<u8>,
    ) -> Result<IsNull, Box<Error + Sync + Send>>;
}

impl<'a, T> ToSql for &'a T
where
    T: ToSql,
{
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        (*self).to_sql(ty, out)
    }

    fn accepts(ty: &Type) -> bool {
        T::accepts(ty)
    }

    to_sql_checked!();
}

impl<T: ToSql> ToSql for Option<T> {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        match *self {
            Some(ref val) => val.to_sql(ty, out),
            None => Ok(IsNull::Yes),
        }
    }

    fn accepts(ty: &Type) -> bool {
        <T as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl<'a, T: ToSql> ToSql for &'a [T] {
    fn to_sql(&self, ty: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        let member_type = match *ty.kind() {
            Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        let dimension = ArrayDimension {
            len: downcast(self.len())?,
            lower_bound: 1,
        };

        types::array_to_sql(
            Some(dimension),
            true,
            member_type.oid(),
            self.iter(),
            |e, w| match e.to_sql(member_type, w)? {
                IsNull::No => Ok(postgres_protocol::IsNull::No),
                IsNull::Yes => Ok(postgres_protocol::IsNull::Yes),
            },
            w,
        )?;
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
    fn to_sql(&self, _: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        types::bytea_to_sql(*self, w);
        Ok(IsNull::No)
    }

    accepts!(BYTEA);

    to_sql_checked!();
}

impl<T: ToSql> ToSql for Vec<T> {
    fn to_sql(&self, ty: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        <&[T] as ToSql>::to_sql(&&**self, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&[T] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl ToSql for Vec<u8> {
    fn to_sql(&self, ty: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        <&[u8] as ToSql>::to_sql(&&**self, ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&[u8] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl<'a> ToSql for &'a str {
    fn to_sql(&self, _: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        types::text_to_sql(*self, w);
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match *ty {
            VARCHAR | TEXT | BPCHAR | NAME | UNKNOWN => true,
            ref ty if ty.name() == "citext" => true,
            _ => false,
        }
    }

    to_sql_checked!();
}

impl<'a> ToSql for Cow<'a, str> {
    fn to_sql(&self, ty: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        <&str as ToSql>::to_sql(&&self.as_ref(), ty, w)
    }

    fn accepts(ty: &Type) -> bool {
        <&str as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

impl ToSql for String {
    fn to_sql(&self, ty: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        <&str as ToSql>::to_sql(&&**self, ty, w)
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
                      w: &mut Vec<u8>)
                      -> Result<IsNull, Box<Error + Sync + Send>> {
                types::$f(*self, w);
                Ok(IsNull::No)
            }

            accepts!($($expected),+);

            to_sql_checked!();
        }
    }
}

simple_to!(bool, bool_to_sql, BOOL);
simple_to!(i8, char_to_sql, CHAR);
simple_to!(i16, int2_to_sql, INT2);
simple_to!(i32, int4_to_sql, INT4);
simple_to!(u32, oid_to_sql, OID);
simple_to!(i64, int8_to_sql, INT8);
simple_to!(f32, float4_to_sql, FLOAT4);
simple_to!(f64, float8_to_sql, FLOAT8);

impl ToSql for HashMap<String, Option<String>> {
    fn to_sql(&self, _: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        types::hstore_to_sql(
            self.iter().map(|(k, v)| (&**k, v.as_ref().map(|v| &**v))),
            w,
        )?;
        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        ty.name() == "hstore"
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

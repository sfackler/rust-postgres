use std::rt::io::Decorator;
use std::rt::io::extensions::{WriterByteConversions, ReaderByteConversions};
use std::rt::io::mem::{MemWriter, MemReader};
use std::str;

pub type Oid = i32;

// Values from pg_type.h
static BOOLOID: Oid = 16;
static BYTEAOID: Oid = 17;
static INT8OID: Oid = 20;
static INT2OID: Oid = 21;
static INT4OID: Oid = 23;
static TEXTOID: Oid = 25;
static FLOAT4OID: Oid = 700;
static FLOAT8OID: Oid = 701;
static VARCHAROID: Oid = 1043;

pub enum Format {
    Text = 0,
    Binary = 1
}

pub fn result_format(ty: Oid) -> Format {
    match ty {
        BOOLOID |
        BYTEAOID |
        INT8OID |
        INT2OID |
        INT4OID |
        FLOAT4OID |
        FLOAT8OID => Binary,
        _ => Text
    }
}

macro_rules! check_oid(
    ($($expected:ident)|+, $actual:ident) => (
        match $actual {
            $($expected)|+ => (),
            actual => fail!("Invalid Oid %?", actual)
        }
    )
)

pub trait FromSql {
    fn from_sql(ty: Oid, raw: &Option<~[u8]>) -> Self;
}

macro_rules! from_conversions_impl(
    ($oid:ident, $t:ty, $f:ident) => (
        impl FromSql for Option<$t> {
            fn from_sql(ty: Oid, raw: &Option<~[u8]>) -> Option<$t> {
                check_oid!($oid, ty)
                do raw.map |buf| {
                    // TODO change to BufReader when implemented
                    let mut reader = MemReader::new(buf.to_owned());
                    reader.$f()
                }
            }
        }
    )
)

macro_rules! from_option_impl(
    ($t:ty) => (
        impl FromSql for $t {
            fn from_sql(ty: Oid, raw: &Option<~[u8]>) -> $t {
                // FIXME when you can specify Self types properly
                let ret: Option<$t> = FromSql::from_sql(ty, raw);
                ret.unwrap()
            }
        }
    )
)

impl FromSql for Option<bool> {
    fn from_sql(ty: Oid, raw: &Option<~[u8]>) -> Option<bool> {
        check_oid!(BOOLOID, ty)
        do raw.map |buf| {
            buf[0] != 0
        }
    }
}
from_option_impl!(bool)

from_conversions_impl!(INT2OID, i16, read_be_i16_)
from_option_impl!(i16)
from_conversions_impl!(INT4OID, i32, read_be_i32_)
from_option_impl!(i32)
from_conversions_impl!(INT8OID, i64, read_be_i64_)
from_option_impl!(i64)
from_conversions_impl!(FLOAT4OID, f32, read_be_f32_)
from_option_impl!(f32)
from_conversions_impl!(FLOAT8OID, f64, read_be_f64_)
from_option_impl!(f64)

impl FromSql for Option<~str> {
    fn from_sql(ty:Oid, raw: &Option<~[u8]>) -> Option<~str> {
        check_oid!(VARCHAROID | TEXTOID, ty)
        do raw.chain_ref |buf| {
            Some(str::from_bytes(buf.as_slice()))
        }
    }
}
from_option_impl!(~str)

impl FromSql for Option<~[u8]> {
    fn from_sql(ty: Oid, raw: &Option<~[u8]>) -> Option<~[u8]> {
        check_oid!(BYTEAOID, ty)
        raw.clone()
    }
}
from_option_impl!(~[u8])

pub trait ToSql {
    fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>);
}

macro_rules! to_option_impl(
    ($($oid:ident)|+, $t:ty) => (
        impl ToSql for Option<$t> {
            fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>) {
                check_oid!($($oid)|+, ty)

                match *self {
                    None => (Text, None),
                    Some(ref val) => val.to_sql(ty)
                }
            }
        }
    )
)

macro_rules! to_conversions_impl(
    ($($oid:ident)|+, $t:ty, $f:ident) => (
        impl ToSql for $t {
            fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>) {
                check_oid!($($oid)|+, ty)

                let mut writer = MemWriter::new();
                writer.$f(*self);
                (Binary, Some(writer.inner()))
            }
        }
    )
)

impl ToSql for bool {
    fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>) {
        check_oid!(BOOLOID, ty)
        (Binary, Some(~[*self as u8]))
    }
}
to_option_impl!(BOOLOID, bool)

to_conversions_impl!(INT2OID, i16, write_be_i16_)
to_option_impl!(INT2OID, i16)
to_conversions_impl!(INT4OID, i32, write_be_i32_)
to_option_impl!(INT4OID, i32)
to_conversions_impl!(INT8OID, i64, write_be_i64_)
to_option_impl!(INT8OID, i64)
to_conversions_impl!(FLOAT4OID, f32, write_be_f32_)
to_option_impl!(FLOAT4OID, f32)
to_conversions_impl!(FLOAT8OID, f64, write_be_f64_)
to_option_impl!(FLOAT8OID, f64)

impl<'self> ToSql for &'self str {
    fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>) {
        check_oid!(VARCHAROID | TEXTOID, ty)
        (Text, Some(self.as_bytes().to_owned()))
    }
}

to_option_impl!(VARCHAROID | TEXTOID, ~str)

impl<'self> ToSql for Option<&'self str> {
    fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>) {
        check_oid!(VARCHAROID | TEXTOID, ty)
        match *self {
            None => (Text, None),
            Some(val) => val.to_sql(ty)
        }
    }
}

impl<'self> ToSql for &'self [u8] {
    fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>) {
        check_oid!(BYTEAOID, ty)
        (Binary, Some(self.to_owned()))
    }
}

to_option_impl!(BYTEAOID, ~[u8])

impl<'self> ToSql for Option<&'self [u8]> {
    fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>) {
        check_oid!(BYTEAOID, ty)
        match *self {
            None => (Text, None),
            Some(val) => val.to_sql(ty)
        }
    }
}

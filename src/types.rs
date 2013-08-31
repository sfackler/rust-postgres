use std::rt::io::Decorator;
use std::rt::io::extensions::WriterByteConversions;
use std::rt::io::mem::MemWriter;
use std::str;
use std::f32;
use std::f64;

pub type Oid = i32;

// Values from pg_type.h
static BOOLOID: Oid = 16;
static INT8OID: Oid = 20;
static INT2OID: Oid = 21;
static INT4OID: Oid = 23;
static FLOAT4OID: Oid = 700;
static FLOAT8OID: Oid = 701;

pub enum Format {
    Text = 0,
    Binary = 1
}

pub trait FromSql {
    fn from_sql(raw: &Option<~[u8]>) -> Self;
}

macro_rules! from_str_impl(
    ($t:ty) => (
        impl FromSql for Option<$t> {
            fn from_sql(raw: &Option<~[u8]>) -> Option<$t> {
                match *raw {
                    None => None,
                    Some(ref buf) => {
                        let s = str::from_bytes_slice(buf.as_slice());
                        Some(FromStr::from_str(s).unwrap())
                    }
                }
            }
        }
    )
)

macro_rules! from_option_impl(
    ($t:ty) => (
        impl FromSql for $t {
            fn from_sql(raw: &Option<~[u8]>) -> $t {
                // FIXME when you can specify Self types properly
                let ret: Option<$t> = FromSql::from_sql(raw);
                ret.unwrap()
            }
        }
    )
)

impl FromSql for Option<bool> {
    fn from_sql(raw: &Option<~[u8]>) -> Option<bool> {
        match *raw {
            None => None,
            Some(ref buf) => {
                assert_eq!(1, buf.len());
                match buf[0] as char {
                    't' => Some(true),
                    'f' => Some(false),
                    byte => fail!("Invalid byte: %?", byte)
                }
            }
        }
    }
}
from_option_impl!(bool)

from_str_impl!(int)
from_option_impl!(int)
from_str_impl!(i8)
from_option_impl!(i8)
from_str_impl!(i16)
from_option_impl!(i16)
from_str_impl!(i32)
from_option_impl!(i32)
from_str_impl!(i64)
from_option_impl!(i64)
from_str_impl!(uint)
from_option_impl!(uint)
from_str_impl!(u8)
from_option_impl!(u8)
from_str_impl!(u16)
from_option_impl!(u16)
from_str_impl!(u32)
from_option_impl!(u32)
from_str_impl!(u64)
from_option_impl!(u64)

impl FromSql for Option<f32> {
    fn from_sql(raw: &Option<~[u8]>) -> Option<f32> {
        match *raw {
            None => None,
            Some(ref buf) => {
                Some(match str::from_bytes_slice(buf.as_slice()) {
                    "NaN" => f32::NaN,
                    "Infinity" => f32::infinity,
                    "-Infinity" => f32::neg_infinity,
                    str => FromStr::from_str(str).unwrap()
                })
            }
        }
    }
}
from_option_impl!(f32)

impl FromSql for Option<f64> {
    fn from_sql(raw: &Option<~[u8]>) -> Option<f64> {
        match *raw {
            None => None,
            Some(ref buf) => {
                Some(match str::from_bytes_slice(buf.as_slice()) {
                    "NaN" => f64::NaN,
                    "Infinity" => f64::infinity,
                    "-Infinity" => f64::neg_infinity,
                    str => FromStr::from_str(str).unwrap()
                })
            }
        }
    }
}
from_option_impl!(f64)

impl FromSql for Option<~str> {
    fn from_sql(raw: &Option<~[u8]>) -> Option<~str> {
        do raw.chain_ref |buf| {
            Some(str::from_bytes(buf.as_slice()))
        }
    }
}
from_option_impl!(~str)

pub trait ToSql {
    fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>);
}

macro_rules! to_str_impl(
    ($t:ty) => (
        impl ToSql for $t {
            fn to_sql(&self, _ty: Oid) -> (Format, Option<~[u8]>) {
                (Text, Some(self.to_str().into_bytes()))
            }
        }
    )
)

macro_rules! to_option_impl(
    ($t:ty) => (
        impl ToSql for Option<$t> {
            fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>) {
                match *self {
                    None => (Text, None),
                    Some(val) => val.to_sql(ty)
                }
            }
        }
    )
)

macro_rules! to_conversions_impl(
    ($oid:ident, $t:ty, $f:ident) => (
        impl ToSql for $t {
            fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>) {
                if ty == $oid {
                    let mut writer = MemWriter::new();
                    writer.$f(*self);
                    (Binary, Some(writer.inner()))
                } else {
                    (Text, Some(self.to_str().into_bytes()))
                }
            }
        }
    )
)

impl ToSql for bool {
    fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>) {
        if ty == BOOLOID {
            (Binary, Some(~[*self as u8]))
        } else {
            (Text, Some(self.to_str().into_bytes()))
        }
    }
}
to_option_impl!(bool)

to_conversions_impl!(INT2OID, i16, write_be_i16_)
to_option_impl!(i16)
to_conversions_impl!(INT4OID, i32, write_be_i32_)
to_option_impl!(i32)
to_conversions_impl!(INT8OID, i64, write_be_i64_)
to_option_impl!(i64)
to_conversions_impl!(FLOAT4OID, f32, write_be_f32_)
to_option_impl!(f32)
to_conversions_impl!(FLOAT8OID, f64, write_be_f64_)
to_option_impl!(f64)

to_str_impl!(int)
to_option_impl!(int)
to_str_impl!(i8)
to_option_impl!(i8)
to_str_impl!(uint)
to_option_impl!(uint)
to_str_impl!(u8)
to_option_impl!(u8)
to_str_impl!(u16)
to_option_impl!(u16)
to_str_impl!(u32)
to_option_impl!(u32)
to_str_impl!(u64)
to_option_impl!(u64)
to_str_impl!(float)
to_option_impl!(float)

impl<'self> ToSql for &'self str {
    fn to_sql(&self, _ty: Oid) -> (Format, Option<~[u8]>) {
        (Text, Some(self.as_bytes().to_owned()))
    }
}

impl ToSql for Option<~str> {
    fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>) {
        match *self {
            None => (Text, None),
            Some(ref val) => val.to_sql(ty)
        }
    }
}

impl<'self> ToSql for Option<&'self str> {
    fn to_sql(&self, ty: Oid) -> (Format, Option<~[u8]>) {
        match *self {
            None => (Text, None),
            Some(val) => val.to_sql(ty)
        }
    }
}

//! Low level Postgres protocol APIs.
//!
//! This crate implements the low level components of Postgres's communication
//! protocol, including message and value serialization and deserialization.
//! It is designed to be used as a building block by higher level APIs such as
//! `rust-postgres`, and should not typically be used directly.
//!
//! # Note
//!
//! This library assumes that the `client_encoding` backend parameter has been
//! set to `UTF8`. It will most likely not behave properly if that is not the case.
#![doc(html_root_url="https://docs.rs/postgres-protocol/0.3")]
#![warn(missing_docs)]
extern crate base64;
extern crate byteorder;
extern crate bytes;
extern crate fallible_iterator;
extern crate generic_array;
extern crate hmac;
extern crate md5;
extern crate memchr;
extern crate rand;
extern crate sha2;
extern crate stringprep;

use byteorder::{BigEndian, ByteOrder};
use std::io;

pub mod authentication;
pub mod message;
pub mod types;

/// A Postgres OID.
pub type Oid = u32;

/// An enum indicating if a value is `NULL` or not.
pub enum IsNull {
    /// The value is `NULL`.
    Yes,
    /// The value is not `NULL`.
    No,
}

#[inline]
fn write_nullable<F, E>(serializer: F, buf: &mut Vec<u8>) -> Result<(), E>
where
    F: FnOnce(&mut Vec<u8>) -> Result<IsNull, E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.extend_from_slice(&[0; 4]);
    let size = match serializer(buf)? {
        IsNull::No => i32::from_usize(buf.len() - base - 4)?,
        IsNull::Yes => -1,
    };
    BigEndian::write_i32(&mut buf[base..], size);

    Ok(())
}

trait FromUsize: Sized {
    fn from_usize(x: usize) -> Result<Self, io::Error>;
}

macro_rules! from_usize {
    ($t:ty) => {
        impl FromUsize for $t {
            #[inline]
            fn from_usize(x: usize) -> io::Result<$t> {
                if x > <$t>::max_value() as usize {
                    Err(io::Error::new(io::ErrorKind::InvalidInput, "value too large to transmit"))
                } else {
                    Ok(x as $t)
                }
            }
        }
    }
}

from_usize!(i16);
from_usize!(i32);

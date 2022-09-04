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
#![doc(html_root_url = "https://docs.rs/postgres-protocol/0.6")]
#![warn(missing_docs, rust_2018_idioms, clippy::all)]

use byteorder::{BigEndian, ByteOrder};
use bytes::{Bytes, BufMut, BytesMut};
use std::{io, intrinsics::size_of};
use memchr::memchr;
use std::mem;

pub mod authentication;
pub mod escape;
pub mod message;
pub mod password;
pub mod types;

/// A Postgres OID.
pub type Oid = u32;

/// A Postgres Log Sequence Number (LSN).
pub type Lsn = u64;

/// An enum indicating if a value is `NULL` or not.
pub enum IsNull {
    /// The value is `NULL`.
    Yes,
    /// The value is not `NULL`.
    No,
}

fn write_nullable<F, E>(serializer: F, buf: &mut BytesMut) -> Result<(), E>
where
    F: FnOnce(&mut BytesMut) -> Result<IsNull, E>,
    E: From<io::Error>,
{
    let base = buf.len();
    buf.put_i32(0);
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
                    Err(io::Error::new(
                        io::ErrorKind::InvalidInput,
                        "value too large to transmit",
                    ))
                } else {
                    Ok(x as $t)
                }
            }
        }
    };
}

from_usize!(i16);
from_usize!(i32);

pub struct Buffer {
    bytes: bytes::Bytes,
    idx: usize,
}

impl Buffer {
    pub fn new(bytes: bytes::Bytes, idx: usize) -> Self {
        Self { bytes, idx }
    }
}

impl Buffer {
    #[inline]
    pub fn slice(&self) -> &[u8] {
        &self.bytes[self.idx..]
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.slice().is_empty()
    }

    #[inline]
    pub fn read_cstr(&mut self) -> io::Result<Bytes> {
        match memchr(0, self.slice()) {
            Some(pos) => {
                let start = self.idx;
                let end = start + pos;
                let cstr = self.bytes.slice(start..end);
                self.idx = end + 1;
                Ok(cstr)
            }
            None => Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "unexpected EOF",
            )),
        }
    }

    #[inline]
    fn read_by_size(&mut self, size: usize, kind: &str) -> io::Result<Bytes> {
        let start = self.idx;
        let end = start + size + 1;
        match self.bytes.get(start..end) {
            Some(s) => {
                self.idx = end;
                Ok(s.into())
            },
            None => io::Error::new(io::ErrorKind::UnexpectedEOF, format!("Unable to read {}", kind)),
        }
    }

    #[inline]
    pub fn read_u16(&mut self) -> io::Result<u16> {
        self.read_by_size(mem::size_of::<u16>(), "u16")
    }

    #[inline]
    pub fn read_i32(&mut self) -> io::Result<i32> {
        self.read_by_size(mem::size_of::<i32>(), "i32")
    }

    #[inline]
    pub fn read_i16(&mut self) -> io::Result<i32> {
        self.read_by_size(mem::size_of::<i16>(), "i16")
    }

    #[inline]
    pub fn read_byten(&mut self, n: usize) -> io::Result<Bytes> {
        self.read_by_size(1, &format!("byte{}", n))
    }

    #[inline]
    pub fn 

    #[inline]
    pub fn read_all(&mut self) -> Bytes {
        let buf = self.bytes.slice(self.idx..);
        self.idx = self.bytes.len();
        buf
    }
}

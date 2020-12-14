//! Types.
//!
//! This module is a reexport of the `postgres_types` crate.

#[doc(inline)]
pub use postgres_types::*;

/// Log Sequence Number for PostgreSQL Write-Ahead Log (transaction log).
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct Lsn(u64);

impl From<&str> for Lsn {
    fn from(lsn_str: &str) -> Self {
        let split: Vec<&str> = lsn_str.split('/').collect();
        assert_eq!(split.len(), 2);
        let (hi, lo) = (
            u64::from_str_radix(split[0], 16).unwrap(),
            u64::from_str_radix(split[1], 16).unwrap(),
        );
        Lsn((hi << 32) | lo)
    }
}

impl From<u64> for Lsn {
    fn from(lsn_u64: u64) -> Self {
        Lsn(lsn_u64)
    }
}

impl From<Lsn> for u64 {
    fn from(lsn: Lsn) -> u64 {
        lsn.0
    }
}

impl From<Lsn> for String {
    fn from(lsn: Lsn) -> String {
        format!("{:X}/{:X}", lsn.0 >> 32, lsn.0 & 0x00000000ffffffff)
    }
}

extern crate uuid;

use std::io::{MemWriter, BufReader};
use std::io::util::LimitReader;
use std::io::ByRefReader;

use self::uuid::Uuid;
use types::{RawFromSql, FromSql, ToSql, RawToSql, Type, Oid};
use Error::{PgWasNull, PgWrongType, PgBadData};
use types::array::{ArrayBase, DimensionInfo, Array};
use Result;

impl RawFromSql for Uuid {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<Uuid> {
        match Uuid::from_bytes(try!(raw.read_to_end())[]) {
            Some(u) => Ok(u),
            None => Err(PgBadData),
        }
    }
}

from_raw_from_impl!(super::Uuid, Uuid, doc = "requires the \"uuid\" feature")
from_array_impl!(super::UuidArray, Uuid, doc = "requires the \"uuid\" feature")

impl RawToSql for Uuid {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()> {
        Ok(try!(w.write(self.as_bytes())))
    }
}

to_raw_to_impl!(super::Uuid, Uuid, doc = "requires the \"type\" feature")

to_array_impl!(super::UuidArray, Uuid, doc = "requires the \"type\" feature")

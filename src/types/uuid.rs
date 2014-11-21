extern crate uuid;

use self::uuid::Uuid;
use types::{RawFromSql, ToSql, RawToSql, Type};
use Error;
use Result;

impl RawFromSql for Uuid {
    fn raw_from_sql<R: Reader>(raw: &mut R) -> Result<Uuid> {
        match Uuid::from_bytes(try!(raw.read_to_end())[]) {
            Some(u) => Ok(u),
            None => Err(Error::BadData),
        }
    }
}

from_raw_from_impl!(Type::Uuid, Uuid, doc = "requires the \"uuid\" feature")
from_array_impl!(Type::UuidArray, Uuid, doc = "requires the \"uuid\" feature")

impl RawToSql for Uuid {
    fn raw_to_sql<W: Writer>(&self, w: &mut W) -> Result<()> {
        Ok(try!(w.write(self.as_bytes())))
    }
}

to_raw_to_impl!(Type::Uuid, Uuid, doc = "requires the \"uuid\" feature")
to_array_impl!(Type::UuidArray, Uuid, doc = "requires the \"uuid\" feature")

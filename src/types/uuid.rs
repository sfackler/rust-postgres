extern crate uuid;

use self::uuid::Uuid;
use types::{FromSql, ToSql, Type, IsNull};
use Error;
use Result;

impl FromSql for Uuid {
    fn from_sql<R: Reader>(_: &Type, raw: &mut R) -> Result<Uuid> {
        match Uuid::from_bytes(&*try!(raw.read_to_end())) {
            Some(u) => Ok(u),
            None => Err(Error::BadResponse),
        }
    }

    accepts!(Type::Uuid);
}

impl ToSql for Uuid {
    fn to_sql<W: Writer+?Sized>(&self, _: &Type, w: &mut W) -> Result<IsNull> {
        try!(w.write_all(self.as_bytes()));
        Ok(IsNull::No)
    }

    accepts!(Type::Uuid);
    to_sql_checked!();
}

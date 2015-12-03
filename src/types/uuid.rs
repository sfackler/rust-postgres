extern crate uuid;

use std::io::prelude::*;

use self::uuid::Uuid;
use types::{FromSql, ToSql, Type, IsNull, SessionInfo};
use Result;
use util;

impl FromSql for Uuid {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo) -> Result<Uuid> {
        let mut bytes = [0; 16];
        try!(util::read_all(raw, &mut bytes));
        Ok(Uuid::from_bytes(&bytes).unwrap())
    }

    accepts!(Type::Uuid);
}

impl ToSql for Uuid {
    fn to_sql<W: Write + ?Sized>(&self, _: &Type, w: &mut W, _: &SessionInfo) -> Result<IsNull> {
        try!(w.write_all(self.as_bytes()));
        Ok(IsNull::No)
    }

    accepts!(Type::Uuid);
    to_sql_checked!();
}

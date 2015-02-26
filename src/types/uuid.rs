extern crate uuid;

use std::result::Result::{Ok, Err};
use std::marker::Sized;
use std::option::Option::{Some, None};
use std::clone::Clone;

use std::io::prelude::*;

use self::uuid::Uuid;
use types::{FromSql, ToSql, Type, IsNull};
use Error;
use Result;
use util;

impl FromSql for Uuid {
    fn from_sql<R: Read>(_: &Type, raw: &mut R) -> Result<Uuid> {
        let mut bytes = [0; 16];
        try!(util::read_all(raw, &mut bytes));
        match Uuid::from_bytes(&bytes) {
            Some(u) => Ok(u),
            None => Err(Error::BadResponse),
        }
    }

    accepts!(Type::Uuid);
}

impl ToSql for Uuid {
    fn to_sql<W: Write+?Sized>(&self, _: &Type, w: &mut W) -> Result<IsNull> {
        try!(w.write_all(self.as_bytes()));
        Ok(IsNull::No)
    }

    accepts!(Type::Uuid);
    to_sql_checked!();
}

extern crate eui48;

use std::io::prelude::*;

use self::eui48::MacAddress;

use types::{FromSql, ToSql, Type, IsNull, SessionInfo};
use Result;
use util;

impl FromSql for MacAddress {
    fn from_sql<R: Read>(_: &Type, raw: &mut R, _: &SessionInfo) -> Result<MacAddress> {
        let mut bytes = [0; 6];
        try!(util::read_all(raw, &mut bytes));
        Ok(MacAddress::new(bytes))
    }

    accepts!(Type::Macaddr);
}

impl ToSql for MacAddress {
    fn to_sql<W: Write + ?Sized>(&self, _: &Type, w: &mut W, _: &SessionInfo) -> Result<IsNull> {
        try!(w.write_all(self.as_bytes()));
        Ok(IsNull::No)
    }

    accepts!(Type::Macaddr);
    to_sql_checked!();
}

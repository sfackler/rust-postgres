extern crate eui48;

use self::eui48::MacAddress;
use std::error::Error;
use postgres_protocol::types;

use types::{FromSql, ToSql, Type, IsNull};

impl FromSql for MacAddress {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<MacAddress, Box<Error + Sync + Send>> {
        let bytes = types::macaddr_from_sql(raw)?;
        Ok(MacAddress::new(bytes))
    }

    accepts!(Type::Macaddr);
}

impl ToSql for MacAddress {
    fn to_sql(&self, _: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        let mut bytes = [0; 6];
        bytes.copy_from_slice(self.as_bytes());
        types::macaddr_to_sql(bytes, w);
        Ok(IsNull::No)
    }

    accepts!(Type::Macaddr);
    to_sql_checked!();
}

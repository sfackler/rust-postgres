use bytes::BytesMut;
use eui48_04::MacAddress;
use postgres_protocol::types;
use std::error::Error;

use crate::{FromSql, IsNull, ToSql, Type};

impl<'a> FromSql<'a> for MacAddress {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<MacAddress, Box<dyn Error + Sync + Send>> {
        let bytes = types::macaddr_from_sql(raw)?;
        Ok(MacAddress::new(bytes))
    }

    accepts!(MACADDR);
}

impl ToSql for MacAddress {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        let mut bytes = [0; 6];
        bytes.copy_from_slice(self.as_bytes());
        types::macaddr_to_sql(bytes, w);
        Ok(IsNull::No)
    }

    accepts!(MACADDR);
    to_sql_checked!();
}

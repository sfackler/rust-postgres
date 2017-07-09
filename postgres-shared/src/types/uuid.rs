extern crate uuid;

use postgres_protocol::types;
use self::uuid::Uuid;
use std::error::Error;

use types::{FromSql, ToSql, Type, IsNull, UUID};

impl FromSql for Uuid {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Uuid, Box<Error + Sync + Send>> {
        let bytes = types::uuid_from_sql(raw)?;
        Ok(Uuid::from_bytes(&bytes).unwrap())
    }

    accepts!(UUID);
}

impl ToSql for Uuid {
    fn to_sql(&self, _: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        types::uuid_to_sql(*self.as_bytes(), w);
        Ok(IsNull::No)
    }

    accepts!(UUID);
    to_sql_checked!();
}

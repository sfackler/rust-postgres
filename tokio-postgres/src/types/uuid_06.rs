use postgres_protocol::types;
use std::error::Error;
use uuid_06::Uuid;

use crate::types::{FromSql, IsNull, ToSql, Type};

impl<'a> FromSql<'a> for Uuid {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Uuid, Box<dyn Error + Sync + Send>> {
        let bytes = types::uuid_from_sql(raw)?;
        Ok(Uuid::from_bytes(&bytes).unwrap())
    }

    accepts!(UUID);
}

impl ToSql for Uuid {
    fn to_sql(&self, _: &Type, w: &mut Vec<u8>) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::uuid_to_sql(*self.as_bytes(), w);
        Ok(IsNull::No)
    }

    accepts!(UUID);
    to_sql_checked!();
}

extern crate uuid;

use postgres_protocol::types;
use self::uuid::Uuid;
use std::error::Error;

use types::{FromSql, ToSql, Type, IsNull, SessionInfo};

impl FromSql for Uuid {
    fn from_sql(_: &Type, raw: &[u8], _: &SessionInfo) -> Result<Uuid, Box<Error + Sync + Send>> {
        let bytes = try!(types::uuid_from_sql(raw));
        Ok(Uuid::from_bytes(&bytes).unwrap())
    }

    accepts!(Type::Uuid);
}

impl ToSql for Uuid {
    fn to_sql(&self,
              _: &Type,
              w: &mut Vec<u8>,
              _: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        types::uuid_to_sql(*self.as_bytes(), w);
        Ok(IsNull::No)
    }

    accepts!(Type::Uuid);
    to_sql_checked!();
}

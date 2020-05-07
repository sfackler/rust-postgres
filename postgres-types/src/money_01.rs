use bytes::BytesMut;
use std::error::Error;
use postgres_protocol::types;
use money_01::Money;

use crate::{FromSql, IsNull, ToSql, Type};

impl<'a> FromSql<'a> for Money {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Money, Box<dyn Error + Sync + Send>> {
        let inner = types::money_from_sql(raw)?;
        Ok(Money::from(inner))
    }

    accepts!(MONEY);
}

impl ToSql for Money {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        types::money_to_sql(self.inner(), w);
        Ok(IsNull::No)
    }

    accepts!(MONEY);
    to_sql_checked!();
}

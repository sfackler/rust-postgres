use bytes::{BytesMut, BufMut};
use std::error::Error;
use url_2::Url;

use crate::{FromSql, IsNull, ToSql, Type};

impl<'a> FromSql<'a> for Url {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Url, Box<dyn Error + Sync + Send>> {
        let url = std::str::from_utf8(raw)?;
        let url = Url::parse(url)?;
        Ok(url)
    }

    accepts!(TEXT);
}

impl ToSql for Url {
    fn to_sql(&self, _: &Type, w: &mut BytesMut) -> Result<IsNull, Box<dyn Error + Sync + Send>> {
        w.put_slice(self.to_string().as_bytes());
        Ok(IsNull::No)
    }

    accepts!(TEXT);
    to_sql_checked!();
}

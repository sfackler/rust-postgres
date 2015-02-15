use serialize::json;

use {Result, Error};
use types::{FromSql, ToSql, IsNull, Type};

impl FromSql for json::Json {
    fn from_sql<R: Reader>(ty: &Type, raw: &mut R) -> Result<json::Json> {
        if let Type::Jsonb = *ty {
            // We only support version 1 of the jsonb binary format
            if try!(raw.read_u8()) != 1 {
                return Err(Error::BadResponse);
            }
        }
        json::Json::from_reader(raw).map_err(|_| Error::BadResponse)
    }

    accepts!(Type::Json, Type::Jsonb);
}

impl ToSql for json::Json {
    fn to_sql<W: Writer+?Sized>(&self, ty: &Type, out: &mut W) -> Result<IsNull> {
        if let Type::Jsonb = *ty {
            try!(out.write_u8(1));
        }

        try!(write!(out, "{}", self));

        Ok(IsNull::No)
    }

    accepts!(Type::Json, Type::Jsonb);
    to_sql_checked!();
}

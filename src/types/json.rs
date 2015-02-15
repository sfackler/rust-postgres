use serialize::json;

use {Result, Error};
use types::{RawFromSql, RawToSql, Type};

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

impl RawToSql for json::Json {
    fn raw_to_sql<W: Writer>(&self, ty: &Type, raw: &mut W) -> Result<()> {
        if let Type::Jsonb = *ty {
            try!(raw.write_u8(1));
        }

        Ok(try!(write!(raw, "{}", self)))
    }
}

to_raw_to_impl!(Type::Json, Type::Jsonb; json::Json);

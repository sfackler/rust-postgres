use serialize::json;

use {Result, Error};
use types::{RawFromSql, RawToSql, Type};

impl RawFromSql for json::Json {
    fn raw_from_sql<R: Reader>(ty: &Type, raw: &mut R) -> Result<json::Json> {
        if let Type::Jsonb = *ty {
            // We only support version 1 of the jsonb binary format
            if try!(raw.read_u8()) != 1 {
                return Err(Error::BadData);
            }
        }
        json::Json::from_reader(raw).map_err(|_| Error::BadData)
    }
}

from_raw_from_impl!(Type::Json, Type::Jsonb; json::Json);

impl RawToSql for json::Json {
    fn raw_to_sql<W: Writer>(&self, ty: &Type, raw: &mut W) -> Result<()> {
        if let Type::Jsonb = *ty {
            try!(raw.write_u8(1));
        }

        Ok(try!(write!(raw, "{}", self)))
    }
}

to_raw_to_impl!(Type::Json, Type::Jsonb; json::Json);

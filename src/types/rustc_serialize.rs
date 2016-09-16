extern crate rustc_serialize;

use self::rustc_serialize::json;
use std::io::{Cursor, Write};
use byteorder::ReadBytesExt;
use std::error::Error;

use types::{FromSql, ToSql, IsNull, Type, SessionInfo};

impl FromSql for json::Json {
    fn from_sql(ty: &Type,
                raw: &[u8],
                _: &SessionInfo)
                -> Result<json::Json, Box<Error + Sync + Send>> {
        let mut raw = Cursor::new(raw);
        if let Type::Jsonb = *ty {
            // We only support version 1 of the jsonb binary format
            if try!(raw.read_u8()) != 1 {
                return Err("unsupported JSONB encoding version".into());
            }
        }
        json::Json::from_reader(&mut raw).map_err(Into::into)
    }

    accepts!(Type::Json, Type::Jsonb);
}

impl ToSql for json::Json {
    fn to_sql(&self,
              ty: &Type,
              mut out: &mut Vec<u8>,
              _: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        if let Type::Jsonb = *ty {
            out.push(1);
        }
        try!(write!(out, "{}", self));
        Ok(IsNull::No)
    }

    accepts!(Type::Json, Type::Jsonb);
    to_sql_checked!();
}

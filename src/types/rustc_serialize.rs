extern crate rustc_serialize;

use self::rustc_serialize::json;
use std::io::{Read, Write};
use std::error::Error;

use types::{FromSql, ToSql, IsNull, Type, SessionInfo};

impl FromSql for json::Json {
    fn from_sql(ty: &Type,
                mut raw: &[u8],
                _: &SessionInfo)
                -> Result<json::Json, Box<Error + Sync + Send>> {
        if let Type::Jsonb = *ty {
            let mut b = [0; 1];
            try!(raw.read_exact(&mut b));
            // We only support version 1 of the jsonb binary format
            if b[0] != 1 {
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

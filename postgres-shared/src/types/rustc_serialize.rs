extern crate rustc_serialize;

use self::rustc_serialize::json;
use std::io::{Read, Write};
use std::error::Error;

use types::{FromSql, IsNull, ToSql, Type, JSON, JSONB};

impl FromSql for json::Json {
    fn from_sql(ty: &Type, mut raw: &[u8]) -> Result<json::Json, Box<Error + Sync + Send>> {
        if *ty == JSONB {
            let mut b = [0; 1];
            raw.read_exact(&mut b)?;
            // We only support version 1 of the jsonb binary format
            if b[0] != 1 {
                return Err("unsupported JSONB encoding version".into());
            }
        }
        json::Json::from_reader(&mut raw).map_err(Into::into)
    }

    accepts!(JSON, JSONB);
}

impl ToSql for json::Json {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        if *ty == JSONB {
            out.push(1);
        }
        write!(out, "{}", self)?;
        Ok(IsNull::No)
    }

    accepts!(JSON, JSONB);
    to_sql_checked!();
}

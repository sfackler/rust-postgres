extern crate serde_json;

use self::serde_json::Value;
use std::error::Error;
use std::io::{Read, Write};

use types::{FromSql, ToSql, IsNull, Type, SessionInfo};

impl FromSql for Value {
    fn from_sql(ty: &Type,
                mut raw: &[u8],
                _: &SessionInfo)
                -> Result<Value, Box<Error + Sync + Send>> {
        if let Type::Jsonb = *ty {
            let mut b = [0; 1];
            try!(raw.read_exact(&mut b));
            // We only support version 1 of the jsonb binary format
            if b[0] != 1 {
                return Err("unsupported JSONB encoding version".into());
            }
        }
        serde_json::de::from_reader(raw).map_err(Into::into)
    }

    accepts!(Type::Json, Type::Jsonb);
}

impl ToSql for Value {
    fn to_sql(&self,
              ty: &Type,
              mut out: &mut Vec<u8>,
              _: &SessionInfo)
              -> Result<IsNull, Box<Error + Sync + Send>> {
        if let Type::Jsonb = *ty {
            out.push(1);
        }
        try!(write!(out, "{:?}", self));
        Ok(IsNull::No)
    }

    accepts!(Type::Json, Type::Jsonb);
    to_sql_checked!();
}

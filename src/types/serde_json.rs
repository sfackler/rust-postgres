extern crate serde_json;

use std::error;
use std::io::prelude::*;
use byteorder::{ReadBytesExt, WriteBytesExt};
use self::serde_json::Value;

use Result;
use error::Error;
use types::{FromSql, ToSql, IsNull, Type, SessionInfo};

impl FromSql for Value {
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _: &SessionInfo) -> Result<Value> {
        if let Type::Jsonb = *ty {
            // We only support version 1 of the jsonb binary format
            if try!(raw.read_u8()) != 1 {
                let err: Box<error::Error+Sync+Send> = "unsupported JSONB encoding version".into();
                return Err(Error::Conversion(err));
            }
        }
        serde_json::de::from_reader(raw).map_err(|err| Error::Conversion(Box::new(err)))
    }

    accepts!(Type::Json, Type::Jsonb);
}

impl ToSql for Value {
    fn to_sql<W: Write+?Sized>(&self, ty: &Type, mut out: &mut W, _: &SessionInfo)
                               -> Result<IsNull> {
        if let Type::Jsonb = *ty {
            try!(out.write_u8(1));
        }

        try!(write!(out, "{:?}", self));

        Ok(IsNull::No)
    }

    accepts!(Type::Json, Type::Jsonb);
    to_sql_checked!();
}

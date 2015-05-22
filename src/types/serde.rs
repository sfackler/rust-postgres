extern crate serde;

use std::io::prelude::*;
use byteorder::{ReadBytesExt, WriteBytesExt};
use self::serde::json::{self, Value};

use {Result, Error};
use types::{FromSql, ToSql, IsNull, Type, SessionInfo};

impl FromSql for Value {
    fn from_sql<R: Read>(ty: &Type, raw: &mut R, _: &SessionInfo) -> Result<Value> {
        if let Type::Jsonb = *ty {
            // We only support version 1 of the jsonb binary format
            if try!(raw.read_u8()) != 1 {
                return Err(Error::BadResponse);
            }
        }
        json::de::from_reader(raw).map_err(|_| Error::BadResponse)
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

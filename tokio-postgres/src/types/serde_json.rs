extern crate serde;
extern crate serde_json;

use self::serde::{Deserialize, Serialize};
use self::serde_json::Value;
use std::error::Error;
use std::fmt::Debug;
use std::io::Read;

use types::{FromSql, IsNull, ToSql, Type};

#[derive(Debug)]
pub struct Json<T>(pub T);

impl<'a, T> FromSql<'a> for Json<T>
where
    T: Deserialize<'a>,
{
    fn from_sql(ty: &Type, mut raw: &'a [u8]) -> Result<Json<T>, Box<Error + Sync + Send>> {
        if *ty == Type::JSONB {
            let mut b = [0; 1];
            raw.read_exact(&mut b)?;
            // We only support version 1 of the jsonb binary format
            if b[0] != 1 {
                return Err("unsupported JSONB encoding version".into());
            }
        }
        serde_json::de::from_slice(raw)
            .map(Json)
            .map_err(Into::into)
    }

    accepts!(JSON, JSONB);
}

impl<T> ToSql for Json<T>
where
    T: Serialize + Debug,
{
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        if *ty == Type::JSONB {
            out.push(1);
        }
        serde_json::ser::to_writer(out, &self.0)?;
        Ok(IsNull::No)
    }

    accepts!(JSON, JSONB);
    to_sql_checked!();
}

impl<'a> FromSql<'a> for Value {
    fn from_sql(ty: &Type, raw: &[u8]) -> Result<Value, Box<Error + Sync + Send>> {
        Json::<Value>::from_sql(ty, raw).map(|json| json.0)
    }

    accepts!(JSON, JSONB);
}

impl ToSql for Value {
    fn to_sql(&self, ty: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        Json(self).to_sql(ty, out)
    }

    accepts!(JSON, JSONB);
    to_sql_checked!();
}

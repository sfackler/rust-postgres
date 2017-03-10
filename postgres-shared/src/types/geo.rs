extern crate geo;

use postgres_protocol::types;
use self::geo::{Bbox, Point};
use std::error::Error;

use types::{FromSql, ToSql, IsNull, Type};

impl FromSql for Point<f64> {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<Error + Sync + Send>> {
        if raw.len() != 16 {
            return Err("invalid message length".into());
        }

        let x = types::float8_from_sql(&raw[0..8])?;
        let y = types::float8_from_sql(&raw[8..16])?;
        Ok(Point::new(x, y))
    }

    accepts!(Type::Point);
}

impl ToSql for Point<f64> {
    fn to_sql(&self, _: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        types::float8_to_sql(self.x(), out);
        types::float8_to_sql(self.y(), out);
        Ok(IsNull::No)
    }

    accepts!(Type::Point);
    to_sql_checked!();
}

impl FromSql for Bbox<f64> {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<Error + Sync + Send>> {
        if raw.len() != 32 {
            return Err("invalid message length".into());
        }

        let xmax = types::float8_from_sql(&raw[0..8])?;
        let ymax = types::float8_from_sql(&raw[8..16])?;
        let xmin = types::float8_from_sql(&raw[16..24])?;
        let ymin = types::float8_from_sql(&raw[24..32])?;
        Ok(Bbox{xmax: xmax, ymax: ymax, xmin: xmin, ymin: ymin})
    }

    accepts!(Type::Box);
}

impl ToSql for Bbox<f64> {
    fn to_sql(&self, _: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        types::float8_to_sql(self.xmax, out);
        types::float8_to_sql(self.ymax, out);
        types::float8_to_sql(self.xmin, out);
        types::float8_to_sql(self.ymin, out);
        Ok(IsNull::No)
    }

    accepts!(Type::Box);
    to_sql_checked!();
}

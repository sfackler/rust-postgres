extern crate geo;

use postgres_protocol::types;
use self::geo::{Bbox, LineString, Point};
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

impl FromSql for LineString<f64> {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<Error + Sync + Send>> {
        if raw.len() < 5 {
            return Err("invalid message length".into());
        }

        // let _ = types::bool_from_sql(&raw[0..1])?; // is path open or closed
        let n_points = types::int4_from_sql(&raw[1..5])? as usize;
        let raw_points = &raw[5..raw.len()];
        if raw_points.len() != 16 * n_points {
            return Err("invalid message length".into());
        }

        let mut offset = 0;
        let mut points = Vec::with_capacity(n_points);
        for _ in 0..n_points {
            let x = types::float8_from_sql(&raw_points[offset..offset+8])?;
            let y = types::float8_from_sql(&raw_points[offset+8..offset+16])?;
            points.push(Point::new(x, y));
            offset += 16;
        }
        Ok(LineString(points))
    }

    accepts!(Type::Path);
}

impl ToSql for LineString<f64> {
    fn to_sql(&self, _: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        let closed = false; // always encode an open path from LineString
        types::bool_to_sql(closed, out);
        types::int4_to_sql(self.0.len() as i32, out);
        for point in &self.0 {
            types::float8_to_sql(point.x(), out);
            types::float8_to_sql(point.y(), out);
        }
        Ok(IsNull::No)
    }

    accepts!(Type::Path);
    to_sql_checked!();
}

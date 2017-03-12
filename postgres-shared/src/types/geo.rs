extern crate geo;

use postgres_protocol::types;
use self::geo::{Bbox, LineString, Point};
use std::error::Error;
use fallible_iterator::FallibleIterator;

use types::{FromSql, ToSql, IsNull, Type};

impl FromSql for Point<f64> {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<Error + Sync + Send>> {
        let point = types::point_from_sql(raw)?;
        Ok(Point::new(point.x(), point.y()))
    }

    accepts!(Type::Point);
}

impl ToSql for Point<f64> {
    fn to_sql(&self, _: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        types::point_to_sql(self.x(), self.y(), out);
        Ok(IsNull::No)
    }

    accepts!(Type::Point);
    to_sql_checked!();
}

impl FromSql for Bbox<f64> {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<Error + Sync + Send>> {
        let bbox = types::box_from_sql(raw)?;
        Ok(Bbox {
            xmin: bbox.lower_left().x(),
            xmax: bbox.upper_right().x(),
            ymin: bbox.lower_left().y(),
            ymax: bbox.upper_right().y(),
        })
    }

    accepts!(Type::Box);
}

impl ToSql for Bbox<f64> {
    fn to_sql(&self, _: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        types::box_to_sql(self.xmin, self.ymin, self.xmax, self.ymax, out);
        Ok(IsNull::No)
    }

    accepts!(Type::Box);
    to_sql_checked!();
}

impl FromSql for LineString<f64> {
    fn from_sql(_: &Type, raw: &[u8]) -> Result<Self, Box<Error + Sync + Send>> {
        let path = types::path_from_sql(raw)?;
        let points = path.points().map(|p| Point::new(p.x(), p.y())).collect()?;
        Ok(LineString(points))
    }

    accepts!(Type::Path);
}

impl ToSql for LineString<f64> {
    fn to_sql(&self, _: &Type, out: &mut Vec<u8>) -> Result<IsNull, Box<Error + Sync + Send>> {
        let closed = false; // always encode an open path from LineString
        types::path_to_sql(closed, self.0.iter().map(|p| (p.x(), p.y())), out)?;
        Ok(IsNull::No)
    }

    accepts!(Type::Path);
    to_sql_checked!();
}

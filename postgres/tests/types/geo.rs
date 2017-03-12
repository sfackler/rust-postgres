extern crate geo;

use self::geo::{Bbox, LineString, Point};
use types::test_type;

#[test]
fn test_point_params() {
    test_type("POINT",
              &[(Some(Point::new(0.0, 0.0)), "POINT(0, 0)"),
                (Some(Point::new(-3.14, 1.618)), "POINT(-3.14, 1.618)"),
                (None, "NULL")]);
}

#[test]
fn test_box_params() {
    test_type("BOX",
              &[(Some(Bbox{xmax: 160.0, ymax: 69701.5615, xmin: -3.14, ymin: 1.618}),
                     "BOX(POINT(160.0, 69701.5615), POINT(-3.14, 1.618))"),
                (None, "NULL")]);
}

#[test]
fn test_path_params() {
    let points = vec![Point::new(0.0, 0.0), Point::new(-3.14, 1.618), Point::new(160.0, 69701.5615)];
    test_type("PATH",
              &[(Some(LineString(points)),"path '((0, 0), (-3.14, 1.618), (160.0, 69701.5615))'"),
                (None, "NULL")]);
}

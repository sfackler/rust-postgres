use std::result::Result::{Ok, Err};
use std::marker::Sized;
use std::slice::SliceExt;
use std::clone::Clone;

use std::io::prelude::*;
use byteorder::{WriteBytesExt, BigEndian};

use {Type, ToSql, Result, Error, Kind};
use types::IsNull;

/// An adapter type mapping slices to Postgres arrays.
///
/// `Slice`'s `ToSql` implementation maps the slice to a one-dimensional
/// Postgres array of the relevant type. This is particularly useful with the
/// `ANY` function to match a column against multiple values without having
/// to dynamically construct the query string.
///
/// # Examples
///
/// ```rust,no_run
/// # fn foo() -> postgres::Result<()> {
/// # use postgres::{Connection, SslMode, Slice};
/// # let conn = Connection::connect("", &SslMode::None).unwrap();
/// let values = &[1i32, 2, 3, 4, 5, 6];
/// let stmt = try!(conn.prepare("SELECT * FROM foo WHERE id = ANY($1)"));
/// for row in &try!(stmt.query(&[&Slice(values)])) {
///     // ...
/// }
/// # Ok(()) }
/// ```
pub struct Slice<'a, T: 'a + ToSql>(pub &'a [T]);

impl<'a, T: 'a + ToSql> ToSql for Slice<'a, T> {
    to_sql_checked!();

    fn to_sql<W: Write+?Sized>(&self, ty: &Type, mut w: &mut W) -> Result<IsNull> {
        let member_type = match ty.kind() {
            &Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };
        if !<T as ToSql>::accepts(&member_type) {
            return Err(Error::WrongType(ty.clone()));
        }

        try!(w.write_i32::<BigEndian>(1)); // number of dimensions
        try!(w.write_i32::<BigEndian>(1)); // has nulls
        try!(w.write_u32::<BigEndian>(member_type.to_oid()));

        try!(w.write_i32::<BigEndian>(self.0.len() as i32));
        try!(w.write_i32::<BigEndian>(0)); // index offset

        for e in self.0 {
            let mut inner_buf = vec![];
            match try!(e.to_sql(&member_type, &mut inner_buf)) {
                IsNull::No => {
                    try!(w.write_i32::<BigEndian>(inner_buf.len() as i32));
                    try!(w.write_all(&inner_buf));
                }
                IsNull::Yes => try!(w.write_i32::<BigEndian>(-1)),
            }
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            &Kind::Array(..) => true,
            _ => false,
        }
    }
}

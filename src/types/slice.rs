use std::io::prelude::*;
use byteorder::{WriteBytesExt, BigEndian};

use Result;
use error::Error;
use types::{Type, ToSql, Kind, IsNull, SessionInfo, downcast};

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
/// # use postgres::{Connection, SslMode};
/// # use postgres::types::Slice;
/// # let conn = Connection::connect("", &SslMode::None).unwrap();
/// let values = &[1i32, 2, 3, 4, 5, 6];
/// let stmt = try!(conn.prepare("SELECT * FROM foo WHERE id = ANY($1)"));
/// for row in &try!(stmt.query(&[&Slice(values)])) {
///     // ...
/// }
/// # Ok(()) }
/// ```
#[derive(Debug)]
pub struct Slice<'a, T: 'a + ToSql>(pub &'a [T]);

impl<'a, T: 'a + ToSql> ToSql for Slice<'a, T> {
    // FIXME should use to_sql_checked!() but blocked on rust-lang/rust#24308
    fn to_sql_checked(&self, ty: &Type, out: &mut Write, ctx: &SessionInfo) -> Result<IsNull> {
        if !<Slice<'a, T> as ToSql>::accepts(ty) {
            return Err(Error::WrongType(ty.clone()));
        }
        self.to_sql(ty, out, ctx)
    }

    fn to_sql<W: Write+?Sized>(&self, ty: &Type, mut w: &mut W, ctx: &SessionInfo) -> Result<IsNull> {
        let member_type = match ty.kind() {
            &Kind::Array(ref member) => member,
            _ => panic!("expected array type"),
        };

        try!(w.write_i32::<BigEndian>(1)); // number of dimensions
        try!(w.write_i32::<BigEndian>(1)); // has nulls
        try!(w.write_u32::<BigEndian>(member_type.oid()));

        try!(w.write_i32::<BigEndian>(try!(downcast(self.0.len()))));
        try!(w.write_i32::<BigEndian>(0)); // index offset

        let mut inner_buf = vec![];
        for e in self.0 {
            match try!(e.to_sql(&member_type, &mut inner_buf, ctx)) {
                IsNull::No => {
                    try!(w.write_i32::<BigEndian>(try!(downcast(inner_buf.len()))));
                    try!(w.write_all(&inner_buf));
                }
                IsNull::Yes => try!(w.write_i32::<BigEndian>(-1)),
            }
            inner_buf.clear();
        }

        Ok(IsNull::No)
    }

    fn accepts(ty: &Type) -> bool {
        match ty.kind() {
            &Kind::Array(ref member) => <T as ToSql>::accepts(member),
            _ => false,
        }
    }
}

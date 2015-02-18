use byteorder::{BigEndian, WriterBytesExt};

use {Type, ToSql, Result, Error, Kind};

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
/// for row in try!(stmt.query(&[&Slice(values)])) {
///     // ...
/// }
/// # Ok(()) }
/// ```
pub struct Slice<'a, T: 'a + ToSql>(pub &'a [T]);

impl<'a, T: 'a + ToSql> ToSql for Slice<'a, T> {
    fn to_sql(&self, ty: &Type) -> Result<Option<Vec<u8>>> {
        let member_type = match ty.kind() {
            &Kind::Array(ref member) => member,
            _ => return Err(Error::WrongType(ty.clone())),
        };

        let mut buf = vec![];
        let _ = buf.write_i32::<BigEndian>(1); // number of dimensions
        let _ = buf.write_i32::<BigEndian>(1); // has nulls
        let _ = buf.write_u32::<BigEndian>(member_type.to_oid());

        let _ = buf.write_i32::<BigEndian>(self.0.len() as i32);
        let _ = buf.write_i32::<BigEndian>(0); // index offset

        for e in self.0 {
            match try!(e.to_sql(&member_type)) {
                Some(inner_buf) => {
                    let _ = buf.write_i32::<BigEndian>(inner_buf.len() as i32);
                    let _ = buf.write_all(&inner_buf);
                }
                None => {
                    let _ = buf.write_i32::<BigEndian>(-1);
                }
            }
        }

        Ok(Some(buf))
    }
}

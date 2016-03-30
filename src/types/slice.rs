use std::io::prelude::*;

use Result;
use types::{Type, ToSql, IsNull, SessionInfo};

/// # Deprecated
///
/// `ToSql` is now implemented directly for slices.
#[derive(Debug)]
pub struct Slice<'a, T: 'a + ToSql>(pub &'a [T]);

impl<'a, T: 'a + ToSql> ToSql for Slice<'a, T> {
    fn to_sql<W: Write + ?Sized>(&self, ty: &Type, w: &mut W, ctx: &SessionInfo) -> Result<IsNull> {
        self.0.to_sql(ty, w, ctx)
    }

    fn accepts(ty: &Type) -> bool {
        <&[T] as ToSql>::accepts(ty)
    }

    to_sql_checked!();
}

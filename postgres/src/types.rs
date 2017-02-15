//! Traits dealing with Postgres data types

#[doc(inline)]
pub use postgres_shared::types::{Oid, Type, Date, Timestamp, Kind, Field, Other,
                                 WasNull, WrongType, FromSql, IsNull, ToSql};

#[doc(hidden)]
pub use postgres_shared::types::__to_sql_checked;

/// Generates a simple implementation of `ToSql::accepts` which accepts the
/// types passed to it.
#[macro_export]
macro_rules! accepts {
    ($($expected:pat),+) => (
        fn accepts(ty: &$crate::types::Type) -> bool {
            match *ty {
                $($expected)|+ => true,
                _ => false
            }
        }
    )
}

/// Generates an implementation of `ToSql::to_sql_checked`.
///
/// All `ToSql` implementations should use this macro.
#[macro_export]
macro_rules! to_sql_checked {
    () => {
        fn to_sql_checked(&self,
                          ty: &$crate::types::Type,
                          out: &mut ::std::vec::Vec<u8>)
                          -> ::std::result::Result<$crate::types::IsNull,
                                                   Box<::std::error::Error +
                                                       ::std::marker::Sync +
                                                       ::std::marker::Send>> {
            $crate::types::__to_sql_checked(self, ty, out)
        }
    }
}

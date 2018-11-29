macro_rules! try_desync {
    ($s:expr, $e:expr) => (
        match $e {
            Ok(ok) => ok,
            Err(err) => {
                $s.desynchronized = true;
                return Err(::std::convert::From::from(err));
            }
        }
    )
}

macro_rules! check_desync {
    ($e:expr) => ({
        if $e.is_desynchronized() {
            return Err(::desynchronized().into());
        }
    })
}

macro_rules! bad_response {
    ($s:expr) => {{
        debug!("Bad response at {}:{}", file!(), line!());
        $s.desynchronized = true;
        return Err(::bad_response().into());
    }};
}

#[cfg(feature = "no-logging")]
macro_rules! debug {
    ($($t:tt)*) => {};
}

#[cfg(feature = "no-logging")]
macro_rules! info {
    ($($t:tt)*) => {};
}

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

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
            return Err(::error::Error::Io(::desynchronized()));
        }
    })
}

macro_rules! bad_response {
    ($s:expr) => ({
        debug!("Bad response at {}:{}", file!(), line!());
        $s.desynchronized = true;
        return Err(::error::Error::Io(::bad_response()));
    })
}

#[cfg(feature = "no-logging")]
macro_rules! debug {
    ($($t:tt)*) => {}
}

#[cfg(feature = "no-logging")]
macro_rules! info {
    ($($t:tt)*) => {}
}

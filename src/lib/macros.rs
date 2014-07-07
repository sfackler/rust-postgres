macro_rules! try_pg_conn(
    ($e:expr) => (
        match $e {
            Ok(ok) => ok,
            Err(err) => return Err(PgConnectStreamError(err))
        }
    )
)

macro_rules! try_pg(
    ($e:expr) => (
        match $e {
            Ok(ok) => ok,
            Err(err) => return Err(PgStreamError(err))
        }
    )
)

macro_rules! try_desync(
    ($e:expr) => (
        match $e {
            Ok(ok) => ok,
            Err(err) => {
                self.desynchronized = true;
                return Err(err);
            }
        }
    )
)

macro_rules! check_desync(
    ($e:expr) => ({
        if $e.canary() != CANARY {
            fail!("PostgresConnection use after free. See mozilla/rust#13246.");
        }
        if $e.is_desynchronized() {
            return Err(PgStreamDesynchronized);
        }
    })
)

macro_rules! bad_response(
    () => ({
        debug!("Unexpected response");
        self.desynchronized = true;
        return Err(PgBadResponse);
    })
)

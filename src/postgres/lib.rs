use std::cell::Cell;
use std::str;
use std::ptr;
use std::libc::{c_void, c_char, c_int};
use std::cast;
use std::iterator::RandomAccessIterator;
use std::vec;

mod ffi {
    use std::libc::{c_char, c_int, c_uchar, c_uint, c_void};

    pub type PGconn = c_void;
    pub type PGresult = c_void;
    pub type Oid = c_uint;

    pub static CONNECTION_OK: c_int = 0;

    pub static PGRES_EMPTY_QUERY: c_int = 0;
    pub static PGRES_COMMAND_OK: c_int = 1;
    pub static PGRES_TUPLES_OK: c_int = 2;

    pub static TEXT_FORMAT: c_int = 0;

    // FIXME when FFI gets fixed
    type PQnoticeProcessor = *c_uchar /*extern "C" fn(*c_void, *c_char)*/;

    #[link_args = "-lpq"]
    extern "C" {
        fn PQconnectdb(conninfo: *c_char) -> *PGconn;
        fn PQsetNoticeProcessor(conn: *PGconn, proc: PQnoticeProcessor,
                                arg: *c_void) -> PQnoticeProcessor;
        fn PQfinish(conn: *PGconn);
        fn PQstatus(conn: *PGconn) -> c_int;
        fn PQerrorMessage(conn: *PGconn) -> *c_char;
        fn PQexec(conn: *PGconn, query: *c_char) -> *PGresult;
        fn PQresultStatus(result: *PGresult) -> c_int;
        fn PQresultErrorMessage(result: *PGresult) -> *c_char;
        fn PQclear(result: *PGresult);
        fn PQprepare(conn: *PGconn, stmtName: *c_char, query: *c_char,
                     nParams: c_int, paramTypes: *Oid) -> *PGresult;
        fn PQdescribePrepared(conn: *PGconn, stmtName: *c_char) -> *PGresult;
        fn PQexecPrepared(conn: *PGconn, stmtName: *c_char, nParams: c_int,
                          paramValues: **c_char, paramLengths: *c_int,
                          paramFormats: *c_int, resultFormat: c_int)
                          -> *PGresult;
        fn PQntuples(result: *PGresult) -> c_int;
        fn PQnfields(result: *PGresult) -> c_int;
        fn PQnparams(result: *PGresult) -> c_int;
        fn PQcmdTuples(result: *PGresult) -> *c_char;
        fn PQgetvalue(result: *PGresult, row_number: c_int, col_number: c_int)
                      -> *c_char;
    }
}

pub struct PostgresConnection<'self> {
    priv conn: *ffi::PGconn,
    priv next_stmt_id: Cell<uint>,
    priv notice_handler: &'self fn(~str)
}

pub fn log_notice_handler(notice: ~str) {
    if notice.starts_with("DEBUG") {
        debug!("%s", notice);
    } else if notice.starts_with("NOTICE") ||
            notice.starts_with("INFO") ||
            notice.starts_with("LOG") {
        info!("%s", notice);
    } else if notice.starts_with("WARNING") {
        warn!("%s", notice);
    } else {
        error!("%s", notice);
    }
}

extern "C" fn notice_handler(arg: *c_void, message: *c_char) {
    unsafe {
        let conn: *PostgresConnection = cast::transmute(arg);
        ((*conn).notice_handler)(str::raw::from_c_str(message));
    }
}

#[unsafe_destructor]
impl<'self> Drop for PostgresConnection<'self> {
    fn drop(&self) {
        unsafe { ffi::PQfinish(self.conn) }
    }
}

impl<'self> PostgresConnection<'self> {
    fn get_error(&self) -> ~str {
        unsafe { str::raw::from_c_str(ffi::PQerrorMessage(self.conn)) }
    }
}

impl<'self> PostgresConnection<'self> {
    pub fn new(uri: &str) -> Result<~PostgresConnection, ~str> {
        unsafe {
            let conn = ~PostgresConnection {conn: do uri.as_c_str |c_uri| {
                ffi::PQconnectdb(c_uri)
            }, next_stmt_id: Cell::new(0), notice_handler: log_notice_handler};
            let arg: *PostgresConnection = &*conn;
            ffi::PQsetNoticeProcessor(conn.conn, notice_handler,
                                      arg as *c_void);

            match ffi::PQstatus(conn.conn) {
                ffi::CONNECTION_OK => Ok(conn),
                _ => Err(conn.get_error())
            }
        }
    }

    pub fn set_notice_handler(&mut self, handler: &'self fn(~str)) {
        self.notice_handler = handler;
    }

    pub fn prepare<'a>(&'a self, query: &str)
                       -> Result<~PostgresStatement<'a>, ~str> {
        let id = self.next_stmt_id.take();
        let name = fmt!("__libpostgres_stmt_%u", id);
        self.next_stmt_id.put_back(id + 1);

        let mut res = unsafe {
            let raw_res = do query.as_c_str |c_query| {
                do name.as_c_str |c_name| {
                    ffi::PQprepare(self.conn, c_name, c_query,
                                   0, ptr::null())
                }
            };
            PostgresResult {result: raw_res}
        };

        if res.status() != ffi::PGRES_COMMAND_OK {
            return Err(res.error());
        }

        res = unsafe {
            let raw_res = do name.as_c_str |c_name| {
                ffi::PQdescribePrepared(self.conn, c_name)
            };
            PostgresResult {result: raw_res}
        };

        if res.status() != ffi::PGRES_COMMAND_OK {
            return Err(res.error());
        }

        Ok(~PostgresStatement {conn: self, name: name,
                               num_params: res.num_params()})
    }

    pub fn update(&self, query: &str, params: &[~str]) -> Result<uint, ~str> {
        do self.prepare(query).chain |stmt| {
            stmt.update(params)
        }
    }

    pub fn query(&self, query: &str, params: &[~str])
                 -> Result<~PostgresResult, ~str> {
        do self.prepare(query).chain |stmt| {
            stmt.query(params)
        }
    }

    pub fn in_transaction<T>(&self,
                             blk: &fn(&PostgresConnection) -> Result<T, ~str>)
                             -> Result<T, ~str> {
        match self.update("BEGIN", []) {
            Ok(_) => (),
            Err(err) => return Err(err)
        };

        // If the task fails in blk, the transaction will roll back when the
        // connection closes
        let ret = blk(self);

        // TODO What to do about errors here?
        if ret.is_ok() {
            self.update("COMMIT", []);
        } else {
            self.update("ROLLBACK", []);
        }

        ret
    }
}

pub struct PostgresStatement<'self> {
    priv conn: &'self PostgresConnection<'self>,
    priv name: ~str,
    priv num_params: uint
}

#[unsafe_destructor]
impl<'self> Drop for PostgresStatement<'self> {
    fn drop(&self) {
        // We can't do self.conn.update(...) since that will create a statement
        let query = fmt!("DEALLOCATE %s", self.name);
        unsafe {
            do query.as_c_str |c_query| {
                ffi::PQclear(ffi::PQexec(self.conn.conn, c_query));
            }
        }
    }
}

impl<'self> PostgresStatement<'self> {
    fn exec(&self, params: &[~str]) -> Result<~PostgresResult, ~str> {
        if params.len() != self.num_params {
            return Err(~"Incorrect number of parameters");
        }

        let res = unsafe {
            let raw_res = do self.name.as_c_str |c_name| {
                do as_c_str_array(params) |c_params| {
                    ffi::PQexecPrepared(self.conn.conn, c_name,
                                        self.num_params as c_int,
                                        c_params, ptr::null(), ptr::null(),
                                        ffi::TEXT_FORMAT)
                }
            };
            ~PostgresResult{result: raw_res}
        };

        match res.status() {
            ffi::PGRES_EMPTY_QUERY |
            ffi::PGRES_COMMAND_OK |
            ffi::PGRES_TUPLES_OK => Ok(res),
            _ => Err(res.error())
        }
    }

    pub fn update(&self, params: &[~str]) -> Result<uint, ~str> {
        do self.exec(params).chain |res| {
            Ok(res.affected_rows())
        }
    }

    pub fn query(&self, params: &[~str]) -> Result<~PostgresResult, ~str> {
        do self.exec(params).chain |res| {
            Ok(res)
        }
    }
}

pub struct PostgresResult {
    priv result: *ffi::PGresult
}

impl Drop for PostgresResult {
    fn drop(&self) {
        unsafe { ffi::PQclear(self.result) }
    }
}

impl PostgresResult {
    fn status(&self) -> c_int {
        unsafe { ffi::PQresultStatus(self.result) }
    }

    fn error(&self) -> ~str {
        unsafe { str::raw::from_c_str(ffi::PQresultErrorMessage(self.result)) }
    }

    fn affected_rows(&self) -> uint {
        let s = unsafe {
            str::raw::from_c_str(ffi::PQcmdTuples(self.result))
        };

        match FromStr::from_str(s) {
            Some(updates) => updates,
            None => 0
        }
    }

    fn num_params(&self) -> uint {
        unsafe { ffi::PQnparams(self.result) as uint }
    }
}

impl Container for PostgresResult {
    fn len(&self) -> uint {
        unsafe { ffi::PQntuples(self.result) as uint }
    }
}

impl PostgresResult {
    pub fn iter<'a>(&'a self) -> PostgresResultIterator<'a> {
        PostgresResultIterator {result: self, next_row: 0}
    }

    pub fn get<'a>(&'a self, idx: uint) -> PostgresRow<'a> {
        if idx >= self.len() {
            fail!("Out of bounds access");
        }

        self.iter().idx(idx).get()
    }
}

pub struct PostgresResultIterator<'self> {
    priv result: &'self PostgresResult,
    priv next_row: uint
}

impl<'self> Iterator<PostgresRow<'self>> for PostgresResultIterator<'self> {
    fn next(&mut self) -> Option<PostgresRow<'self>> {
        if self.result.len() == self.next_row {
            return None;
        }

        let row = self.next_row;
        self.next_row += 1;
        Some(PostgresRow {result: self.result, row: row})
    }

    fn size_hint(&self) -> (uint, Option<uint>) {
        let rem = self.result.len() - self.next_row;
        (rem, Some(rem))
    }
}

impl<'self> RandomAccessIterator<PostgresRow<'self>> for
        PostgresResultIterator<'self> {
    fn indexable(&self) -> uint {
        self.result.len()
    }

    fn idx(&self, idx: uint) -> Option<PostgresRow<'self>> {
        if idx < self.indexable() {
            Some(PostgresRow {result: self.result, row: idx})
        } else {
            None
        }
    }
}

pub struct PostgresRow<'self> {
    priv result: &'self PostgresResult,
    priv row: uint
}

impl<'self> Container for PostgresRow<'self> {
    fn len(&self) -> uint {
        unsafe { ffi::PQnfields(self.result.result) as uint }
    }
}

impl<'self, T: FromStr> Index<uint, Option<T>> for PostgresRow<'self> {
    fn index(&self, idx: &uint) -> Option<T> {
        self.get(*idx)
    }
}

impl<'self> PostgresRow<'self> {
    pub fn get<T: FromStr>(&self, idx: uint) -> Option<T> {
        if idx >= self.len() {
            fail!("Out of bounds access");
        }

        let s = unsafe {
            let raw_s = ffi::PQgetvalue(self.result.result,
                                        self.row as c_int,
                                        idx as c_int);
            str::raw::from_c_str(raw_s)
        };

        FromStr::from_str(s)
    }
}

fn as_c_str_array<T>(array: &[~str], blk: &fn(**c_char) -> T) -> T {
    let mut c_array: ~[*c_char] = vec::with_capacity(array.len() + 1);
    for s in array.iter() {
        // DANGER, WILL ROBINSON
        do s.as_c_str |c_s| {
            c_array.push(c_s);
        }
    }
    c_array.push(ptr::null());
    blk(vec::raw::to_ptr(c_array))
}

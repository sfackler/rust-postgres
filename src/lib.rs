use std::cell::Cell;
use std::c_str::{ToCStr, CString};
use std::str;
use std::ptr;
use std::libc::{c_void, c_char, c_int};
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
        fn PQgetisnull(result: *PGresult, row_number: c_int, col_number: c_int)
            -> c_int;
        fn PQgetvalue(result: *PGresult, row_number: c_int, col_number: c_int)
            -> *c_char;
    }
}

pub struct PostgresConnection {
    priv conn: *ffi::PGconn,
    priv next_stmt_id: Cell<uint>,
}

extern "C" fn notice_handler(_arg: *c_void, message: *c_char) {
    let message = unsafe { str::raw::from_c_str(message) };
    if message.starts_with("DEBUG") {
        debug!("%s", message);
    } else if message.starts_with("NOTICE") ||
            message.starts_with("INFO") ||
            message.starts_with("LOG") {
        info!("%s", message);
    } else if message.starts_with("WARNING") {
        warn!("%s", message);
    } else {
        error!("%s", message);
    }
}

#[unsafe_destructor]
impl Drop for PostgresConnection {
    fn drop(&self) {
        unsafe { ffi::PQfinish(self.conn) }
    }
}

impl PostgresConnection {
    fn status(&self) -> c_int {
        unsafe { ffi::PQstatus(self.conn) }
    }

    fn get_error(&self) -> ~str {
        unsafe { str::raw::from_c_str(ffi::PQerrorMessage(self.conn)) }
    }

    fn set_notice_processor(&mut self,
                            handler: *u8 /* extern "C" fn(*c_void, *c_char) */,
                            arg: *c_void) {
        unsafe { ffi::PQsetNoticeProcessor(self.conn, handler, arg); }
    }
}

impl PostgresConnection {
    pub fn new(uri: &str) -> Result<~PostgresConnection, ~str> {
        let mut conn = ~PostgresConnection {
            conn: do uri.with_c_str |c_uri| {
                unsafe { ffi::PQconnectdb(c_uri) }
            },
            next_stmt_id: Cell::new(0)
        };

        conn.set_notice_processor(notice_handler, ptr::null());

        match conn.status() {
            ffi::CONNECTION_OK => Ok(conn),
            _ => Err(conn.get_error())
        }
    }

    pub fn prepare<'a>(&'a self, query: &str)
        -> Result<~PostgresStatement<'a>, ~str> {
        let id = self.next_stmt_id.take();
        let name = fmt!("__libpostgres_stmt_%u", id);
        self.next_stmt_id.put_back(id + 1);

        let mut res = unsafe {
            let raw_res = do query.with_c_str |c_query| {
                do name.with_c_str |c_name| {
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
            let raw_res = do name.with_c_str |c_name| {
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

    pub fn update(&self, query: &str, params: &[&ToSql])
        -> Result<uint, ~str> {
        do self.prepare(query).chain |stmt| {
            stmt.update(params)
        }
    }

    pub fn query(&self, query: &str, params: &[&ToSql])
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
    priv conn: &'self PostgresConnection,
    priv name: ~str,
    priv num_params: uint
}

#[unsafe_destructor]
impl<'self> Drop for PostgresStatement<'self> {
    fn drop(&self) {
        // We can't do self.conn.update(...) since that will create a statement
        let query = fmt!("DEALLOCATE %s", self.name);
        unsafe {
            do query.with_c_str |c_query| {
                ffi::PQclear(ffi::PQexec(self.conn.conn, c_query));
            }
        }
    }
}

impl<'self> PostgresStatement<'self> {
    fn exec(&self, params: &[&ToSql]) -> Result<~PostgresResult, ~str> {
        if params.len() != self.num_params {
            return Err(~"Incorrect number of parameters");
        }

        let res = unsafe {
            let raw_res = do self.name.with_c_str |c_name| {
                let str_params: ~[Option<~str>] = do params.map |param| {
                    param.to_sql()
                };
                do with_c_str_array(str_params) |c_params| {
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

    pub fn update(&self, params: &[&ToSql]) -> Result<uint, ~str> {
        do self.exec(params).chain |res| {
            Ok(res.affected_rows())
        }
    }

    pub fn query(&self, params: &[&ToSql]) -> Result<~PostgresResult, ~str> {
        do self.exec(params).chain |res| {
            Ok(res)
        }
    }
}

fn with_c_str_array<T>(array: &[Option<~str>], blk: &fn(**c_char) -> T) -> T {
    let mut cstrs: ~[CString] = ~[];
    let mut c_array: ~[*c_char] = ~[];
    for s in array.iter() {
        match *s {
            None => {
                c_array.push(ptr::null())
            }
            Some(ref s) => {
                cstrs.push(s.to_c_str());
                do cstrs.last().with_ref |c_str| {
                    c_array.push(c_str);
                }
            }
        }
    }
    blk(vec::raw::to_ptr(c_array))
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

    fn get_value(&self, row: uint, col: uint) -> Option<~str> {
        unsafe {
            match ffi::PQgetisnull(self.result, row as c_int, col as c_int) {
                0 => {
                    let raw_s = ffi::PQgetvalue(self.result,
                                                row as c_int,
                                                col as c_int);
                    Some(str::raw::from_c_str(raw_s))
                }
                _ => None
            }
        }
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

        self.iter().idx(idx).unwrap()
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

impl<'self, T: FromSql> Index<uint, T> for PostgresRow<'self> {
    fn index(&self, idx: &uint) -> T {
        self.get(*idx)
    }
}

impl<'self> PostgresRow<'self> {
    fn get_value(&self, col: uint) -> Option<~str> {
        self.result.get_value(self.row, col)
    }
}

impl<'self> PostgresRow<'self> {
    pub fn get<T: FromSql>(&self, idx: uint) -> T {
        if idx >= self.len() {
            fail!("Out of bounds access");
        }
        FromSql::from_sql(self, idx)
    }
}

pub trait FromSql {
    fn from_sql(row: &PostgresRow, idx: uint) -> Self;
}

macro_rules! from_opt_impl(
    ($t:ty) => (
        impl FromSql for $t {
            fn from_sql(row: &PostgresRow, idx: uint) -> $t {
                FromSql::from_sql::<Option<$t>>(row, idx).unwrap()
            }
        }
    )
)

macro_rules! from_str_opt_impl(
    ($t:ty) => (
        impl FromSql for Option<$t> {
            fn from_sql(row: &PostgresRow, idx: uint) -> Option<$t> {
                do row.get_value(idx).chain |s| {
                    Some(FromStr::from_str(s).unwrap())
                }
            }
        }
    )
)

from_opt_impl!(int)
from_str_opt_impl!(int)
from_opt_impl!(i8)
from_str_opt_impl!(i8)
from_opt_impl!(i16)
from_str_opt_impl!(i16)
from_opt_impl!(i32)
from_str_opt_impl!(i32)
from_opt_impl!(i64)
from_str_opt_impl!(i64)
from_opt_impl!(uint)
from_str_opt_impl!(uint)
from_opt_impl!(u8)
from_str_opt_impl!(u8)
from_opt_impl!(u16)
from_str_opt_impl!(u16)
from_opt_impl!(u32)
from_str_opt_impl!(u32)
from_opt_impl!(u64)
from_str_opt_impl!(u64)
from_opt_impl!(float)
from_str_opt_impl!(float)
from_opt_impl!(f32)
from_str_opt_impl!(f32)
from_opt_impl!(f64)
from_str_opt_impl!(f64)

impl FromSql for Option<~str> {
    fn from_sql(row: &PostgresRow, idx: uint) -> Option<~str> {
        row.get_value(idx)
    }
}
from_opt_impl!(~str)

pub trait ToSql {
    fn to_sql(&self) -> Option<~str>;
}

macro_rules! to_str_impl(
    ($t:ty) => (
        impl ToSql for $t {
            fn to_sql(&self) -> Option<~str> {
                Some(self.to_str())
            }
        }
    )
)

macro_rules! to_str_opt_impl(
    ($t:ty) => (
        impl ToSql for Option<$t> {
            fn to_sql(&self) -> Option<~str> {
                match *self {
                    Some(ref val) => Some(val.to_sql().unwrap()),
                    None => None
                }
            }
        }
    )
)

to_str_impl!(int)
to_str_opt_impl!(int)
to_str_impl!(i8)
to_str_opt_impl!(i8)
to_str_impl!(i16)
to_str_opt_impl!(i16)
to_str_impl!(i32)
to_str_opt_impl!(i32)
to_str_impl!(i64)
to_str_opt_impl!(i64)
to_str_impl!(uint)
to_str_opt_impl!(uint)
to_str_impl!(u8)
to_str_opt_impl!(u8)
to_str_impl!(u16)
to_str_opt_impl!(u16)
to_str_impl!(u32)
to_str_opt_impl!(u32)
to_str_impl!(u64)
to_str_opt_impl!(u64)
to_str_impl!(float)
to_str_opt_impl!(float)
to_str_impl!(f32)
to_str_opt_impl!(f32)
to_str_impl!(f64)
to_str_opt_impl!(f64)

impl<'self> ToSql for &'self str {
    fn to_sql(&self) -> Option<~str> {
        Some(self.to_str())
    }
}

impl ToSql for ~str {
    fn to_sql(&self) -> Option<~str> {
        Some(self.clone())
    }
}

impl ToSql for Option<~str> {
    fn to_sql(&self) -> Option<~str> {
        self.clone()
    }
}

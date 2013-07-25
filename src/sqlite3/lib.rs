extern mod sql;

use std::libc::c_int;
use std::ptr;
use std::str;

mod ffi {
    use std::libc::{c_char, c_int, c_void};

    pub type sqlite3 = c_void;
    pub type sqlite3_stmt = c_void;

    pub static SQLITE_OK: c_int = 0;
    pub static SQLITE_ROW: c_int = 100;
    pub static SQLITE_DONE: c_int = 101;

    #[link_args = "-lsqlite3"]
    extern "C" {
        fn sqlite3_open(filename: *c_char, ppDb: *mut *sqlite3) -> c_int;
        fn sqlite3_close(db: *sqlite3) -> c_int;
        fn sqlite3_errmsg(db: *sqlite3) -> *c_char;
        fn sqlite3_changes(db: *sqlite3) -> c_int;
        fn sqlite3_prepare_v2(db: *sqlite3, zSql: *c_char, nByte: c_int,
                              ppStmt: *mut *sqlite3_stmt, pzTail: *mut *c_char)
            -> c_int;
        fn sqlite3_reset(pStmt: *sqlite3_stmt) -> c_int;
        fn sqlite3_step(pStmt: *sqlite3_stmt) -> c_int;
        fn sqlite3_column_count(pStmt: *sqlite3_stmt) -> c_int;
        fn sqlite3_column_text(pStmt: *sqlite3_stmt, iCol: c_int) -> *c_char;
        fn sqlite3_finalize(pStmt: *sqlite3_stmt) -> c_int;
    }
}

pub fn open(filename: &str) -> Result<~Connection, ~str> {
    let mut conn = ~Connection {conn: ptr::null()};
    let ret = do filename.as_c_str |c_filename| {
        unsafe { ffi::sqlite3_open(c_filename, &mut conn.conn) }
    };

    match ret {
        ffi::SQLITE_OK => Ok(conn),
        _ => Err(conn.get_error())
    }
}

pub struct Connection {
    priv conn: *ffi::sqlite3
}

impl Drop for Connection {
    fn drop(&self) {
        let ret = unsafe { ffi::sqlite3_close(self.conn) };
        assert!(ret == ffi::SQLITE_OK);
    }
}

impl Connection {
    fn get_error(&self) -> ~str {
        unsafe {
            str::raw::from_c_str(ffi::sqlite3_errmsg(self.conn))
        }
    }
}

impl Connection {
    pub fn prepare<'a>(&'a self, query: &str)
            -> Result<~PreparedStatement<'a>, ~str> {
        let mut stmt = ~PreparedStatement {conn: self, stmt: ptr::null()};
        let ret = do query.as_c_str |c_query| {
            unsafe {
                ffi::sqlite3_prepare_v2(self.conn, c_query, -1, &mut stmt.stmt,
                                        ptr::mut_null())
            }
        };

        match ret {
            ffi::SQLITE_OK => Ok(stmt),
            _ => Err(self.get_error())
        }
    }

    pub fn update(&self, query: &str) -> Result<uint, ~str> {
        self.prepare(query).chain(|stmt| stmt.update())
    }

    pub fn query<T>(&self, query: &str, blk: &fn (&mut ResultIterator) -> T)
            -> Result<T, ~str> {
        let stmt = match self.prepare(query) {
            Ok(stmt) => stmt,
            Err(err) => return Err(err)
        };

        let mut it = match stmt.query() {
            Ok(it) => it,
            Err(err) => return Err(err)
        };

        Ok(blk(&mut it))
    }
}

pub struct PreparedStatement<'self> {
    priv conn: &'self Connection,
    priv stmt: *ffi::sqlite3_stmt
}

#[unsafe_destructor]
impl<'self> Drop for PreparedStatement<'self> {
    fn drop(&self) {
        unsafe { ffi::sqlite3_finalize(self.stmt); }
    }
}

impl<'self> PreparedStatement<'self> {
    pub fn update(&self) -> Result<uint, ~str> {
        unsafe { ffi::sqlite3_reset(self.stmt); }
        let ret = unsafe { ffi::sqlite3_step(self.stmt) };

        match ret {
            // TODO: Should we consider a query that returned rows an error?
            ffi::SQLITE_DONE | ffi::SQLITE_ROW =>
                Ok(unsafe { ffi::sqlite3_changes(self.conn.conn) } as uint),
            _ => Err(self.conn.get_error())
        }
    }

    pub fn query<'a>(&'a self) -> Result<ResultIterator<'a>, ~str> {
        unsafe { ffi::sqlite3_reset(self.stmt); }
        Ok(ResultIterator {stmt: self})
    }
}

pub struct ResultIterator<'self> {
    priv stmt: &'self PreparedStatement<'self>
}

impl<'self> Iterator<Row<'self>> for ResultIterator<'self> {
    fn next(&mut self) -> Option<Row<'self>> {
        let ret = unsafe { ffi::sqlite3_step(self.stmt.stmt) };
        match ret {
            ffi::SQLITE_ROW => Some(Row {stmt: self.stmt}),
            // TODO: Ignoring errors for now
            _ => None
        }
    }
}

pub struct Row<'self> {
    priv stmt: &'self PreparedStatement<'self>
}

impl<'self> Container for Row<'self> {
    fn len(&self) -> uint {
        unsafe { ffi::sqlite3_column_count(self.stmt.stmt) as uint }
    }

    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl<'self> Row<'self> {
    pub fn get<T: FromStr>(&self, idx: uint) -> Option<T> {
        let raw = unsafe {
            ffi::sqlite3_column_text(self.stmt.stmt, idx as c_int)
        };

        if ptr::is_null(raw) {
            return None;
        }

        FromStr::from_str(unsafe { str::raw::from_c_str(raw) })
    }
}

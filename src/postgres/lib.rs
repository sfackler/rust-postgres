extern mod sql;

use sql::{ToSqlStr, FromSqlStr};

use std::str;
use std::ptr;

mod ffi {
    use std::libc::{c_char, c_int, c_uint, c_void};

    pub type PGconn = c_void;
    pub type PGresult = c_void;
    pub type OId = c_uint;

    pub enum ConnStatusType {
        CONNECTION_OK,
        CONNECTION_BAD,
        CONNECTION_STARTED,
        CONNECTION_MADE,
        CONNECTION_AWAITING_RESPONSE,
        CONNECTION_AUTH_OK,
        CONNECTION_SETENV,
        CONNECTION_SSL_STARTUP,
        CONNECTION_NEEDED
    }

    pub enum ExecStatusType {
        PGRES_EMPTY_QUERY = 0,
        PGRES_COMMAND_OK,
        PGRES_TUPLES_OK,
        PGRES_COPY_OUT,
        PGRES_COPY_IN,
        PGRES_BAD_RESPONSE,
        PGRES_NONFATAL_ERROR,
        PGRES_FATAL_ERROR,
        PGRES_COPY_BOTH,
        PGRES_SINGLE_TUPLE
    }

    #[link_args = "-lpq"]
    extern "C" {
        fn PQconnectdb(conninfo: *c_char) -> *PGconn;
        fn PQfinish(conn: *PGconn);
        fn PQstatus(conn: *PGconn) -> ConnStatusType;
        fn PQerrorMessage(conn: *PGconn) -> *c_char;
        fn PQexecParams(conn: *PGconn, command: *c_char, nParams: c_int,
                        paramTypes: *OId, paramValues: **c_char,
                        paramLengths: *c_int, paramFormats: *c_int,
                        resultFormat: c_int) -> *PGresult;
        fn PQresultStatus(res: *PGresult) -> ExecStatusType;
        fn PQresultErrorMessage(res: *PGresult) -> *c_char;
        fn PQclear(res: *PGresult);
        fn PQntuples(res: *PGresult) -> c_int;
    }
}

fn open(name: &str) -> Result<~Connection, ~str> {
    unsafe {
        let conn = ~Connection {conn: do name.as_c_str |c_name| {
            ffi::PQconnectdb(c_name)
        }};

        match ffi::PQstatus(conn.conn) {
            ffi::CONNECTION_OK => Ok(conn),
            _ => Err(str::raw::from_c_str(ffi::PQerrorMessage(conn.conn)))
        }
    }
}

pub struct Connection {
    priv conn: *ffi::PGconn
}

impl Drop for Connection {
    fn drop(&self) {
        unsafe {
            ffi::PQfinish(self.conn)
        }
    }
}

impl Connection {
    fn query(&self, query: &str, params: &[@ToSqlStr]) -> Result<~RowIterator, ~str> {
        Err(~"foo")
    }
}

pub struct RowIterator {
    priv res: *ffi::PGresult,
    priv row: Row
}

impl Drop for RowIterator {
    fn drop(&self) {
        unsafe {
            ffi::PQclear(self.res)
        }
    }
}

impl<'self> Iterator<&'self Row<'self>> for RowIterator {
    fn next(&mut self) -> Option<&'self Row> {
        unsafe {
            if ffi::PQntuples(self.res) == self. {
                return None;
            }
        }
    }
}

pub struct Row<'self> {
    priv res: *ffi::PGresult,
    priv row: uint
}

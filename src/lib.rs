extern mod extra;

use extra::container::Deque;
use extra::digest::Digest;
use extra::md5::Md5;
use extra::url::{UserInfo, Url};
use extra::ringbuf::RingBuf;
use std::cell::Cell;
use std::hashmap::HashMap;
use std::rt::io::{io_error, Decorator};
use std::rt::io::mem::MemWriter;
use std::rt::io::net::ip::SocketAddr;
use std::rt::io::net::tcp::TcpStream;

use message::*;
use types::{Oid, ToSql, FromSql};

mod message;
mod types;

macro_rules! match_read_message(
    ($conn:expr, { $($($p:pat)|+ => $e:expr),+ }) => (
        match {
            let ref conn = $conn;
            let resp;
            loop {
                match conn.read_message() {
                    NoticeResponse { fields } => handle_notice_response(fields),
                    msg => {
                        resp = msg;
                        break;
                    }
                }
            }
            resp
        } {
            $(
                $($p)|+ => $e
            ),+
        }
    )
)

fn handle_notice_response(fields: ~[(u8, ~str)]) {
    let err = PostgresDbError::new(fields);
    info!("%s: %s", err.severity, err.message);
}

#[deriving(ToStr)]
pub enum PostgresConnectError {
    InvalidUrl,
    MissingUser,
    DbError(PostgresDbError),
    MissingPassword,
    UnsupportedAuthentication
}

#[deriving(ToStr)]
pub enum PostgresErrorPosition {
    Position(uint),
    InternalPosition {
        position: uint,
        query: ~str
    }
}

#[deriving(ToStr)]
pub struct PostgresDbError {
    // This could almost be an enum, except the values can be localized :(
    severity: ~str,
    // Should probably end up as an enum
    code: ~str,
    message: ~str,
    position: Option<PostgresErrorPosition>,
    where: Option<~str>,
    file: ~str,
    line: uint,
    routine: ~str
}

impl PostgresDbError {
    fn new(fields: ~[(u8, ~str)]) -> PostgresDbError {
        // move_rev_iter is more efficient than move_iter
        let mut map: HashMap<u8, ~str> = fields.move_rev_iter().collect();
        PostgresDbError {
            severity: map.pop(&('S' as u8)).unwrap(),
            code: map.pop(&('C' as u8)).unwrap(),
            message: map.pop(&('M' as u8)).unwrap(),
            position: match map.pop(&('P' as u8)) {
                Some(pos) => Some(Position(FromStr::from_str(pos).unwrap())),
                None => match map.pop(&('p' as u8)) {
                    Some(pos) => Some(InternalPosition {
                        position: FromStr::from_str(pos).unwrap(),
                        query: map.pop(&('q' as u8)).unwrap()
                    }),
                    None => None
                }
            },
            where: map.pop(&('W' as u8)),
            file: map.pop(&('F' as u8)).unwrap(),
            line: FromStr::from_str(map.pop(&('L' as u8)).unwrap()).unwrap(),
            routine: map.pop(&('R' as u8)).unwrap()
        }
    }
}

pub struct PostgresConnection {
    priv stream: Cell<TcpStream>,
    priv next_stmt_id: Cell<int>
}

impl Drop for PostgresConnection {
    fn drop(&self) {
        do io_error::cond.trap(|_| {}).inside {
            self.write_message(&Terminate);
        }
    }
}

impl PostgresConnection {
    pub fn connect(url: &str) -> PostgresConnection {
        match PostgresConnection::try_connect(url) {
            Ok(conn) => conn,
            Err(err) => fail!("Failed to connect: %s", err.to_str())
        }
    }

    pub fn try_connect(url: &str) -> Result<PostgresConnection,
                                            PostgresConnectError> {
        let Url {
            host,
            port,
            user,
            path,
            query: args,
            _
        }: Url = match FromStr::from_str(url) {
            Some(url) => url,
            None => return Err(InvalidUrl)
        };
        let user = match user {
            Some(user) => user,
            None => return Err(MissingUser)
        };
        let mut args = args;

        // This seems silly
        let socket_url = format!("{}:{}", host,
                                 port.unwrap_or_default(~"5432"));
        let addr: SocketAddr = match FromStr::from_str(socket_url) {
            Some(addr) => addr,
            None => return Err(InvalidUrl)
        };

        let conn = PostgresConnection {
            // Need to figure out what to do about unwrap here
            stream: Cell::new(TcpStream::connect(addr).unwrap()),
            next_stmt_id: Cell::new(0)
        };

        // We have to clone here since we need the user again for auth
        args.push((~"user", user.user.clone()));
        if !path.is_empty() {
            args.push((~"database", path));
        }
        conn.write_message(&StartupMessage {
            version: PROTOCOL_VERSION,
            parameters: args.as_slice()
        });

        match conn.handle_auth(user) {
            Some(err) => return Err(err),
            None => ()
        }

        loop {
            match_read_message!(conn, {
                ParameterStatus { parameter, value } =>
                    info!("Parameter %s = %s", parameter, value),
                BackendKeyData {_} => (),
                ReadyForQuery {_} => break,
                resp => fail!("Bad response: %?", resp.to_str())
            })
        }

        Ok(conn)
    }

    fn write_messages(&self, messages: &[&FrontendMessage]) {
        let mut buf = MemWriter::new();
        for &message in messages.iter() {
            buf.write_message(message);
        }
        do self.stream.with_mut_ref |s| {
            s.write(buf.inner_ref().as_slice());
        }
    }

    fn write_message(&self, message: &FrontendMessage) {
        do self.stream.with_mut_ref |s| {
            s.write_message(message);
        }
    }

    fn read_message(&self) -> BackendMessage {
        do self.stream.with_mut_ref |s| {
            s.read_message()
        }
    }

    fn handle_auth(&self, user: UserInfo) -> Option<PostgresConnectError> {
        match_read_message!(self, {
            AuthenticationOk => return None,
            AuthenticationCleartextPassword => {
                let pass = match user.pass {
                    Some(pass) => pass,
                    None => return Some(MissingPassword)
                };
                self.write_message(&PasswordMessage { password: pass });
            },
            AuthenticationMD5Password { salt } => {
                let UserInfo { user, pass } = user;
                let pass = match pass {
                    Some(pass) => pass,
                    None => return Some(MissingPassword)
                };
                let input = pass + user;
                let mut md5 = Md5::new();
                md5.input_str(input);
                let output = md5.result_str();
                md5.reset();
                md5.input_str(output);
                md5.input(salt);
                let output = "md5" + md5.result_str();
                self.write_message(&PasswordMessage {
                    password: output.as_slice()
                });
            },
            resp => fail!("Bad response: %?", resp.to_str())
        })

        match_read_message!(self, {
            AuthenticationOk => None,
            ErrorResponse { fields } =>
                Some(DbError(PostgresDbError::new(fields))),
            resp => fail!("Bad response: %?", resp.to_str())
        })
    }

    pub fn prepare<'a>(&'a self, query: &str) -> PostgresStatement<'a> {
        match self.try_prepare(query) {
            Ok(stmt) => stmt,
            Err(err) => fail!("Error preparing \"%s\": %s", query,
                              err.to_str())
        }
    }

    pub fn try_prepare<'a>(&'a self, query: &str)
                           -> Result<PostgresStatement<'a>, PostgresDbError> {
        let id = self.next_stmt_id.take();
        let stmt_name = format!("statement_{}", id);
        self.next_stmt_id.put_back(id + 1);

        let types = [];
        self.write_messages([
                &Parse {
                    name: stmt_name,
                    query: query,
                    param_types: types
                },
                &Describe {
                    variant: 'S' as u8,
                    name: stmt_name
                },
                &Sync]);

        match_read_message!(self, {
            ParseComplete => (),
            ErrorResponse { fields } => {
                self.wait_for_ready();
                return Err(PostgresDbError::new(fields));
            },
            resp => fail!("Bad response: %?", resp.to_str())
        })

        let param_types = match_read_message!(self, {
            ParameterDescription { types } => types,
            resp => fail!("Bad response: %?", resp.to_str())
        });

        let result_desc = match_read_message!(self, {
            RowDescription { descriptions } => descriptions,
            NoData => ~[],
            resp => fail!("Bad response: %?", resp.to_str())
        });

        self.wait_for_ready();

        Ok(PostgresStatement {
            conn: self,
            name: stmt_name,
            param_types: param_types,
            result_desc: result_desc,
            next_portal_id: Cell::new(0)
        })
    }

    pub fn in_transaction<T>(&self, blk: &fn(&PostgresTransaction) -> T) -> T {
        self.quick_query("BEGIN");

        let trans = PostgresTransaction {
            conn: self,
            commit: Cell::new(true)
        };
        // If this fails, Postgres will rollback when the connection closes
        let ret = blk(&trans);

        if trans.commit.take() {
            self.quick_query("COMMIT");
        } else {
            self.quick_query("ROLLBACK");
        }

        ret
    }

    pub fn update(&self, query: &str, params: &[&ToSql]) -> uint {
        match self.try_update(query, params) {
            Ok(res) => res,
            Err(err) => fail!("Error running update: %s", err.to_str())
        }
    }

    pub fn try_update(&self, query: &str, params: &[&ToSql])
            -> Result<uint, PostgresDbError> {
        do self.try_prepare(query).chain |stmt| {
            stmt.try_update(params)
        }
    }

    fn quick_query(&self, query: &str) {
        self.write_message(&Query { query: query });

        loop {
            match_read_message!(self, {
                ReadyForQuery {_} => break,
                ErrorResponse { fields } =>
                    fail!("Error: %s", PostgresDbError::new(fields).to_str()),
                _ => ()
            })
        }
    }

    fn wait_for_ready(&self) {
        match_read_message!(self, {
            ReadyForQuery {_} => (),
            resp => fail!("Bad response: %?", resp.to_str())
        })
    }
}

pub struct PostgresTransaction<'self> {
    priv conn: &'self PostgresConnection,
    priv commit: Cell<bool>
}

impl<'self> PostgresTransaction<'self> {
    pub fn prepare<'a>(&'a self, query: &str) -> PostgresStatement<'a> {
        self.conn.prepare(query)
    }

    pub fn try_prepare<'a>(&'a self, query: &str)
                           -> Result<PostgresStatement<'a>, PostgresDbError> {
        self.conn.try_prepare(query)
    }

    pub fn update(&self, query: &str, params: &[&ToSql]) -> uint {
        self.conn.update(query, params)
    }

    pub fn try_update(&self, query: &str, params: &[&ToSql])
            -> Result<uint, PostgresDbError> {
        self.conn.try_update(query, params)
    }

    pub fn will_commit(&self) -> bool {
        let commit = self.commit.take();
        self.commit.put_back(commit);
        commit
    }

    pub fn set_commit(&self) {
        self.commit.take();
        self.commit.put_back(true);
    }

    pub fn set_rollback(&self) {
        self.commit.take();
        self.commit.put_back(false);
    }
}

pub struct PostgresStatement<'self> {
    priv conn: &'self PostgresConnection,
    priv name: ~str,
    priv param_types: ~[Oid],
    priv result_desc: ~[RowDescriptionEntry],
    priv next_portal_id: Cell<uint>
}

#[unsafe_destructor]
impl<'self> Drop for PostgresStatement<'self> {
    fn drop(&self) {
        do io_error::cond.trap(|_| {}).inside {
            self.conn.write_messages([
                &Close {
                    variant: 'S' as u8,
                    name: self.name.as_slice()
                },
                &Sync]);
            loop {
                match_read_message!(self.conn, {
                    ReadyForQuery {_} => break,
                    _ => ()
                })
            }
        }
    }
}

impl<'self> PostgresStatement<'self> {
    fn bind(&self, portal_name: &str, params: &[&ToSql])
            -> Option<PostgresDbError> {
        let mut formats = ~[];
        let mut values = ~[];
        for (&param, &ty) in params.iter().zip(self.param_types.iter()) {
            let (format, value) = param.to_sql(ty);
            formats.push(format as i16);
            values.push(value);
        };

        let result_formats = [];

        self.conn.write_messages([
            &Bind {
                portal: portal_name,
                statement: self.name.as_slice(),
                formats: formats,
                values: values,
                result_formats: result_formats
            },
            &Sync]);

        let ret = match_read_message!(self.conn, {
            BindComplete => None,
            ErrorResponse { fields } => Some(PostgresDbError::new(fields)),
            resp => fail!("Bad response: %?", resp.to_str())
        });

        self.conn.wait_for_ready();

        ret
    }

    pub fn update(&self, params: &[&ToSql]) -> uint {
        match self.try_update(params) {
            Ok(count) => count,
            Err(err) => fail!("Error running update: %s", err.to_str())
        }
    }

    pub fn try_update(&self, params: &[&ToSql])
                      -> Result<uint, PostgresDbError> {
        // The unnamed portal is automatically cleaned up at sync time
        match self.bind("", params) {
            Some(err) => {
                return Err(err);
            }
            None => ()
        }

        self.conn.write_messages([
            &Execute {
                portal: &"",
                max_rows: 0
            },
            &Sync]);

        let num;
        loop {
            match_read_message!(self.conn, {
                CommandComplete { tag } => {
                    let s = tag.split_iter(' ').last().unwrap();
                    num = match FromStr::from_str(s) {
                        None => 0,
                        Some(n) => n
                    };
                    break;
                },
                DataRow {_} => (),
                EmptyQueryResponse => {
                    num = 0;
                    break;
                },
                NoticeResponse {_} => (),
                ErrorResponse { fields } => {
                    self.conn.wait_for_ready();
                    return Err(PostgresDbError::new(fields));
                },
                resp => fail!("Bad response: %?", resp.to_str())
            })
        }
        self.conn.wait_for_ready();

        Ok(num)
    }

    pub fn query(&'self self, params: &[&ToSql]) -> PostgresResult<'self> {
        self.lazy_query(0, params)
    }

    pub fn try_query(&'self self, params: &[&ToSql])
                         -> Result<PostgresResult<'self>, PostgresDbError> {
        self.try_lazy_query(0, params)
    }

    pub fn lazy_query(&'self self, row_limit: uint, params: &[&ToSql])
            -> PostgresResult<'self> {
        match self.try_lazy_query(row_limit, params) {
            Ok(result) => result,
            Err(err) => fail!("Error running query: %s", err.to_str())
        }
    }

    pub fn try_lazy_query(&'self self, row_limit: uint, params: &[&ToSql])
            -> Result<PostgresResult<'self>, PostgresDbError> {
        let id = self.next_portal_id.take();
        let portal_name = format!("{}_portal_{}", self.name.as_slice(), id);
        self.next_portal_id.put_back(id + 1);

        match self.bind(portal_name, params) {
            Some(err) => {
                self.conn.wait_for_ready();
                return Err(err);
            }
            None => ()
        }

        let mut result = PostgresResult {
            stmt: self,
            name: portal_name,
            data: RingBuf::new(),
            more_rows: true,
            max_rows: row_limit
        };
        // We have to make sure to execute the result at least once since it
        // may have side effects (e.g. INSERT ... RETURNING ...)
        result.execute();

        Ok(result)
    }
}

pub struct PostgresResult<'self> {
    priv stmt: &'self PostgresStatement<'self>,
    priv name: ~str,
    priv data: RingBuf<~[Option<~[u8]>]>,
    priv max_rows: uint,
    priv more_rows: bool
}

#[unsafe_destructor]
impl<'self> Drop for PostgresResult<'self> {
    fn drop(&self) {
        do io_error::cond.trap(|_| {}).inside {
            self.stmt.conn.write_messages([
                &Close {
                    variant: 'P' as u8,
                    name: self.name.as_slice()
                },
                &Sync]);
            loop {
                match_read_message!(self.stmt.conn, {
                    ReadyForQuery {_} => break,
                    _ => ()
                })
            }
        }
    }
}

impl<'self> Iterator<PostgresRow> for PostgresResult<'self> {
    fn next(&mut self) -> Option<PostgresRow> {
        if self.data.is_empty() && self.more_rows {
            self.execute();
        }

        do self.data.pop_front().map_move |row| {
            PostgresRow { data: row }
        }
    }
}

impl<'self> PostgresResult<'self> {
    fn execute(&mut self) {
        self.stmt.conn.write_messages([
            &Execute {
                portal: self.name,
                max_rows: self.max_rows as i32
            },
            &Sync]);

        loop {
            match_read_message!(self.stmt.conn, {
                EmptyQueryResponse |
                CommandComplete {_} => {
                    self.more_rows = false;
                    break;
                },
                DataRow { row } => self.data.push_back(row),
                PortalSuspended => {
                    self.more_rows = true;
                    break;
                },
                resp => fail!("Bad response: %?", resp.to_str())
            })
        }
        self.stmt.conn.wait_for_ready();
    }
}

pub struct PostgresRow {
    priv data: ~[Option<~[u8]>]
}

impl<'self> Container for PostgresRow {
    fn len(&self) -> uint {
        self.data.len()
    }
}

impl<T: FromSql> Index<uint, T> for PostgresRow {
    fn index(&self, idx: &uint) -> T {
        self.get(*idx)
    }
}

impl PostgresRow {
    pub fn get<T: FromSql>(&self, idx: uint) -> T {
        FromSql::from_sql(&self.data[idx])
    }
}

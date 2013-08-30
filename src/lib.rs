extern mod extra;

use extra::digest::Digest;
use extra::md5::Md5;
use extra::url::{UserInfo, Url};
use std::cell::Cell;
use std::hashmap::HashMap;
use std::rt::io::io_error;
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
        let socket_url = format!("{:s}:{:s}", host,
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
        self.write_message(&Parse {
            name: stmt_name,
            query: query,
            param_types: types
        });
        self.write_message(&Describe { variant: 'S' as u8, name: stmt_name });
        self.write_message(&Sync);

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

        match_read_message!(self, {
            RowDescription {_} | NoData => (),
            resp => fail!("Bad response: %?", resp.to_str())
        })

        self.wait_for_ready();

        Ok(PostgresStatement {
            conn: self,
            name: stmt_name,
            param_types: param_types,
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
        loop {
            match_read_message!(self, {
                ReadyForQuery {_} => break,
                resp => fail!("Bad response: %?", resp.to_str())
            })
        }
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
    priv next_portal_id: Cell<uint>
}

#[unsafe_destructor]
impl<'self> Drop for PostgresStatement<'self> {
    fn drop(&self) {
        do io_error::cond.trap(|_| {}).inside {
            self.conn.write_message(&Close {
                variant: 'S' as u8,
                name: self.name.as_slice()
            });
            self.conn.write_message(&Sync);
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
    fn execute(&self, portal_name: &str, params: &[&ToSql])
               -> Option<PostgresDbError> {
        let mut formats = ~[];
        let mut values = ~[];
        for (&param, &ty) in params.iter().zip(self.param_types.iter()) {
            let (format, value) = param.to_sql(ty);
            formats.push(format as i16);
            values.push(value);
        };

        let result_formats = [];

        self.conn.write_message(&Bind {
            portal: portal_name,
            statement: self.name.as_slice(),
            formats: formats,
            values: values,
            result_formats: result_formats
        });
        self.conn.write_message(&Execute {
            portal: portal_name.as_slice(),
            max_rows: 0
        });
        self.conn.write_message(&Sync);

        match_read_message!(self.conn, {
            BindComplete => None,
            ErrorResponse { fields } => Some(PostgresDbError::new(fields)),
            resp => fail!("Bad response: %?", resp.to_str())
        })
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
        match self.execute("", params) {
            Some(err) => {
                self.conn.wait_for_ready();
                return Err(err);
            }
            None => ()
        }

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

    pub fn query<'a>(&'a self, params: &[&ToSql]) -> PostgresResult<'a> {
        match self.try_query(params) {
            Ok(result) => result,
            Err(err) => fail!("Error running query: %s", err.to_str())
        }
    }

    pub fn try_query<'a>(&'a self, params: &[&ToSql])
                         -> Result<PostgresResult<'a>, PostgresDbError> {
        let id = self.next_portal_id.take();
        let portal_name = format!("{:s}_portal_{}", self.name.as_slice(), id);
        self.next_portal_id.put_back(id + 1);

        match self.execute(portal_name, params) {
            Some(err) => {
                self.conn.wait_for_ready();
                return Err(err);
            }
            None => ()
        }

        let mut data = ~[];
        loop {
            match_read_message!(self.conn, {
                EmptyQueryResponse => break,
                DataRow { row } => data.push(row),
                CommandComplete {_} => break,
                NoticeResponse {_} => (),
                ErrorResponse { fields } => {
                    self.conn.wait_for_ready();
                    return Err(PostgresDbError::new(fields));
                },
                resp => fail!("Bad response: %?", resp.to_str())
            })
        }
        self.conn.wait_for_ready();

        Ok(PostgresResult {
            stmt: self,
            name: portal_name,
            data: data
        })
    }
}

pub struct PostgresResult<'self> {
    priv stmt: &'self PostgresStatement<'self>,
    priv name: ~str,
    priv data: ~[~[Option<~[u8]>]]
}

#[unsafe_destructor]
impl<'self> Drop for PostgresResult<'self> {
    fn drop(&self) {
        do io_error::cond.trap(|_| {}).inside {
            self.stmt.conn.write_message(&Close {
                variant: 'P' as u8,
                name: self.name.as_slice()
            });
            self.stmt.conn.write_message(&Sync);
            loop {
                match_read_message!(self.stmt.conn, {
                    ReadyForQuery {_} => break,
                    _ => ()
                })
            }
        }
    }
}

impl<'self> PostgresResult<'self> {
    pub fn iter<'a>(&'a self) -> PostgresResultIterator<'a> {
        PostgresResultIterator { result: self, next_row: 0 }
    }
}

pub struct PostgresResultIterator<'self> {
    priv result: &'self PostgresResult<'self>,
    priv next_row: uint
}

impl<'self> Iterator<PostgresRow<'self>> for PostgresResultIterator<'self> {
    fn next(&mut self) -> Option<PostgresRow<'self>> {
        if self.next_row == self.result.data.len() {
            return None;
        }

        let row = self.next_row;
        self.next_row += 1;
        Some(PostgresRow { result: self.result, row: row })
    }
}

pub struct PostgresRow<'self> {
    priv result: &'self PostgresResult<'self>,
    priv row: uint
}

impl<'self> Container for PostgresRow<'self> {
    fn len(&self) -> uint {
        self.result.data[self.row].len()
    }
}

impl<'self, T: FromSql> Index<uint, T> for PostgresRow<'self> {
    fn index(&self, idx: &uint) -> T {
        self.get(*idx)
    }
}

impl<'self> PostgresRow<'self> {
    pub fn get<T: FromSql>(&self, idx: uint) -> T {
        FromSql::from_sql(&self.result.data[self.row][idx])
    }
}

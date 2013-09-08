#[link(name = "postgres",
       vers = "0.1",
       url = "https://github.com/sfackler/rust-postgres")];

extern mod extra;

use extra::container::Deque;
use extra::digest::Digest;
use extra::ringbuf::RingBuf;
use extra::md5::Md5;
use extra::url::{UserInfo, Url};
use std::cell::Cell;
use std::hashmap::HashMap;
use std::rt::io::{io_error, Decorator};
use std::rt::io::mem::MemWriter;
use std::rt::io::net;
use std::rt::io::net::ip;
use std::rt::io::net::ip::SocketAddr;
use std::rt::io::net::tcp::TcpStream;

use message::*;
use types::{PostgresType, ToSql, FromSql};

mod message;
mod types;

macro_rules! match_read_message(
    ($conn:expr, { $($($p:pat)|+ => $e:expr),+ }) => (
        match {
            let ref conn = $conn;
            let resp;
            loop {
                let msg = conn.read_message();
                info2!("{}", msg.to_str());
                match msg {
                    NoticeResponse { fields } =>
                        handle_notice_response(fields),
                    ParameterStatus { parameter, value } =>
                        info!("Parameter %s = %s", parameter, value),
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

macro_rules! match_read_message_or_fail(
    ($conn:expr, { $($($p:pat)|+ => $e:expr),+ }) => (
        match_read_message!($conn, {
            $(
              $($p)|+ => $e
            ),+ ,
            resp => fail2!("Bad response: {}", resp.to_str())
        })
    )
)

fn handle_notice_response(fields: ~[(u8, ~str)]) {
    let err = PostgresDbError::new(fields);
    info2!("{}: {}", err.severity, err.message);
}

#[deriving(ToStr)]
pub enum PostgresConnectError {
    InvalidUrl,
    MissingUser,
    DnsError,
    SocketError,
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
            Err(err) => fail2!("Failed to connect: {}", err.to_str())
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

        let port = match port {
            Some(port) => FromStr::from_str(port).unwrap(),
            None => 5432
        };

        let stream = match PostgresConnection::open_socket(host, port) {
            Ok(stream) => stream,
            Err(err) => return Err(err)
        };

        let conn = PostgresConnection {
            // Need to figure out what to do about unwrap here
            stream: Cell::new(stream),
            next_stmt_id: Cell::new(0)
        };

        args.push((~"client_encoding", ~"UTF8"));
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
            match_read_message_or_fail!(conn, {
                BackendKeyData {_} => (),
                ReadyForQuery {_} => break
            })
        }

        Ok(conn)
    }

    fn open_socket(host: &str, port: ip::Port)
            -> Result<TcpStream, PostgresConnectError> {
        let addrs = do io_error::cond.trap(|_| {}).inside {
            net::get_host_addresses(host)
        };
        let addrs = match addrs {
            Some(addrs) => addrs,
            None => return Err(DnsError)
        };

        for addr in addrs.iter() {
            let socket = do io_error::cond.trap(|_| {}).inside {
                TcpStream::connect(SocketAddr { ip: *addr, port: port })
            };
            match socket {
                Some(socket) => return Ok(socket),
                None => ()
            }
        }

        Err(SocketError)
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
        match_read_message_or_fail!(self, {
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
            }
        })

        match_read_message_or_fail!(self, {
            AuthenticationOk => None,
            ErrorResponse { fields } =>
                Some(DbError(PostgresDbError::new(fields)))
        })
    }

    pub fn prepare<'a>(&'a self, query: &str) -> NormalPostgresStatement<'a> {
        match self.try_prepare(query) {
            Ok(stmt) => stmt,
            Err(err) => fail2!("Error preparing \"{}\": {}", query,
                               err.to_str())
        }
    }

    pub fn try_prepare<'a>(&'a self, query: &str)
                -> Result<NormalPostgresStatement<'a>, PostgresDbError> {
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

        match_read_message_or_fail!(self, {
            ParseComplete => (),
            ErrorResponse { fields } => {
                self.wait_for_ready();
                return Err(PostgresDbError::new(fields));
            }
        })

        let param_types = match_read_message_or_fail!(self, {
            ParameterDescription { types } =>
                types.iter().map(|ty| { PostgresType::from_oid(*ty) })
                    .collect()
        });

        let result_desc = match_read_message_or_fail!(self, {
            RowDescription { descriptions } => {
                let mut res: ~[ResultDescription] = descriptions
                    .move_rev_iter().map(|desc| {
                        ResultDescription::from_row_description_entry(desc)
                    }).collect();
                res.reverse();
                res
            },
            NoData => ~[]
        });

        self.wait_for_ready();

        Ok(NormalPostgresStatement {
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
            Err(err) => fail2!("Error running update: {}", err.to_str())
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
                    fail2!("Error: {}", PostgresDbError::new(fields).to_str()),
                _ => ()
            })
        }
    }

    fn wait_for_ready(&self) {
        match_read_message_or_fail!(self, {
            ReadyForQuery {_} => ()
        })
    }
}

pub struct PostgresTransaction<'self> {
    priv conn: &'self PostgresConnection,
    priv commit: Cell<bool>
}

impl<'self> PostgresTransaction<'self> {
    pub fn prepare<'a>(&'a self, query: &str)
            -> TransactionalPostgresStatement<'a> {
        TransactionalPostgresStatement { stmt: self.conn.prepare(query) }
    }

    pub fn try_prepare<'a>(&'a self, query: &str)
            -> Result<TransactionalPostgresStatement<'a>, PostgresDbError> {
        do self.conn.try_prepare(query).map_move |stmt| {
            TransactionalPostgresStatement { stmt: stmt }
        }
    }

    pub fn update(&self, query: &str, params: &[&ToSql]) -> uint {
        self.conn.update(query, params)
    }

    pub fn try_update(&self, query: &str, params: &[&ToSql])
            -> Result<uint, PostgresDbError> {
        self.conn.try_update(query, params)
    }

    pub fn in_transaction<T>(&self, blk: &fn(&PostgresTransaction) -> T) -> T {
        self.conn.quick_query("SAVEPOINT sp");

        let nested_trans = PostgresTransaction {
            conn: self.conn,
            commit: Cell::new(true)
        };

        let ret = blk(&nested_trans);

        if nested_trans.commit.take() {
            self.conn.quick_query("RELEASE sp");
        } else {
            self.conn.quick_query("ROLLBACK TO sp");
        }

        ret
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

pub trait PostgresStatement {
    fn param_types<'a>(&'a self) -> &'a [PostgresType];
    fn result_descriptions<'a>(&'a self) -> &'a [ResultDescription];
    fn update(&self, params: &[&ToSql]) -> uint;
    fn try_update(&self, params: &[&ToSql]) -> Result<uint, PostgresDbError>;
    fn query<'a>(&'a self, params: &[&ToSql]) -> PostgresResult<'a>;
    fn try_query<'a>(&'a self, params: &[&ToSql])
            -> Result<PostgresResult<'a>, PostgresDbError>;
    fn find_col_named(&self, col: &str) -> Option<uint>;
}

pub struct NormalPostgresStatement<'self> {
    priv conn: &'self PostgresConnection,
    priv name: ~str,
    priv param_types: ~[PostgresType],
    priv result_desc: ~[ResultDescription],
    priv next_portal_id: Cell<uint>
}

#[deriving(Eq)]
pub struct ResultDescription {
    name: ~str,
    ty: PostgresType
}

impl ResultDescription {
    fn from_row_description_entry(row: RowDescriptionEntry)
            -> ResultDescription {
        let RowDescriptionEntry { name, type_oid, _ } = row;

        ResultDescription {
            name: name,
            ty: PostgresType::from_oid(type_oid)
        }
    }
}

#[unsafe_destructor]
impl<'self> Drop for NormalPostgresStatement<'self> {
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

impl<'self> NormalPostgresStatement<'self> {
    fn execute(&self, portal_name: &str, row_limit: uint, params: &[&ToSql])
            -> Option<PostgresDbError> {
        let mut formats = ~[];
        let mut values = ~[];
        for (&param, &ty) in params.iter().zip(self.param_types.iter()) {
            let (format, value) = param.to_sql(ty);
            formats.push(format as i16);
            values.push(value);
        };

        let result_formats: ~[i16] = self.result_desc.iter().map(|desc| {
            desc.ty.result_format() as i16
        }).collect();

        self.conn.write_messages([
            &Bind {
                portal: portal_name,
                statement: self.name.as_slice(),
                formats: formats,
                values: values,
                result_formats: result_formats
            },
            &Execute {
                portal: portal_name,
                max_rows: row_limit as i32
            },
            &Sync]);

        match_read_message_or_fail!(self.conn, {
            BindComplete => None,
            ErrorResponse { fields } => {
                self.conn.wait_for_ready();
                Some(PostgresDbError::new(fields))
            }
        })
    }

    fn lazy_query<'a>(&'a self, row_limit: uint, params: &[&ToSql])
            -> PostgresResult<'a> {
        match self.try_lazy_query(row_limit, params) {
            Ok(result) => result,
            Err(err) => fail2!("Error executing query: {}", err.to_str())
        }
    }

    fn try_lazy_query<'a>(&'a self, row_limit: uint, params: &[&ToSql])
            -> Result<PostgresResult<'a>, PostgresDbError> {
        let id = self.next_portal_id.take();
        let portal_name = format!("{}_portal_{}", self.name, id);
        self.next_portal_id.put_back(id + 1);

        match self.execute(portal_name, row_limit, params) {
            Some(err) => {
                return Err(err);
            }
            None => ()
        }

        let mut result = PostgresResult {
            stmt: self,
            name: portal_name,
            data: RingBuf::new(),
            row_limit: row_limit,
            more_rows: true
        };
        result.read_rows();

        Ok(result)
    }
}

impl<'self> PostgresStatement for NormalPostgresStatement<'self> {
    fn param_types<'a>(&'a self) -> &'a [PostgresType] {
        self.param_types.as_slice()
    }

    fn result_descriptions<'a>(&'a self) -> &'a [ResultDescription] {
        self.result_desc.as_slice()
    }

    fn update(&self, params: &[&ToSql]) -> uint {
        match self.try_update(params) {
            Ok(count) => count,
            Err(err) => fail2!("Error running update: {}", err.to_str())
        }
    }

    fn try_update(&self, params: &[&ToSql])
                      -> Result<uint, PostgresDbError> {
        match self.execute("", 0, params) {
            Some(err) => {
                return Err(err);
            }
            None => ()
        }

        let num;
        loop {
            match_read_message_or_fail!(self.conn, {
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
                }
            })
        }
        self.conn.wait_for_ready();

        Ok(num)
    }

    fn query<'a>(&'a self, params: &[&ToSql])
            -> PostgresResult<'a> {
        self.lazy_query(0, params)
    }

    fn try_query<'a>(&'a self, params: &[&ToSql])
            -> Result<PostgresResult<'a>, PostgresDbError> {
        self.try_lazy_query(0, params)
    }

    fn find_col_named(&self, col: &str) -> Option<uint> {
        do self.result_desc.iter().position |desc| {
            desc.name.as_slice() == col
        }
    }
}

pub struct TransactionalPostgresStatement<'self> {
    priv stmt: NormalPostgresStatement<'self>
}

impl<'self> PostgresStatement for TransactionalPostgresStatement<'self> {
    fn param_types<'a>(&'a self) -> &'a [PostgresType] {
        self.stmt.param_types()
    }

    fn result_descriptions<'a>(&'a self) -> &'a [ResultDescription] {
        self.stmt.result_descriptions()
    }

    fn update(&self, params: &[&ToSql]) -> uint {
        self.stmt.update(params)
    }

    fn try_update(&self, params: &[&ToSql]) -> Result<uint, PostgresDbError> {
        self.stmt.try_update(params)
    }

    fn query<'a>(&'a self, params: &[&ToSql]) -> PostgresResult<'a> {
        self.stmt.query(params)
    }

    fn try_query<'a>(&'a self, params: &[&ToSql])
            -> Result<PostgresResult<'a>, PostgresDbError> {
        self.stmt.try_query(params)
    }

    fn find_col_named(&self, col: &str) -> Option<uint> {
        self.stmt.find_col_named(col)
    }
}

impl<'self> TransactionalPostgresStatement<'self> {
    pub fn lazy_query<'a>(&'a self, row_limit: uint, params: &[&ToSql])
            -> PostgresResult<'a> {
        self.stmt.lazy_query(row_limit, params)
    }

    pub fn try_lazy_query<'a>(&'a self, row_limit: uint, params: &[&ToSql])
            -> Result<PostgresResult<'a>, PostgresDbError> {
        self.stmt.try_lazy_query(row_limit, params)
    }
}

pub struct PostgresResult<'self> {
    priv stmt: &'self NormalPostgresStatement<'self>,
    priv name: ~str,
    priv data: RingBuf<~[Option<~[u8]>]>,
    priv row_limit: uint,
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

impl<'self> PostgresResult<'self> {
    fn read_rows(&mut self) {
        loop {
            match_read_message_or_fail!(self.stmt.conn, {
                EmptyQueryResponse |
                CommandComplete {_} => {
                    self.more_rows = false;
                    break;
                },
                PortalSuspended => {
                    self.more_rows = true;
                    break;
                },
                DataRow { row } => self.data.push_back(row)
            })
        }
        self.stmt.conn.wait_for_ready();
    }

    fn execute(&mut self) {
        self.stmt.conn.write_messages([
            &Execute {
                portal: self.name,
                max_rows: self.row_limit as i32
            },
            &Sync]);
        self.read_rows();
    }
}

impl<'self> Iterator<PostgresRow<'self>> for PostgresResult<'self> {
    fn next(&mut self) -> Option<PostgresRow<'self>> {
        if self.data.is_empty() && self.more_rows {
            self.execute();
        }

        do self.data.pop_front().map_move |row| {
            PostgresRow {
                stmt: self.stmt,
                data: row
            }
        }
    }
}

pub struct PostgresRow<'self> {
    priv stmt: &'self NormalPostgresStatement<'self>,
    priv data: ~[Option<~[u8]>]
}

impl<'self> Container for PostgresRow<'self> {
    fn len(&self) -> uint {
        self.data.len()
    }
}

impl<'self, I: RowIndex, T: FromSql> Index<I, T> for PostgresRow<'self> {
    fn index(&self, idx: &I) -> T {
        let idx = idx.idx(self.stmt);
        FromSql::from_sql(self.stmt.result_desc[idx].ty,
                          &self.data[idx])
    }
}

pub trait RowIndex {
    fn idx(&self, stmt: &NormalPostgresStatement) -> uint;
}

impl RowIndex for uint {
    fn idx(&self, _stmt: &NormalPostgresStatement) -> uint {
        *self
    }
}

// This is a convenience as the 0 in get[0] resolves to int :(
impl RowIndex for int {
    fn idx(&self, _stmt: &NormalPostgresStatement) -> uint {
        assert!(*self >= 0);
        *self as uint
    }
}

impl<'self> RowIndex for &'self str {
    fn idx(&self, stmt: &NormalPostgresStatement) -> uint {
        match stmt.find_col_named(*self) {
            Some(idx) => idx,
            None => fail2!("No column with name {}", *self)
        }
    }
}

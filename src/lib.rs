extern mod extra;

use extra::digest::Digest;
use extra::md5::Md5;
use extra::url::Url;
use std::cell::Cell;
use std::rt::io::net::ip::SocketAddr;
use std::rt::io::net::tcp::TcpStream;
use std::str;

use message::*;

mod message;

pub struct PostgresConnection {
    priv stream: Cell<TcpStream>,
    priv next_stmt_id: Cell<int>
}

impl Drop for PostgresConnection {
    fn drop(&self) {
        self.write_message(&Terminate);
    }
}

impl PostgresConnection {
    pub fn connect(url: &str) -> PostgresConnection {
        let url: Url = FromStr::from_str(url).unwrap();

        let socket_url = fmt!("%s:%s", url.host,
                              url.port.get_ref().as_slice());
        let addr: SocketAddr = FromStr::from_str(socket_url).unwrap();
        let conn = PostgresConnection {
            stream: Cell::new(TcpStream::connect(addr).unwrap()),
            next_stmt_id: Cell::new(0)
        };

        let mut args = url.query.clone();
        args.push((~"user", url.user.get_ref().user.clone()));
        if !url.path.is_empty() {
            args.push((~"database", url.path.clone()));
        }
        conn.write_message(&StartupMessage(args.as_slice()));

        conn.handle_auth(&url);

        loop {
            match conn.read_message() {
                ParameterStatus(param, value) =>
                    info!("Parameter %s = %s", param, value),
                BackendKeyData(*) => (),
                ReadyForQuery(*) => break,
                resp => fail!("Bad response: %?", resp.to_str())
            }
        }

        conn
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

    fn handle_auth(&self, url: &Url) {
        loop {
            match self.read_message() {
                AuthenticationOk => break,
                AuthenticationCleartextPassword => {
                    let pass = url.user.get_ref().pass.get_ref().as_slice();
                    self.write_message(&PasswordMessage(pass));
                }
                AuthenticationMD5Password(nonce) => {
                    let input = url.user.get_ref().pass.get_ref().as_slice() +
                            url.user.get_ref().user.as_slice();
                    let mut md5 = Md5::new();
                    md5.input_str(input);
                    let output = md5.result_str();
                    md5.reset();
                    md5.input_str(output);
                    md5.input(nonce);
                    let output = "md5" + md5.result_str();
                    self.write_message(&PasswordMessage(output.as_slice()));
                }
                resp => fail!("Bad response: %?", resp.to_str())
            }
        }
    }

    pub fn prepare<'a>(&'a self, query: &str) -> PostgresStatement<'a> {
        let id = self.next_stmt_id.take();
        let stmt_name = ifmt!("statement_{}", id);
        self.next_stmt_id.put_back(id + 1);

        let types = [];
        self.write_message(&Parse(stmt_name, query, types));
        self.write_message(&Sync);

        match self.read_message() {
            ParseComplete => (),
            resp @ ErrorResponse(*) => fail!("Error: %?", resp.to_str()),
            resp => fail!("Bad response: %?", resp.to_str())
        }

        self.wait_for_ready();

        self.write_message(&Describe('S' as u8, stmt_name));
        self.write_message(&Sync);

        let num_params = match self.read_message() {
            ParameterDescription(ref types) => types.len(),
            resp => fail!("Bad response: %?", resp.to_str())
        };

        match self.read_message() {
            RowDescription(*) | NoData => (),
            resp => fail!("Bad response: %?", resp.to_str())
        }

        self.wait_for_ready();

        PostgresStatement {
            conn: self,
            name: stmt_name,
            num_params: num_params,
            next_portal_id: Cell::new(0)
        }
    }

    pub fn in_transaction<T, E: ToStr>(&self, blk: &fn(&PostgresConnection)
                                                       -> Result<T, E>)
                                       -> Result<T, E> {
        self.quick_query("BEGIN");

        // If this fails, Postgres will rollback when the connection closes
        let ret = blk(self);

        if ret.is_ok() {
            self.quick_query("COMMIT");
        } else {
            self.quick_query("ROLLBACK");
        }

        ret
    }

    fn quick_query(&self, query: &str) {
        self.write_message(&Query(query));

        loop {
            match self.read_message() {
                ReadyForQuery(*) => break,
                resp @ ErrorResponse(*) => fail!("Error: %?", resp.to_str()),
                _ => ()
            }
        }
    }

    fn wait_for_ready(&self) {
        loop {
            match self.read_message() {
                ReadyForQuery(*) => break,
                resp => fail!("Bad response: %?", resp.to_str())
            }
        }
    }
}

pub struct PostgresStatement<'self> {
    priv conn: &'self PostgresConnection,
    priv name: ~str,
    priv num_params: uint,
    priv next_portal_id: Cell<uint>
}

#[unsafe_destructor]
impl<'self> Drop for PostgresStatement<'self> {
    fn drop(&self) {
        self.conn.write_message(&Close('S' as u8, self.name.as_slice()));
        self.conn.write_message(&Sync);
        loop {
            match self.conn.read_message() {
                ReadyForQuery(*) => break,
                _ => ()
            }
        }
    }
}

impl<'self> PostgresStatement<'self> {
    pub fn num_params(&self) -> uint {
        self.num_params
    }

    fn execute(&self, portal_name: &str, params: &[&ToSql]) {
        if self.num_params != params.len() {
            fail!("Expected %u params but got %u", self.num_params,
                  params.len());
        }

        let formats = [];
        let values: ~[Option<~[u8]>] = params.iter().map(|val| val.to_sql())
                .collect();
        let result_formats = [];

        self.conn.write_message(&Bind(portal_name, self.name.as_slice(),
                                      formats, values, result_formats));
        self.conn.write_message(&Execute(portal_name.as_slice(), 0));
        self.conn.write_message(&Sync);

        match self.conn.read_message() {
            BindComplete => (),
            resp @ ErrorResponse(*) => fail!("Error: %?", resp.to_str()),
            resp => fail!("Bad response: %?", resp.to_str())
        }
    }

    pub fn update(&self, params: &[&ToSql]) -> uint {
        // The unnamed portal is automatically cleaned up at sync time
        self.execute("", params);

        let mut num = 0;
        loop {
            match self.conn.read_message() {
                CommandComplete(ret) => {
                    let s = ret.split_iter(' ').last().unwrap();
                    match FromStr::from_str(s) {
                        None => (),
                        Some(n) => num = n
                    }
                    break;
                }
                DataRow(*) => (),
                EmptyQueryResponse => break,
                NoticeResponse(*) => (),
                resp @ ErrorResponse(*) => fail!("Error: %?", resp.to_str()),
                resp => fail!("Bad response: %?", resp.to_str())
            }
        }
        self.conn.wait_for_ready();

        num
    }

    pub fn query<'a>(&'a self, params: &[&ToSql]) -> PostgresResult<'a> {
        let id = self.next_portal_id.take();
        let portal_name = ifmt!("{:s}_portal_{}", self.name.as_slice(), id);
        self.next_portal_id.put_back(id + 1);

        self.execute(portal_name, params);

        let mut data = ~[];
        loop {
            match self.conn.read_message() {
                EmptyQueryResponse => break,
                DataRow(row) => data.push(row),
                CommandComplete(*) => break,
                NoticeResponse(*) => (),
                resp @ ErrorResponse(*) => fail!("Error: %?", resp.to_str()),
                resp => fail!("Bad response: %?", resp.to_str())
            }
        }

        PostgresResult {
            stmt: self,
            name: portal_name,
            data: data
        }
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
        self.stmt.conn.write_message(&Close('P' as u8, self.name.as_slice()));
        self.stmt.conn.write_message(&Sync);
        loop {
            match self.stmt.conn.read_message() {
                ReadyForQuery(*) => break,
                _ => ()
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

pub trait FromSql {
    fn from_sql(raw: &Option<~[u8]>) -> Self;
}

macro_rules! from_str_impl(
    ($t:ty) => (
        impl FromSql for Option<$t> {
            fn from_sql(raw: &Option<~[u8]>) -> Option<$t> {
                match *raw {
                    None => None,
                    Some(ref buf) => {
                        let s = str::from_bytes_slice(buf.as_slice());
                        Some(FromStr::from_str(s).unwrap())
                    }
                }
            }
        }
    )
)

macro_rules! from_option_impl(
    ($t:ty) => (
        impl FromSql for $t {
            fn from_sql(raw: &Option<~[u8]>) -> $t {
                FromSql::from_sql::<Option<$t>>(raw).unwrap()
            }
        }
    )
)

from_str_impl!(int)
from_option_impl!(int)
from_str_impl!(i8)
from_option_impl!(i8)
from_str_impl!(i16)
from_option_impl!(i16)
from_str_impl!(i32)
from_option_impl!(i32)
from_str_impl!(i64)
from_option_impl!(i64)
from_str_impl!(uint)
from_option_impl!(uint)
from_str_impl!(u8)
from_option_impl!(u8)
from_str_impl!(u16)
from_option_impl!(u16)
from_str_impl!(u32)
from_option_impl!(u32)
from_str_impl!(u64)
from_option_impl!(u64)
from_str_impl!(float)
from_option_impl!(float)
from_str_impl!(f32)
from_option_impl!(f32)
from_str_impl!(f64)
from_option_impl!(f64)

impl FromSql for Option<~str> {
    fn from_sql(raw: &Option<~[u8]>) -> Option<~str> {
        do raw.chain_ref |buf| {
            Some(str::from_bytes(buf.as_slice()))
        }
    }
}
from_option_impl!(~str)

pub trait ToSql {
    fn to_sql(&self) -> Option<~[u8]>;
}

macro_rules! to_str_impl(
    ($t:ty) => (
        impl ToSql for $t {
            fn to_sql(&self) -> Option<~[u8]> {
                Some(self.to_str().into_bytes())
            }
        }
    )
)

macro_rules! to_option_impl(
    ($t:ty) => (
        impl ToSql for Option<$t> {
            fn to_sql(&self) -> Option<~[u8]> {
                do self.chain |val| {
                    val.to_sql()
                }
            }
        }
    )
)

to_str_impl!(int)
to_option_impl!(int)
to_str_impl!(i8)
to_option_impl!(i8)
to_str_impl!(i16)
to_option_impl!(i16)
to_str_impl!(i32)
to_option_impl!(i32)
to_str_impl!(i64)
to_option_impl!(i64)
to_str_impl!(uint)
to_option_impl!(uint)
to_str_impl!(u8)
to_option_impl!(u8)
to_str_impl!(u16)
to_option_impl!(u16)
to_str_impl!(u32)
to_option_impl!(u32)
to_str_impl!(u64)
to_option_impl!(u64)
to_str_impl!(float)
to_option_impl!(float)
to_str_impl!(f32)
to_option_impl!(f32)
to_str_impl!(f64)
to_option_impl!(f64)

impl<'self> ToSql for &'self str {
    fn to_sql(&self) -> Option<~[u8]> {
        Some(self.as_bytes().to_owned())
    }
}

impl ToSql for Option<~str> {
    fn to_sql(&self) -> Option<~[u8]> {
        do self.chain_ref |val| {
            val.to_sql()
        }
    }
}

impl<'self> ToSql for Option<&'self str> {
    fn to_sql(&self) -> Option<~[u8]> {
        do self.chain |val| {
            val.to_sql()
        }
    }
}

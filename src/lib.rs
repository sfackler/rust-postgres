extern mod extra;

use std::cell::Cell;
use std::hashmap::HashMap;
use std::rt::io::net::ip::SocketAddr;
use std::rt::io::net::tcp::TcpStream;
use extra::url::Url;
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
        let parsed_url: Url = FromStr::from_str(url).unwrap();

        let socket_url = fmt!("%s:%s", parsed_url.host,
                              parsed_url.port.get_ref().as_slice());
        let addr: SocketAddr = FromStr::from_str(socket_url).unwrap();
        let conn = PostgresConnection {
            stream: Cell::new(TcpStream::connect(addr).unwrap()),
            next_stmt_id: Cell::new(0)
        };

        let mut args = HashMap::new();
        args.insert(&"user", parsed_url.user.get_ref().user.as_slice());
        conn.write_message(&StartupMessage(args));

        match conn.read_message() {
            AuthenticationOk => (),
            resp => fail!("Bad response: %?", resp.to_str())
        }

        loop {
            match conn.read_message() {
                ParameterStatus(param, value) =>
                    printfln!("Param %s = %s", param, value),
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

    fn execute(&self, portal_name: &str) {
        let formats = [];
        let values = [];
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

    pub fn update(&self) -> uint {
        self.execute("");

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

    pub fn query<'a>(&'a self) -> PostgresResult<'a> {
        let id = self.next_portal_id.take();
        let portal_name = ifmt!("{:s}_portal_{}", self.name.as_slice(), id);
        self.next_portal_id.put_back(id + 1);

        self.execute(portal_name);

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

impl FromSql for int {
    fn from_sql(raw: &Option<~[u8]>) -> int {
        FromStr::from_str(str::from_bytes_slice(raw.get_ref().as_slice()))
            .unwrap()
    }
}

extern mod extra;

use std::cell::Cell;
use std::hashmap::HashMap;
use std::rt::io::io_error;
use std::rt::io::net::ip::SocketAddr;
use std::rt::io::net::tcp::TcpStream;
use extra::url::Url;

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
            resp => fail!("Bad response: %?", resp)
        }

        conn.finish_connect();

        conn
    }

    fn finish_connect(&self) {
        loop {
            match self.read_message() {
                ParameterStatus(param, value) =>
                    printfln!("Param %s = %s", param, value),
                BackendKeyData(*) => (),
                ReadyForQuery(*) => break,
                resp => fail!("Bad response: %?", resp)
            }
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

    pub fn prepare<'a>(&'a self, query: &str) -> PostgresStatement<'a> {
        let id = self.next_stmt_id.take();
        let stmt_name = ifmt!("statement_{}", id);
        self.next_stmt_id.put_back(id + 1);

        let types = [];
        self.write_message(&Parse(stmt_name, query, types));
        self.write_message(&Sync);

        match self.read_message() {
            ParseComplete => (),
            ErrorResponse(ref data) => fail!("Error: %?", data),
            resp => fail!("Bad response: %?", resp)
        }

        self.wait_for_ready();

        self.write_message(&Describe('S' as u8, stmt_name));
        self.write_message(&Sync);

        let num_params = match self.read_message() {
            ParameterDescription(ref types) => types.len(),
            resp => fail!("Bad response: %?", resp)
        };

        match self.read_message() {
            RowDescription(*) | NoData => (),
            resp => fail!("Bad response: %?", resp)
        }

        self.wait_for_ready();

        PostgresStatement {
            conn: self,
            name: stmt_name,
            num_params: num_params,
            next_portal_id: Cell::new(0)
        }
    }

    fn wait_for_ready(&self) {
        match self.read_message() {
            ReadyForQuery(*) => (),
            resp => fail!("Bad response: %?", resp)
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
        self.conn.read_message(); // CloseComplete or ErrorResponse
        self.conn.wait_for_ready();
    }
}

impl<'self> PostgresStatement<'self> {
    pub fn num_params(&self) -> uint {
        self.num_params
    }

    pub fn query(&self) {
        let id = self.next_portal_id.take();
        let portal_name = ifmt!("{:s}_portal_{}", self.name.as_slice(), id);
        self.next_portal_id.put_back(id + 1);

        let formats = [];
        let values = [];
        let result_formats = [];

        self.conn.write_message(&Bind(portal_name, self.name.as_slice(),
                                      formats, values, result_formats));
        self.conn.write_message(&Sync);

        match self.conn.read_message() {
            BindComplete => (),
            ErrorResponse(ref data) => fail!("Error: %?", data),
            resp => fail!("Bad response: %?", resp)
        }

        self.conn.wait_for_ready();
    }
}

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
        do io_error::cond.trap(|_| { }).inside {
            do self.stream.with_mut_ref |s| {
                s.write_message(&Terminate);
            }
        }
    }
}

impl PostgresConnection {
    pub fn connect(url: &str, username: &str) -> PostgresConnection {
        let parsed_url: Url = FromStr::from_str(url).unwrap();

        let socket_url = fmt!("%s:%s", parsed_url.host,
                              parsed_url.port.unwrap_or_default(~"5432"));
        let addr: SocketAddr = FromStr::from_str(socket_url).unwrap();
        let conn = PostgresConnection {
            stream: Cell::new(TcpStream::connect(addr).unwrap()),
            next_stmt_id: Cell::new(0)
        };

        let mut args = HashMap::new();
        args.insert(&"user", username);
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
            num_params: num_params
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
    priv num_params: uint
}

impl<'self> PostgresStatement<'self> {

}

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

        do conn.stream.with_mut_ref |s| {
            let mut args = HashMap::new();
            args.insert(~"user", username.to_owned());
            s.write_message(&StartupMessage(args));
        }

        let resp = do conn.stream.with_mut_ref |s| {
            s.read_message()
        };
        match resp {
            AuthenticationOk => (),
            _ => fail!("Bad response: %?", resp)
        }

        conn.finish_connect();

        conn
    }

    fn finish_connect(&self) {
        loop {
            match self.stream.with_mut_ref(|s| { s.read_message() }) {
                ParameterStatus(param, value) =>
                    printfln!("Param %s = %s", param, value),
                BackendKeyData(*) => loop,
                ReadyForQuery(_) => break,
                resp => fail!("Bad response: %?", resp)
            }
        }
    }

    pub fn prepare<'a>(&'a self, query: &str) -> PostgresStatement<'a> {
        let id = self.next_stmt_id.take();
        let query_name = ifmt!("statement_{}", id);
        self.next_stmt_id.put_back(id + 1);

        do self.stream.with_mut_ref |s| {
            s.write_message(&Parse(query_name.clone(), query.to_owned(), ~[]));
        }

        do self.stream.with_mut_ref |s| {
            s.write_message(&Sync);
        }

        match self.stream.with_mut_ref(|s| { s.read_message() }) {
            ParseComplete => (),
            ErrorResponse(ref data) => fail!("Error: %?", data),
            resp => fail!("Bad response: %?", resp)
        }

        match self.stream.with_mut_ref(|s| { s.read_message() }) {
            ReadyForQuery(*) => (),
            resp => fail!("Bad response: %?", resp)
        }

        PostgresStatement { conn: self, name: query_name }
    }
}

pub struct PostgresStatement<'self> {
    priv conn: &'self PostgresConnection,
    priv name: ~str
}

impl<'self> PostgresStatement<'self> {

}

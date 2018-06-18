use futures::sync::mpsc;
use postgres_protocol::message::frontend;
use postgres_shared::stmt::Column;

use proto::connection::Request;
use types::Type;

pub struct Statement {
    sender: mpsc::UnboundedSender<Request>,
    name: String,
    params: Vec<Type>,
    columns: Vec<Column>,
}

impl Drop for Statement {
    fn drop(&mut self) {
        let mut buf = vec![];
        frontend::close(b'S', &self.name, &mut buf).expect("statement name not valid");
        let (sender, _) = mpsc::channel(0);
        self.sender.unbounded_send(Request {
            messages: buf,
            sender,
        });
    }
}

impl Statement {
    pub fn new(
        sender: mpsc::UnboundedReceiver<Request>,
        name: String,
        params: Vec<Type>,
        columns: Vec<Column>,
    ) -> Statement {
        Statement {
            sender,
            name,
            params,
            columns,
        }
    }

    pub fn params(&self) -> &[Type] {
        &self.params
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

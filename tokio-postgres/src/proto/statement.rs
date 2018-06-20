use futures::sync::mpsc;
use postgres_protocol::message::frontend;
use postgres_shared::stmt::Column;
use std::sync::Arc;

use proto::connection::Request;
use types::Type;

pub struct StatementInner {
    sender: mpsc::UnboundedSender<Request>,
    name: String,
    params: Vec<Type>,
    columns: Vec<Column>,
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        let mut buf = vec![];
        frontend::close(b'S', &self.name, &mut buf).expect("statement name not valid");
        frontend::sync(&mut buf);
        let (sender, _) = mpsc::channel(0);
        let _ = self.sender.unbounded_send(Request {
            messages: buf,
            sender,
        });
    }
}

#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Statement {
    pub fn new(
        sender: mpsc::UnboundedSender<Request>,
        name: String,
        params: Vec<Type>,
        columns: Vec<Column>,
    ) -> Statement {
        Statement(Arc::new(StatementInner {
            sender,
            name,
            params,
            columns,
        }))
    }

    pub fn name(&self) -> &str {
        &self.0.name
    }

    pub fn params(&self) -> &[Type] {
        &self.0.params
    }

    pub fn columns(&self) -> &[Column] {
        &self.0.columns
    }
}

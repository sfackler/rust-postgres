use std::sync::Arc;

use proto::client::WeakClient;
use types::Type;
use Column;

pub struct StatementInner {
    client: WeakClient,
    name: String,
    params: Vec<Type>,
    columns: Vec<Column>,
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            client.close_statement(&self.name);
        }
    }
}

#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Statement {
    pub fn new(
        client: WeakClient,
        name: String,
        params: Vec<Type>,
        columns: Vec<Column>,
    ) -> Statement {
        Statement(Arc::new(StatementInner {
            client,
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

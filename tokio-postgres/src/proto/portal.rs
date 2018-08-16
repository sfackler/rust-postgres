use std::sync::Arc;

use proto::client::WeakClient;
use proto::statement::Statement;

struct Inner {
    client: WeakClient,
    name: String,
    statement: Statement,
}

impl Drop for Inner {
    fn drop(&mut self) {
        if let Some(client) = self.client.upgrade() {
            client.close_portal(&self.name);
        }
    }
}

#[derive(Clone)]
pub struct Portal(Arc<Inner>);

impl Portal {
    pub fn new(client: WeakClient, name: String, statement: Statement) -> Portal {
        Portal(Arc::new(Inner {
            client,
            name,
            statement,
        }))
    }

    pub fn name(&self) -> &str {
        &self.0.name
    }

    pub fn statement(&self) -> &Statement {
        &self.0.statement
    }
}

use std::sync::Arc;

use proto::client::WeakClient;
use proto::statement::Statement;

struct Inner {
    client: WeakClient,
    name: String,
    statement: Statement,
}

pub struct Portal(Arc<Inner>);

impl Drop for Portal {
    fn drop(&mut self) {
        if let Some(client) = self.0.client.upgrade() {
            client.close_portal(&self.0.name);
        }
    }
}

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

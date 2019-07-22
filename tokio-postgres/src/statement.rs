use std::sync::Arc;

//use crate::proto::client::WeakClient;
use crate::column::Column;
use crate::types::Type;

pub struct StatementInner {
    //client: WeakClient,
    name: String,
    params: Vec<Type>,
    columns: Vec<Column>,
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        /*if let Some(client) = self.client.upgrade() {
            client.close_statement(&self.name);
        }*/
    }
}

/// A prepared statement.
///
/// Prepared statements can only be used with the connection that created them.
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Statement {
    pub(crate) fn new(
        //client: WeakClient,
        name: String,
        params: Vec<Type>,
        columns: Vec<Column>,
    ) -> Statement {
        Statement(Arc::new(StatementInner {
            //client,
            name,
            params,
            columns,
        }))
    }

    pub(crate) fn name(&self) -> &str {
        &self.0.name
    }

    pub fn params(&self) -> &[Type] {
        &self.0.params
    }

    pub fn columns(&self) -> &[Column] {
        &self.0.columns
    }
}

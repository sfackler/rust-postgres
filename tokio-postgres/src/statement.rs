use crate::client::InnerClient;
use crate::codec::FrontendMessage;
use crate::connection::RequestMessages;
use crate::types::Type;
use postgres_protocol::message::frontend;
use std::{
    fmt,
    sync::{Arc, Weak},
};

enum StatementInner {
    Unnamed {
        query: String,
        params: Vec<Type>,
        columns: Vec<Column>,
    },
    Named {
        client: Weak<InnerClient>,
        name: String,
        params: Vec<Type>,
        columns: Vec<Column>,
    },
}

impl Drop for StatementInner {
    fn drop(&mut self) {
        if let StatementInner::Named { client, name, .. } = self {
            if let Some(client) = client.upgrade() {
                let buf = client.with_buf(|buf| {
                    frontend::close(b'S', name, buf).unwrap();
                    frontend::sync(buf);
                    buf.split().freeze()
                });
                let _ = client.send(RequestMessages::Single(FrontendMessage::Raw(buf)));
            }
        }
    }
}

/// A prepared statement.
///
/// Prepared statements can only be used with the connection that created them.
#[derive(Clone)]
pub struct Statement(Arc<StatementInner>);

impl Statement {
    pub(crate) fn named(
        inner: &Arc<InnerClient>,
        name: String,
        params: Vec<Type>,
        columns: Vec<Column>,
    ) -> Statement {
        Statement(Arc::new(StatementInner::Named {
            client: Arc::downgrade(inner),
            name,
            params,
            columns,
        }))
    }

    pub(crate) fn unnamed(query: String, params: Vec<Type>, columns: Vec<Column>) -> Self {
        Statement(Arc::new(StatementInner::Unnamed {
            query,
            params,
            columns,
        }))
    }

    pub(crate) fn name(&self) -> &str {
        match &*self.0 {
            StatementInner::Unnamed { .. } => "",
            StatementInner::Named { name, .. } => name,
        }
    }

    pub(crate) fn query(&self) -> Option<&str> {
        match &*self.0 {
            StatementInner::Unnamed { query, .. } => Some(query),
            StatementInner::Named { .. } => None,
        }
    }

    /// Returns the expected types of the statement's parameters.
    pub fn params(&self) -> &[Type] {
        match &*self.0 {
            StatementInner::Unnamed { params, .. } => params,
            StatementInner::Named { params, .. } => params,
        }
    }

    /// Returns information about the columns returned when the statement is queried.
    pub fn columns(&self) -> &[Column] {
        match &*self.0 {
            StatementInner::Unnamed { columns, .. } => columns,
            StatementInner::Named { columns, .. } => columns,
        }
    }
}

/// Information about a column of a query.
pub struct Column {
    name: String,
    type_: Type,
}

impl Column {
    pub(crate) fn new(name: String, type_: Type) -> Column {
        Column { name, type_ }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the type of the column.
    pub fn type_(&self) -> &Type {
        &self.type_
    }
}

impl fmt::Debug for Column {
    fn fmt(&self, fmt: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt.debug_struct("Column")
            .field("name", &self.name)
            .field("type", &self.type_)
            .finish()
    }
}

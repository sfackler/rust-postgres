use crate::client::InnerClient;
use crate::connection::Request;
use crate::types::Type;
use std::sync::{Arc, Weak};

pub struct Statement {
    client: Weak<InnerClient>,
    name: String,
    params: Vec<Type>,
    columns: Vec<Column>,
}

impl Statement {
    pub(crate) fn new(
        inner: &Arc<InnerClient>,
        name: String,
        params: Vec<Type>,
        columns: Vec<Column>,
    ) -> Statement {
        Statement {
            client: Arc::downgrade(inner),
            name,
            params,
            columns,
        }
    }

    /// Returns the expected types of the statement's parameters.
    pub fn params(&self) -> &[Type] {
        &self.params
    }

    /// Returns information about the columns returned when the statement is queried.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

#[derive(Debug)]
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

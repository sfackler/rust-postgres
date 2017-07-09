//! Prepared statements.

use std::mem;
use std::sync::Arc;
use std::sync::mpsc::Sender;

#[doc(inline)]
pub use postgres_shared::stmt::Column;

use types::Type;

/// A prepared statement.
pub struct Statement {
    close_sender: Sender<(u8, String)>,
    name: String,
    params: Vec<Type>,
    columns: Arc<Vec<Column>>,
}

impl Drop for Statement {
    fn drop(&mut self) {
        let name = mem::replace(&mut self.name, String::new());
        let _ = self.close_sender.send((b'S', name));
    }
}

impl Statement {
    pub(crate) fn new(
        close_sender: Sender<(u8, String)>,
        name: String,
        params: Vec<Type>,
        columns: Arc<Vec<Column>>,
    ) -> Statement {
        Statement {
            close_sender: close_sender,
            name: name,
            params: params,
            columns: columns,
        }
    }

    pub(crate) fn columns_arc(&self) -> &Arc<Vec<Column>> {
        &self.columns
    }

    pub(crate) fn name(&self) -> &str {
        &self.name
    }

    /// Returns the types of query parameters for this statement.
    pub fn parameters(&self) -> &[Type] {
        &self.params
    }

    /// Returns information about the resulting columns for this statement.
    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

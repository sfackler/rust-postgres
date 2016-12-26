use std::mem;
use std::sync::Arc;
use std::sync::mpsc::Sender;

#[doc(inline)]
pub use postgres_shared::Column;

use StatementNew;
use types::Type;

pub struct Statement {
    close_sender: Sender<(u8, String)>,
    name: String,
    params: Vec<Type>,
    columns: Arc<Vec<Column>>,
}

impl StatementNew for Statement {
    fn new(close_sender: Sender<(u8, String)>,
           name: String,
           params: Vec<Type>,
           columns: Arc<Vec<Column>>)
           -> Statement {
        Statement {
            close_sender: close_sender,
            name: name,
            params: params,
            columns: columns,
        }
    }

    fn columns_arc(&self) -> &Arc<Vec<Column>> {
        &self.columns
    }

    fn name(&self) -> &str {
        &self.name
    }
}

impl Drop for Statement {
    fn drop(&mut self) {
        let name = mem::replace(&mut self.name, String::new());
        let _ = self.close_sender.send((b'S', name));
    }
}

impl Statement {
    pub fn parameters(&self) -> &[Type] {
        &self.params
    }

    pub fn columns(&self) -> &[Column] {
        &self.columns
    }
}

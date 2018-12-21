use tokio_postgres::types::Type;
use tokio_postgres::Column;

pub struct Statement(pub(crate) tokio_postgres::Statement);

impl Statement {
    pub fn params(&self) -> &[Type] {
        self.0.params()
    }

    pub fn columns(&self) -> &[Column] {
        self.0.columns()
    }
}

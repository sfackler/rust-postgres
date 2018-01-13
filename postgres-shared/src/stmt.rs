use postgres_protocol::Oid;

use types::Type;

/// Information about a column of a Postgres query.
#[derive(Debug)]
pub struct Column {
    table: Oid,
    name: String,
    type_: Type,
}

impl Column {
    #[doc(hidden)]
    pub fn new(name: String, table: Oid, type_: Type) -> Column {
        Column {
            name: name,
            table,
            type_: type_,
        }
    }

    /// Returns the name of the column.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the table of the column.
    pub fn table(&self) -> Oid {
        self.table
    }

    /// Returns the type of the column.
    pub fn type_(&self) -> &Type {
        &self.type_
    }
}

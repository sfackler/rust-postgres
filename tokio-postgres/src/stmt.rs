use types::Type;

/// Information about a column of a Postgres query.
#[derive(Debug)]
pub struct Column {
    name: String,
    type_: Type,
}

impl Column {
    #[doc(hidden)]
    pub fn new(name: String, type_: Type) -> Column {
        Column {
            name: name,
            type_: type_,
        }
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

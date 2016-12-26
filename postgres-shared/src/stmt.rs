use types::Type;

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

    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn type_(&self) -> &Type {
        &self.type_
    }
}

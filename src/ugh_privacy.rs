use types::{Oid, Type};

/// Information about an unknown type.
#[derive(PartialEq, Eq, Clone, Debug)]
pub struct Unknown {
    name: String,
    oid: Oid,
    element_type: Option<Box<Type>>,
}

pub fn new_unknown(name: String, oid: Oid, element_type: Option<Box<Type>>) -> Unknown {
    Unknown {
        name: name,
        oid: oid,
        element_type: element_type,
    }
}

impl Unknown {
    /// The name of the type.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The OID of this type.
    pub fn oid(&self) -> Oid {
        self.oid
    }

    /// If this type is an array or range, the type of its members.
    pub fn element_type(&self) -> Option<&Type> {
        self.element_type.as_ref().map(|e| &**e)
    }
}


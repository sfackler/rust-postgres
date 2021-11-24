use syn::{Error, Ident, Type};

use crate::overrides::Overrides;

pub struct Field {
    pub name: String,
    pub ident: Ident,
    pub type_: Type,
}

impl Field {
    pub fn parse(raw: &syn::Field) -> Result<Field, Error> {
        let overrides = Overrides::extract(&raw.attrs)?;

        let ident = raw.ident.as_ref().unwrap().clone();
        Ok(Field {
            name: overrides.name.unwrap_or_else(|| {
                let name = ident.to_string();
                match name.strip_prefix("r#") {
                    Some(name) => name.to_string(),
                    None => name,
                }
            }),
            ident,
            type_: raw.ty.clone(),
        })
    }
}

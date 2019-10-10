use syn::{Error, Fields, Ident};

use crate::overrides::Overrides;

pub struct Variant {
    pub ident: Ident,
    pub name: String,
}

impl Variant {
    pub fn parse(raw: &syn::Variant) -> Result<Variant, Error> {
        match raw.fields {
            Fields::Unit => {}
            _ => {
                return Err(Error::new_spanned(
                    raw,
                    "non-C-like enums are not supported",
                ))
            }
        }

        let overrides = Overrides::extract(&raw.attrs)?;
        Ok(Variant {
            ident: raw.ident.clone(),
            name: overrides.name.unwrap_or_else(|| raw.ident.to_string()),
        })
    }
}

use syn::{Error, Fields, Ident};

use crate::{field_variant_overrides::FieldVariantOverrides, struct_overrides::StructOverrides};

pub struct Variant {
    pub ident: Ident,
    pub name: String,
}

impl Variant {
    pub fn parse(struct_overrides: &StructOverrides, raw: &syn::Variant) -> Result<Variant, Error> {
        use convert_case::Casing;

        match raw.fields {
            Fields::Unit => {}
            _ => {
                return Err(Error::new_spanned(
                    raw,
                    "non-C-like enums are not supported",
                ))
            }
        }

        let mut overrides = FieldVariantOverrides::extract(&raw.attrs)?;
        Ok(Variant {
            ident: raw.ident.clone(),
            name: overrides.name.take().unwrap_or_else(|| {
                let name = raw.ident.to_string();
                let name = name.strip_prefix("r#").map(String::from).unwrap_or(name);
                struct_overrides
                    .rename_all
                    .map(|case| name.to_case(case))
                    .unwrap_or(name)
            }),
        })
    }
}

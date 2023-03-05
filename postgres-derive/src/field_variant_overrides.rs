use syn::{Attribute, Error, Lit, Meta, NestedMeta};

#[derive(Default)]
pub struct FieldVariantOverrides {
    pub name: Option<String>,
}

impl FieldVariantOverrides {
    pub fn extract(attrs: &[Attribute]) -> Result<FieldVariantOverrides, Error> {
        let mut overrides: FieldVariantOverrides = Default::default();

        for attr in attrs {
            let attr = attr.parse_meta()?;

            if !attr.path().is_ident("postgres") {
                continue;
            }

            let list = match attr {
                Meta::List(ref list) => list,
                bad => return Err(Error::new_spanned(bad, "expected a #[postgres(...)]")),
            };

            for item in &list.nested {
                match item {
                    NestedMeta::Meta(Meta::NameValue(meta)) => {
                        if meta.path.is_ident("name") {
                            let value = match &meta.lit {
                                Lit::Str(s) => s.value(),
                                bad => {
                                    return Err(Error::new_spanned(
                                        bad,
                                        "expected a string literal",
                                    ))
                                }
                            };
                            overrides.name = Some(value);
                        } else {
                            return Err(Error::new_spanned(&meta.path, "unknown override"));
                        }
                    }
                    bad => return Err(Error::new_spanned(bad, "unknown attribute")),
                }
            }
        }

        Ok(overrides)
    }
}

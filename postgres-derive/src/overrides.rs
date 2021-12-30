use syn::{Attribute, Error, Lit, Meta, NestedMeta};

pub struct Overrides {
    pub name: Option<String>,
    pub transparent: bool,
}

impl Overrides {
    pub fn extract(attrs: &[Attribute]) -> Result<Overrides, Error> {
        let mut overrides = Overrides {
            name: None,
            transparent: false,
        };

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
                        if !meta.path.is_ident("name") {
                            return Err(Error::new_spanned(&meta.path, "unknown override"));
                        }

                        let value = match &meta.lit {
                            Lit::Str(s) => s.value(),
                            bad => {
                                return Err(Error::new_spanned(bad, "expected a string literal"))
                            }
                        };

                        overrides.name = Some(value);
                    }
                    NestedMeta::Meta(Meta::Path(ref path)) => {
                        if !path.is_ident("transparent") {
                            return Err(Error::new_spanned(path, "unknown override"));
                        }

                        overrides.transparent = true;
                    }
                    bad => return Err(Error::new_spanned(bad, "unknown attribute")),
                }
            }
        }

        Ok(overrides)
    }
}

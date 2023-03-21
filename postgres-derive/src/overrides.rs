use syn::punctuated::Punctuated;
use syn::{Attribute, Error, Expr, ExprLit, Lit, Meta, Token};

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
            if !attr.path().is_ident("postgres") {
                continue;
            }

            let list = match &attr.meta {
                Meta::List(ref list) => list,
                bad => return Err(Error::new_spanned(bad, "expected a #[postgres(...)]")),
            };

            let nested = list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;

            for item in nested {
                match item {
                    Meta::NameValue(meta) => {
                        if !meta.path.is_ident("name") {
                            return Err(Error::new_spanned(&meta.path, "unknown override"));
                        }

                        let value = match &meta.value {
                            Expr::Lit(ExprLit {
                                lit: Lit::Str(lit), ..
                            }) => lit.value(),
                            bad => {
                                return Err(Error::new_spanned(bad, "expected a string literal"))
                            }
                        };

                        overrides.name = Some(value);
                    }
                    Meta::Path(path) => {
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

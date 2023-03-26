use syn::punctuated::Punctuated;
use syn::{Attribute, Error, Expr, ExprLit, Lit, Meta, Token};

use crate::case::{RenameRule, RENAME_RULES};

pub struct Overrides {
    pub name: Option<String>,
    pub rename_all: Option<RenameRule>,
    pub transparent: bool,
}

impl Overrides {
    pub fn extract(attrs: &[Attribute]) -> Result<Overrides, Error> {
        let mut overrides = Overrides {
            name: None,
            rename_all: None,
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
                        let name_override = meta.path.is_ident("name");
                        let rename_all_override = meta.path.is_ident("rename_all");
                        if !name_override && !rename_all_override {
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

                        if name_override {
                            overrides.name = Some(value);
                        } else if rename_all_override {
                            let rename_rule = RENAME_RULES
                                .iter()
                                .find(|rule| rule.0 == value)
                                .map(|val| val.1)
                                .ok_or_else(|| {
                                    Error::new_spanned(
                                        &meta.value,
                                        format!(
                                            "invalid rename_all rule, expected one of: {}",
                                            RENAME_RULES
                                                .iter()
                                                .map(|rule| format!("\"{}\"", rule.0))
                                                .collect::<Vec<_>>()
                                                .join(", ")
                                        ),
                                    )
                                })?;

                            overrides.rename_all = Some(rename_rule);
                        }
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

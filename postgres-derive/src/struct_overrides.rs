use syn::{Attribute, Error, Lit, Meta, NestedMeta};

use crate::rename_rule::RenameRule;

#[derive(Default)]
pub struct StructOverrides {
    pub name: Option<String>,
    pub transparent: bool,
    pub rename_all: Option<convert_case::Case>,
}

impl StructOverrides {
    pub fn extract(attrs: &[Attribute]) -> Result<StructOverrides, Error> {
        use itertools::Itertools;
        use strum::IntoEnumIterator;

        let mut overrides: StructOverrides = Default::default();

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
                        } else if meta.path.is_ident("rename_all") {
                            let rename_rule: RenameRule = match &meta.lit {
                                Lit::Str(s) => s.value().parse().ok(),
                                _other => None,
                            }
                            .ok_or_else(|| {
                                let all_variants = RenameRule::iter()
                                    .map(|variant| format!("\"{variant}\""))
                                    .join(", ");
                                Error::new_spanned(
                                    &meta.lit,
                                    format!("expected one of: {all_variants}"),
                                )
                            })?;
                            overrides.rename_all = Some(rename_rule.into());
                        } else {
                            return Err(Error::new_spanned(&meta.path, "unknown override"));
                        }
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

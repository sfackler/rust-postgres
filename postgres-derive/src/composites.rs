use proc_macro2::Span;
use std::collections::HashSet;
use syn::{
    punctuated::Punctuated, AngleBracketedGenericArguments, Error, GenericArgument, GenericParam,
    Generics, Ident, Lifetime, Path, PathArguments, PathSegment, Type, TypeParamBound,
};

use crate::{case::RenameRule, overrides::Overrides};

pub struct NamedField {
    pub name: String,
    pub ident: Ident,
    pub type_: Type,
    pub borrowed_lifetimes: HashSet<Lifetime>,
}

impl NamedField {
    pub fn parse(raw: &syn::Field, rename_all: Option<RenameRule>) -> Result<NamedField, Error> {
        let overrides = Overrides::extract(&raw.attrs, false)?;
        let ident = raw.ident.as_ref().unwrap().clone();

        let borrowed_lifetimes = extract_borrowed_lifetimes(raw, &overrides);

        // field level name override takes precedence over container level rename_all override
        let name = match overrides.name {
            Some(n) => n,
            None => {
                let name = ident.to_string();
                let stripped = name.strip_prefix("r#").map(String::from).unwrap_or(name);

                match rename_all {
                    Some(rule) => rule.apply_to_field(&stripped),
                    None => stripped,
                }
            }
        };

        Ok(NamedField {
            name,
            ident,
            type_: raw.ty.clone(),
            borrowed_lifetimes,
        })
    }
}

pub struct UnnamedField {
    pub borrowed_lifetimes: HashSet<Lifetime>,
}

impl UnnamedField {
    pub fn parse(raw: &syn::Field) -> Result<UnnamedField, Error> {
        let overrides = Overrides::extract(&raw.attrs, false)?;
        let borrowed_lifetimes = extract_borrowed_lifetimes(raw, &overrides);
        Ok(UnnamedField { borrowed_lifetimes })
    }
}

pub(crate) fn extract_borrowed_lifetimes(
    raw: &syn::Field,
    overrides: &Overrides,
) -> HashSet<Lifetime> {
    let mut borrowed_lifetimes = HashSet::new();

    // If the field is a reference, it's lifetime should be implicitly borrowed. Serde does
    // the same thing
    if let Type::Reference(ref_type) = &raw.ty {
        borrowed_lifetimes.insert(ref_type.lifetime.to_owned().unwrap());
    }

    // Borrow all generic lifetimes of fields marked with #[postgres(borrow)]
    if overrides.borrows {
        if let Type::Path(type_path) = &raw.ty {
            for segment in &type_path.path.segments {
                if let PathArguments::AngleBracketed(AngleBracketedGenericArguments {
                    args, ..
                }) = &segment.arguments
                {
                    let lifetimes = args.iter().filter_map(|a| match a {
                        GenericArgument::Lifetime(lifetime) => Some(lifetime.to_owned()),
                        _ => None,
                    });
                    borrowed_lifetimes.extend(lifetimes);
                }
            }
        }
    }

    borrowed_lifetimes
}

pub(crate) fn append_generic_bound(mut generics: Generics, bound: &TypeParamBound) -> Generics {
    for param in &mut generics.params {
        if let GenericParam::Type(param) = param {
            param.bounds.push(bound.to_owned())
        }
    }
    generics
}

pub(crate) fn new_derive_path(last: PathSegment) -> Path {
    let mut path = Path {
        leading_colon: None,
        segments: Punctuated::new(),
    };
    path.segments
        .push(Ident::new("postgres_types", Span::call_site()).into());
    path.segments.push(last);
    path
}

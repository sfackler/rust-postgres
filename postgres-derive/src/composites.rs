use proc_macro2::Span;
use syn::{
    punctuated::Punctuated, Error, GenericParam, Generics, Ident, Path, PathSegment, Type,
    TypeParamBound,
};

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

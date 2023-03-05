use proc_macro2::Span;
use syn::{
    punctuated::Punctuated, Error, GenericParam, Generics, Ident, Path, PathSegment, Type,
    TypeParamBound,
};

use crate::{field_variant_overrides::FieldVariantOverrides, struct_overrides::StructOverrides};

pub struct Field {
    pub name: String,
    pub ident: Ident,
    pub type_: Type,
}

impl Field {
    pub fn parse(struct_overrides: &StructOverrides, raw: &syn::Field) -> Result<Field, Error> {
        use convert_case::Casing;

        let mut overrides = FieldVariantOverrides::extract(&raw.attrs)?;

        let ident = raw.ident.as_ref().unwrap().clone();
        Ok(Field {
            name: overrides.name.take().unwrap_or_else(|| {
                let name = ident.to_string();
                let name = name.strip_prefix("r#").map(String::from).unwrap_or(name);
                struct_overrides
                    .rename_all
                    .map(|case| name.to_case(case))
                    .unwrap_or(name)
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

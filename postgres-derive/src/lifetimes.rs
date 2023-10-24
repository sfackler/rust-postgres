use std::collections::HashSet;
use syn::{AngleBracketedGenericArguments, GenericArgument, Lifetime, PathArguments, Type};
use crate::overrides::Overrides;

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

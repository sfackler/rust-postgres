//! An internal crate for `postgres-types`.

#![recursion_limit = "256"]
extern crate proc_macro;

use proc_macro::TokenStream;

mod accepts;
mod composites;
mod enums;
mod fromsql;
mod overrides;
mod tosql;

#[proc_macro_derive(ToSql, attributes(postgres))]
pub fn derive_tosql(input: TokenStream) -> TokenStream {
    let input = syn::parse(input).unwrap();
    tosql::expand_derive_tosql(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#[proc_macro_derive(FromSql, attributes(postgres))]
pub fn derive_fromsql(input: TokenStream) -> TokenStream {
    let input = syn::parse(input).unwrap();
    fromsql::expand_derive_fromsql(input)
        .unwrap_or_else(|e| e.to_compile_error())
        .into()
}

#![recursion_limit = "256"]

extern crate proc_macro;
extern crate syn;
#[macro_use]
extern crate quote;
extern crate proc_macro2;

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
    tosql::expand_derive_tosql(input).unwrap().into()
}

#[proc_macro_derive(FromSql, attributes(postgres))]
pub fn derive_fromsql(input: TokenStream) -> TokenStream {
    let input = syn::parse(input).unwrap();
    fromsql::expand_derive_fromsql(input).unwrap().into()
}

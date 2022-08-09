use proc_macro2::{Span, TokenStream};
use quote::{format_ident, quote};
use std::iter;
use syn::{
    Data, DataStruct, DeriveInput, Error, Fields, GenericParam, Generics, Ident, Lifetime,
    LifetimeDef,
};

use crate::accepts;
use crate::composites::Field;
use crate::enums::Variant;
use crate::overrides::Overrides;

pub fn expand_derive_fromsql(input: DeriveInput) -> Result<TokenStream, Error> {
    let overrides = Overrides::extract(&input.attrs)?;

    if overrides.name.is_some() && overrides.transparent {
        return Err(Error::new_spanned(
            &input,
            "#[postgres(transparent)] is not allowed with #[postgres(name = \"...\")]",
        ));
    }

    let name = overrides.name.unwrap_or_else(|| input.ident.to_string());

    let (accepts_body, to_sql_body) = if overrides.transparent {
        match input.data {
            Data::Struct(DataStruct {
                fields: Fields::Unnamed(ref fields),
                ..
            }) if fields.unnamed.len() == 1 => {
                let field = fields.unnamed.first().unwrap();
                (
                    accepts::transparent_body(field),
                    transparent_body(&input.ident, field),
                )
            }
            _ => {
                return Err(Error::new_spanned(
                    input,
                    "#[postgres(transparent)] may only be applied to single field tuple structs",
                ))
            }
        }
    } else {
        match input.data {
        Data::Enum(ref data) => {
            let variants = data
                .variants
                .iter()
                .map(Variant::parse)
                .collect::<Result<Vec<_>, _>>()?;
            (
                accepts::enum_body(&name, &variants),
                enum_body(&input.ident, &variants),
            )
        }
        Data::Struct(DataStruct {
            fields: Fields::Unnamed(ref fields),
            ..
        }) if fields.unnamed.len() == 1 => {
            let field = fields.unnamed.first().unwrap();
            (
                domain_accepts_body(&name, field),
                domain_body(&input.ident, field),
            )
        }
        Data::Struct(DataStruct {
            fields: Fields::Named(ref fields),
            ..
        }) => {
            let fields = fields
                .named
                .iter()
                .map(Field::parse)
                .collect::<Result<Vec<_>, _>>()?;
            (
                accepts::composite_body(&name, "FromSql", &fields),
                composite_body(&input.ident, &fields),
            )
        }
        _ => {
            return Err(Error::new_spanned(
                input,
                "#[derive(FromSql)] may only be applied to structs, single field tuple structs, and enums",
            ))
        }
    }
    };

    let ident = &input.ident;
    let (generics, lifetime) = build_generics(&input.generics);
    let (impl_generics, _, _) = generics.split_for_impl();
    let (_, ty_generics, where_clause) = input.generics.split_for_impl();
    let out = quote! {
        impl#impl_generics postgres_types::FromSql<#lifetime> for #ident#ty_generics #where_clause {
            fn from_sql(_type: &postgres_types::Type, buf: &#lifetime [u8])
                        -> std::result::Result<#ident#ty_generics,
                                               std::boxed::Box<dyn std::error::Error +
                                                               std::marker::Sync +
                                                               std::marker::Send>> {
                #to_sql_body
            }

            fn accepts(type_: &postgres_types::Type) -> bool {
                #accepts_body
            }
        }
    };

    Ok(out)
}

fn transparent_body(ident: &Ident, field: &syn::Field) -> TokenStream {
    let ty = &field.ty;
    quote! {
        <#ty as postgres_types::FromSql>::from_sql(_type, buf).map(#ident)
    }
}

fn enum_body(ident: &Ident, variants: &[Variant]) -> TokenStream {
    let variant_names = variants.iter().map(|v| &v.name);
    let idents = iter::repeat(ident);
    let variant_idents = variants.iter().map(|v| &v.ident);

    quote! {
        match std::str::from_utf8(buf)? {
            #(
                #variant_names => std::result::Result::Ok(#idents::#variant_idents),
            )*
            s => {
                std::result::Result::Err(
                    std::convert::Into::into(format!("invalid variant `{}`", s)))
            }
        }
    }
}

// Domains are sometimes but not always just represented by the bare type (!?)
fn domain_accepts_body(name: &str, field: &syn::Field) -> TokenStream {
    let ty = &field.ty;
    let normal_body = accepts::domain_body(name, field);

    quote! {
        if <#ty as postgres_types::FromSql>::accepts(type_) {
            return true;
        }

        #normal_body
    }
}

fn domain_body(ident: &Ident, field: &syn::Field) -> TokenStream {
    let ty = &field.ty;
    quote! {
        <#ty as postgres_types::FromSql>::from_sql(_type, buf).map(#ident)
    }
}

fn composite_body(ident: &Ident, fields: &[Field]) -> TokenStream {
    let temp_vars = &fields
        .iter()
        .map(|f| format_ident!("__{}", f.ident))
        .collect::<Vec<_>>();
    let field_names = &fields.iter().map(|f| &f.name).collect::<Vec<_>>();
    let field_idents = &fields.iter().map(|f| &f.ident).collect::<Vec<_>>();

    quote! {
        let fields = match *_type.kind() {
            postgres_types::Kind::Composite(ref fields) => fields,
            _ => unreachable!(),
        };

        let mut buf = buf;
        let num_fields = postgres_types::private::read_be_i32(&mut buf)?;
        if num_fields as usize != fields.len() {
            return std::result::Result::Err(
                std::convert::Into::into(format!("invalid field count: {} vs {}", num_fields, fields.len())));
        }

        #(
            let mut #temp_vars = std::option::Option::None;
        )*

        for field in fields {
            let oid = postgres_types::private::read_be_i32(&mut buf)? as u32;
            if oid != field.type_().oid() {
                return std::result::Result::Err(std::convert::Into::into("unexpected OID"));
            }

            match field.name() {
                #(
                    #field_names => {
                        #temp_vars = std::option::Option::Some(
                            postgres_types::private::read_value(field.type_(), &mut buf)?);
                    }
                )*
                _ => unreachable!(),
            }
        }

        std::result::Result::Ok(#ident {
            #(
                #field_idents: #temp_vars.unwrap(),
            )*
        })
    }
}

fn build_generics(source: &Generics) -> (Generics, Lifetime) {
    let mut out = source.to_owned();
    // don't worry about lifetime name collisions, it doesn't make sense to derive FromSql on a struct with a lifetime
    let lifetime = Lifetime::new("'a", Span::call_site());
    out.params.insert(
        0,
        GenericParam::Lifetime(LifetimeDef::new(lifetime.to_owned())),
    );

    (out, lifetime)
}

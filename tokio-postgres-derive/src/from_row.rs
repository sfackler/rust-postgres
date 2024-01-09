use proc_macro2::TokenStream as TokenStream2;
use quote::quote;

/// Calls the fallible entry point and writes any errors to the token stream.
/// Fallible entry point for generating a `FromRow` implementation
pub fn derive_from_row(input: syn::DeriveInput) -> syn::Result<TokenStream2> {
    Ok(DeriveFromRow::parse(input)?.generate())
}

struct DeriveFromRow {
    ident: syn::Ident,
    generics: syn::Generics,
    fields: Vec<FromRowField>,
}

impl DeriveFromRow {
    fn parse(input: syn::DeriveInput) -> syn::Result<Self> {
        let syn::Data::Struct(syn::DataStruct {
            fields: syn::Fields::Named(fields),
            ..
        }) = input.data
        else {
            let span = syn::spanned::Spanned::span(&input);

            return Err(syn::Error::new(
                span,
                "derive macro `FromRow` is only supported on structs with named fields",
            ));
        };

        let fields = fields
            .named
            .into_iter()
            .map(FromRowField::parse)
            .collect::<syn::Result<_>>()?;

        Ok(Self {
            ident: input.ident,
            generics: input.generics,
            fields,
        })
    }
    /// Generate the `FromRow` implementation.
    fn generate(self) -> TokenStream2 {
        let ident = &self.ident;

        let (impl_generics, ty_generics, where_clause) = self.generics.split_for_impl();

        let original_predicates = where_clause.iter().flat_map(|w| &w.predicates);

        let mut predicates = Vec::new();
        for field in &self.fields {
            field.push_predicates(&mut predicates)
        }

        let from_row_fields = self
            .fields
            .iter()
            .map(FromRowField::generate)
            .collect::<Vec<_>>();

        quote! {
            impl #impl_generics ::tokio_postgres::FromRow for #ident #ty_generics where #(#original_predicates,)* #(#predicates,)* {

                fn from_row(row: &::tokio_postgres::Row) -> ::std::result::Result<Self, ::tokio_postgres::Error> {
                    ::std::result::Result::Ok(Self {
                        #(#from_row_fields),*
                    })
                }
            }
        }
    }
}

/// A single field inside of a struct that derives `FromRow`
// #[darling(attributes(from_row), forward_attrs(allow, doc, cfg))]
struct FromRowField {
    /// The identifier of this field.
    ident: syn::Ident,
    /// The identifier of this field as a string.
    ident_str: String,
    /// The type specified in this field.
    ty: syn::Type,
    /// Any attributes that are captured by this macro.
    attrs: FromRowFieldAttrs,
}

impl FromRowField {
    fn parse(input: syn::Field) -> syn::Result<Self> {
        // This can't panic as long as we make sure we're working with named structs.
        let ident = input.ident.expect("must be a named field");

        Ok(Self {
            ident_str: ident.to_string(),
            ident,
            ty: input.ty,
            attrs: FromRowFieldAttrs::parse(input.attrs)?,
        })
    }

    /// Returns a token stream of the type that should be returned from either
    /// `FromRow` (when using `flatten`) or `FromSql`.
    fn target_ty(&self) -> &syn::Type {
        if let Some(from) = &self.attrs.from {
            from
        } else if let Some(try_from) = &self.attrs.try_from {
            try_from
        } else {
            &self.ty
        }
    }

    /// Returns the name that maps to the actual sql column.
    /// By default this is the same as the rust field name but can be overwritten by `#[from_row(rename = "..")]`.
    fn column_name(&self) -> &str {
        if let Some(rename) = &self.attrs.rename {
            rename
        } else {
            &self.ident_str
        }
    }

    /// Pushes the needed where clause predicates for this field.
    ///
    /// By default this is `T: for<'a> postgres::types::FromSql<'a>`,
    /// when using `flatten` it's: `T: postgres_from_row::FromRow`
    /// and when using either `from` or `try_from` attributes it additionally pushes this bound:
    /// `T: std::convert::From<R>`, where `T` is the type specified in the struct and `R` is the
    /// type specified in the `[try]_from` attribute.
    fn push_predicates(&self, predicates: &mut Vec<TokenStream2>) {
        let target_ty = self.target_ty();
        let ty = &self.ty;

        if self.attrs.flatten {
            predicates.push(quote! (#target_ty: ::tokio_postgres::FromRow))
        } else if self.attrs.skip {
            predicates.push(quote! (#target_ty: ::std::default::Default))
        } else {
            predicates.push(quote! (#target_ty: for<'a> ::tokio_postgres::types::FromSql<'a>))
        };

        if self.attrs.from.is_some() {
            predicates.push(quote!(#ty: std::convert::From<#target_ty>))
        } else if self.attrs.try_from.is_some() {
            let try_from = quote!(::std::convert::TryFrom<#target_ty>);

            predicates.extend([
                quote!(#ty: #try_from),
                quote!(::tokio_postgres::Error: ::std::convert::From<<#ty as #try_from>::Error>),
            ]);
        }
    }

    /// Generate the line needed to retrieve this field from a row when calling `from_row`.
    fn generate(&self) -> TokenStream2 {
        let ident = &self.ident;
        let column_name = self.column_name();
        let field_ty = &self.ty;
        let target_ty = self.target_ty();

        let mut base = if self.attrs.flatten {
            quote!(<#target_ty as ::tokio_postgres::FromRow>::from_row(row)?)
        } else if self.attrs.skip {
            quote!(<#field_ty as ::std::default::Default>::default())
        } else {
            quote!(::tokio_postgres::Row::try_get::<&::std::primitive::str, #target_ty>(row, #column_name)?)
        };

        if self.attrs.from.is_some() {
            base = quote!(<#field_ty as ::std::convert::From<#target_ty>>::from(#base));
        } else if self.attrs.try_from.is_some() {
            base = quote!(<#field_ty as ::std::convert::TryFrom<#target_ty>>::try_from(#base)?);
        };

        quote!(#ident: #base)
    }
}

#[derive(Default)]
struct FromRowFieldAttrs {
    /// Whether to flatten this field. Flattening means calling the `FromRow` implementation
    /// of `self.ty` instead of extracting it directly from the row.
    // #[darling(default)]
    flatten: bool,
    /// Optionally use this type as the target for `FromRow` or `FromSql`, and then
    /// call `TryFrom::try_from` to convert it the `self.ty`.
    try_from: Option<syn::Type>,
    /// Optionally use this type as the target for `FromRow` or `FromSql`, and then
    /// call `From::from` to convert it the `self.ty`.
    from: Option<syn::Type>,
    /// Override the name of the actual sql column instead of using `self.ident`.
    /// Is not compatible with `flatten` since no column is needed there.
    rename: Option<String>,
    /// Skip this field when looking for columns in the row, instead initialize it using
    /// `Default::default`.
    skip: bool,
}

impl FromRowFieldAttrs {
    fn parse(attrs: Vec<syn::Attribute>) -> syn::Result<Self> {
        let mut this = Self::default();

        for attr in attrs {
            if !attr.path().is_ident("from_row") {
                continue;
            }

            attr.parse_nested_meta(|meta| {
                if meta.path.is_ident("flatten") {
                    this.flatten = true
                } else if meta.path.is_ident("skip") {
                    this.skip = true
                } else if meta.path.is_ident("try_from") {
                    let lit: syn::LitStr = meta.value()?.parse()?;
                    this.try_from = Some(lit.parse()?)
                } else if meta.path.is_ident("from") {
                    let lit: syn::LitStr = meta.value()?.parse()?;
                    this.from = Some(lit.parse()?)
                } else if meta.path.is_ident("rename") {
                    let lit: syn::LitStr = meta.value()?.parse()?;
                    this.rename = Some(lit.value())
                } else {
                    return Err(meta.error("unexpected `from_row` attribute."));
                }

                if this.rename.is_some() && this.flatten {
                    return Err(meta.error(
                        r#"can't combine `#[from_row(flatten)]` with `#[from_row(rename = "..")]`"#,
                    ));
                }

                if this.skip && (this.rename.is_some() || this.try_from.is_some() || this.flatten || this.rename.is_some()) {
                    return Err(meta.error(r#"can't combine `#[from_row(skip)]` with other attributes"#))
                }

                if this.from.is_some() && this.try_from.is_some() {
                    return Err(meta.error(
                        r#"can't combine `#[from_row(try_from = "..")]` with `#[from_row(from = "..")]`"#,
                    ));
                }

                Ok(())
            })?;
        }

        Ok(this)
    }
}

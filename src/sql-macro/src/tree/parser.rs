use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{punctuated::Punctuated, spanned::Spanned, DeriveInput, Field, Fields, Variant};

use crate::{attribute::AttributeExtractor, utils::parse_string_value};

const MAX_CHOICES: usize = 20;
const BOXED_ENUM_MIN_VARIANTS: usize = 5;

/// The trait to derive for tree parsers.
const TRAIT: &str = "TreeParser";
/// The attribute name used when deriving the tree parser trait.
const ATTRIBUTE: &str = "parser";

struct AttributeArgment;

impl AttributeArgment {
    const DEPENDENCY: &'static str = "dependency";
    const LABEL: &'static str = "label";
    const FUNCTION: &'static str = "function";
}

enum ParserDependency {
    None,
    One(Type),
    Tupe(Vec<Type>),
}

impl ParserDependency {
    fn extract(e: Option<Expr>) -> syn::Result<Self> {
        e.map(|value| {
            let t = parse_string_value(&value, Type::parse)?;
            match t {}
        })
        .unwrap_or_else(|| Ok(Self::None))
    }
}

pub(crate) fn derive_tree_parser(input: DeriveInput) -> syn::Result<TokenStream> {
    let name = &input.ident;

    let parser = match &input.data {
        syn::Data::Struct(data_struct) => derive_struct(name, &data_struct.fields)?,
        syn::Data::Enum(data_enum) => derive_enum(name, &data_enum.variants)?,
        syn::Data::Union(data_union) => {
            return Err(syn::Error::new(
                input.span(),
                format!("`{TRAIT}` can only be derived for enums or structs"),
            ))
        }
    };

    let (dependency, label) = {
        let mut extractor = AttributeExtractor::try_new(ATTRIBUTE, &input.attrs)?;
        let dependency = extractor
            .extract_argument_value(AttributeArgment::DEPENDENCY, ParserDependency::extract)?;
        let label = extractor.extract_argument_value(AttributeArgment::LABEL, Ok)?;

        extractor.extract_empty()?;
        (dependency, label)
    };

    let parser = match label {
        Some(label) => quote! {#parser.labelled(#label)},
        None => parser,
    };

    let (generics, args_type, args_bounds) = match dependency {
        ParserDependency::None => (quote! {I, E}, quote! {()}, quote! {}),
        ParserDependency::One(item) => (
            quote! {I, E, P},
            quote! {P},
            quote! {P: chumsky::Parser<'a, I, #item, E> + Clone + 'a},
        ),
        ParserDependency::Tupe(items) => {
            let params = (0..items.len())
                .map(|i| format_ident!("P{}", i + 1))
                .collect::<Vec<_>>();
            let bounds = items
                .iter()
                .zip(params.iter())
                .map(|(item, p)| {
                    quote! { #p: chumsky::Parser<'a, I, #item, E> + Clone + 'a}
                })
                .collect::<Vec<_>>();
            (
                quote! {I, E, #(#params),* },
                quote! {(#(#params),*,)},
                quote! {#(#bounds),*},
            )
        }
    };

    let trait_name = format_ident!("{TRAIT}");

    Ok(quote! {
        impl <'a, #generics> crate::tree::#trait_name <'a, I, E, #args_type> for #name
        where
            I: chumsky::input::Input<'a, Token = crate::token::Token<'a>>
                + chumsky::input::ValueInput<'a>,
            I::Span: std::convert::Into<crate::span::TokenSpan> + Clone,
            E: chumsky::extra::ParserExtra<'a, I>,
            E::Error: chumsky::label::LabelError<'a, I, crate::token::TokenLabel>,
            #args_bounds
        {
            fn parser(
                args: #args_type,
                options: &'a crate::options::ParserOptions
            ) -> impl chumsky::Parser<'a, I, Self, E> + Clone {
                use chumsky::Parser;

                #parser
            }
        }
    })
}

fn derive_enum(
    enum_name: &Ident,
    variants: &Punctuated<Variant, Token![,]>,
) -> syn::Result<TokenStream> {
    if variants.is_empty() {
        return Err(syn::Error::new(
            variants.span(),
            format!("cannot derive `{TRAIT}` for empty enums"),
        ));
    }

    let choices = variants
        .iter()
        .map(|variant| derive_enum_variant(enum_name, variant))
        .collect::<syn::Result<Vec<_>>>()?;
    let n = choices.len();
    let parser = derive_choices(choices);
    let token_stream = if n < BOXED_ENUM_MIN_VARIANTS {
        quote! { #parser }
    } else {
        quote! {#parser.boxed()}
    };

    Ok(token_stream)
}

fn derive_choices(choices: Vec<TokenStream>) -> TokenStream {
    let choices = if choices.len() <= MAX_CHOICES {
        choices
    } else {
        let chunk_size = choices.len().div_ceil(MAX_CHOICES);
        choices
            .chunks(chunk_size)
            .map(|chunk| derive_choices(chunk.to_vec()))
            .collect()
    };
    if choices.len() > 1 {
        quote! {chumsky::prelude::choice((#(#choices), *))}
    } else {
        quote! { #(#choices), *}
    }
}

fn derive_enum_variant(enum_name: &Ident, variant: &Variant) -> syn::Result<TokenStream> {
    AttributeExtractor::try_new(ATTRIBUTE, &variant.attrs)?.extract_empty()?;
    let variant_name = &variant.ident;
    let name = quote! {#enum_name::#variant_name};
    derive_fields(name, variant, &variant.fields)
}

fn derive_struct(struct_name: &Ident, fields: &Fields) -> syn::Result<TokenStream> {
    derive_fields(quote! {#struct_name}, fields, fields)
}

struct ParseFields {
    parser: TokenStream,
    args: TokenStream,
    initializer: TokenStream,
}

fn derive_fields_inner<'a>(
    spanned: impl Spanned,
    fields: impl IntoIterator<Item = &'a Field>,
) -> syn::Result<ParseFields> {
    fields
        .into_iter()
        .enumerate()
        .try_fold(None, |acc, (i, field)| -> sync::Result<_> {
            let field_function = {
                let mut extractor = AttributeExtractor::try_new(ATTRIBUTE, &field.attrs)?;
                let func = extractor.extract_argument_value(AttributeArgment::FUNCTION, Ok)?;
                extractor.extract_empty()?;
                func
            };
            let field_arg = field
                .ident
                .to_owned()
                .unwrap_or_else(|| format_ident!("v{}", i));
            let field_type = &field.ty;
            let field_parser = if let Some(function) = field_function {
                quote! {{ let func = #function; func(args.clone(), options)}}
            } else {
                quote! {{ <#field_type>::parser((), options)}}
            };

            match acc {
                Some(ParseFields {
                    parser,
                    args,
                    initializer,
                }) => Ok(Some(ParseFields {
                    parser: quote! {#parser.then(#field_parser)},
                    args: quote! {(#args, #field_arg)},
                    initializer: quote! { #initializer, #field_arg},
                })),
                None => Ok(Some(ParseFields {
                    parser: field_parser,
                    args: quote! { #field_arg },
                    initializer: quote! { #field_arg },
                })),
            }
        })?
        .ok_or_else(|| syn::Error::new(spanned.span(), format!("cannot derive `{}` for no fields")))
}

fn derive_fields(
    name: TokenStream,
    spanned: impl Spanned,
    fields: &Fields,
) -> syn::Result<TokenStream> {
    match fields {
        Fields::Named(fields_named) => {
            let ParseFields {
                parser,
                args,
                initializer,
            } = derive_fields_inner(spanned, &fields_name.named)?;
            Ok(quote! {
                #parser.map(|#args| #name { #initializer }).boxed()
            })
        }
        Fields::Unnamed(fields_unnamed) => {
            let ParseFields {
                parser,
                args,
                initializer,
            } = derive_fields_inner(spanned, &fields_unnamed.unnamed)?;
            Ok(quote! { #parser.map(|#args| #name ( #initializer))})
        }
        Fields::Unit => Err(syn::Error::new(
            spanned.span(),
            format!("cannot derive `{TRAIT}` for unit fields"),
        )),
    }
}

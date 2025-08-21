use proc_macro::TokenStream;
use syn::{parse_macro_input, DeriveInput};

mod attribute;
mod tree;
mod utils;

#[proc_macro_derive(TreeParser, attributes(parser))]
pub fn derive_tree_parser(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    tree::parser::derive_tree_parser(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

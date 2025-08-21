use std::mem;

use syn::{punctuated::Punctuated, spanned::Spanned, Attribute, Meta, MetaNameValue, Token};

pub struct AttributeExtractor<'a> {
    name: &'a str,
    arguments: Vec<MetaNameValue>,
    paths: Vec<Path>,
}

impl<'a> AttributeExtractor<'a> {
    pub fn try_new(name: &'a str, attributes: &[Attribute]) -> syn::Result<Self> {
        let mut arguments = Vec::new();
        let mut paths = Vec::new();

        for attr in attributes {
            if !attr.path().is_ident(name) {
                continue;
            }

            let nested = attr.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)?;
            for meta in nested {
                match meta {
                    Meta::Path(path) => paths.push(path),
                    Meta::List(_meta_list) => {
                        return Err(syn::Error::new(meta.span(), "invalid attribute value"))
                    }
                    Meta::NameValue(meta_name_value) => arguments.push(meta_name_value),
                }
            }

            Ok(Self {
                name,
                arguments,
                paths,
            })
        }
    }

    pub fn extract_empty(&self) -> syn::Result<()> {
        if let Some(x) = self.arguments.first() {
            Err(syn::Error::new(
                x.span(),
                format!("unexpected `{}` attribute path", self.name),
            ))
        } else {
            Ok(())
        }
    }

    pub fn extract_argument_value<T, F>(&mut self, argument: &str, transform: F) -> syn::Result<T>
    where
        F: FnOnce(Option<Expr>) -> syn::Result<T>,
    {
        let argments = mem::take(&mut self.arguments);
        let (mut extracted, remaining) = argments
            .into_iter()
            .partition::<Vec<_>, _>(|x| x.path.is_ident(argument));
        self.arguments = remaining;
        let one = extracted.pop();

        if let Some(other) = extracted.last() {
            Err(syn::Error::new(
                other.span(),
                format!(
                    "duplicated `{}` argument for the `{}` attribute",
                    argument, self.name
                ),
            ))
        } else {
            transform(one.map(|x| x.value))
        }
    }

    #[allow(unused)]
    pub fn extract_path(&mut self, path: &str) -> syn::Result<Option<()>> {
        let paths = mem::take(&mut self.paths);
        let (mut extracted, remaining) = paths
            .into_iter()
            .partition::<Vec<_>, _>(|x| x.is_ident(path));

        self.paths = remaining;
        let one = extracted.pop();
        if let Some(other) = extracted.last() {
            Err(syn::Error::new(
                other.span(),
                format!(
                    "duplicated `{}` path for the `{}` attribute",
                    path, self.name
                ),
            ))
        } else {
            Ok(one.map(|_| ()))
        }
    }
}

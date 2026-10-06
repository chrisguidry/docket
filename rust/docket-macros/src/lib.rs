//! The `#[derive(Task)]` macro for docket-rs.  Use it through `docket::Task`.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, LitStr, Path, Type, parse_macro_input};

/// Implements `docket::Task` for an argument type.
///
/// ```ignore
/// #[derive(Serialize, Deserialize, Task)]
/// #[task(name = "charge", output = Receipt)]
/// pub struct Charge {
///     pub customer: u64,
///     pub cents: u64,
/// }
/// ```
///
/// `name` is required, because queued tasks refer to it: a name taken from
/// the type would change when the type is renamed and strand those tasks.
/// `output` defaults to `()`.  `crate = path` names the docket crate when it
/// is not available as `docket`.
#[proc_macro_derive(Task, attributes(task))]
#[cfg_attr(coverage_nightly, coverage(off))]
pub fn derive_task(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

fn expand(input: &DeriveInput) -> syn::Result<TokenStream2> {
    let mut name: Option<LitStr> = None;
    let mut output: Option<Type> = None;
    let mut krate: Option<Path> = None;

    for attribute in input.attrs.iter().filter(|a| a.path().is_ident("task")) {
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("name") {
                name = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("output") {
                output = Some(meta.value()?.parse()?);
            } else if meta.path.is_ident("crate") {
                krate = Some(meta.value()?.parse()?);
            } else {
                return Err(meta.error("expected `name`, `output`, or `crate`"));
            }
            Ok(())
        })?;
    }

    let Some(name) = name else {
        return Err(syn::Error::new_spanned(
            &input.ident,
            "#[derive(Task)] needs #[task(name = \"...\")]",
        ));
    };
    if name.value().is_empty() {
        return Err(syn::Error::new_spanned(
            &name,
            "a task name cannot be empty",
        ));
    }
    let output = output.map_or_else(|| quote!(()), |output| quote!(#output));
    let krate = krate.map_or_else(|| quote!(::docket), |krate| quote!(#krate));

    let ident = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics #krate::Task for #ident #type_generics #where_clause {
            const NAME: &'static str = #name;
            type Output = #output;
        }
    })
}

#[cfg(test)]
mod tests;

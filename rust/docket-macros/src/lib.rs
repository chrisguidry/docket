//! The `#[derive(Task)]` macro for docket-rs.  Use it through `docket::Task`.

#![cfg_attr(coverage_nightly, feature(coverage_attribute))]

use proc_macro::TokenStream;
use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::ext::IdentExt;
use syn::{Data, DeriveInput, Field, Fields, LitStr, Path, Type, parse_macro_input};

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
///
/// On a named field, `#[task(logged)]` shows its value in a worker's log
/// lines, and `#[task(logged(length_only))]` shows only its length.  The
/// field goes by its serialized name, after serde's `rename` and
/// `rename_all`.
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

    let fields = task_fields(input, &krate)?;

    let ident = &input.ident;
    let (impl_generics, type_generics, where_clause) = input.generics.split_for_impl();
    Ok(quote! {
        impl #impl_generics #krate::Task for #ident #type_generics #where_clause {
            const NAME: &'static str = #name;
            type Output = #output;
            #fields
        }
    })
}

/// The `FIELDS` constant for a struct with named fields, or nothing for any
/// other shape, whose fields have no names to show in a log line.
fn task_fields(input: &DeriveInput, krate: &TokenStream2) -> syn::Result<TokenStream2> {
    let all: Vec<&Field> = match &input.data {
        Data::Struct(data) => data.fields.iter().collect(),
        Data::Enum(data) => data.variants.iter().flat_map(|v| v.fields.iter()).collect(),
        Data::Union(data) => data.fields.named.iter().collect(),
    };
    let named =
        matches!(&input.data, Data::Struct(data) if matches!(data.fields, Fields::Named(_)));
    let rename_all = serde_name(&input.attrs, "rename_all")?;
    let mut entries = Vec::new();
    for field in all {
        let logged = logged(field)?;
        let Some(ident) = field.ident.as_ref().filter(|_| named) else {
            if logged.is_some() {
                return Err(syn::Error::new_spanned(
                    field,
                    "`logged` needs a field of a struct with named fields",
                ));
            }
            continue;
        };
        let name = match serde_name(&field.attrs, "rename")? {
            Some(name) => name,
            None => {
                renamed(&ident.unraw().to_string(), rename_all.as_deref()).ok_or_else(|| {
                    syn::Error::new_spanned(&input.ident, "unknown `rename_all` rule")
                })?
            }
        };
        let variant = match logged {
            None => quote!(Hidden),
            Some(false) => quote!(Value),
            Some(true) => quote!(Length),
        };
        entries.push(quote! {
            #krate::TaskField { name: #name, logged: #krate::Logged::#variant }
        });
    }
    if !named {
        return Ok(TokenStream2::new());
    }
    Ok(quote! {
        const FIELDS: &'static [#krate::TaskField] = &[#(#entries),*];
    })
}

/// The value of a serde setting such as `rename = "..."`, or its
/// `serialize` half in `rename(serialize = "...")`, among `attrs`.  Other
/// serde settings are skipped.
fn serde_name(attrs: &[syn::Attribute], setting: &str) -> syn::Result<Option<String>> {
    let mut found = None;
    for attribute in attrs.iter().filter(|a| a.path().is_ident("serde")) {
        attribute.parse_nested_meta(|meta| {
            let wanted = meta.path.is_ident(setting);
            // A flag such as `default` has neither a value nor a list.
            if meta.input.peek(syn::token::Paren) {
                meta.parse_nested_meta(|inner| {
                    let value: LitStr = inner.value()?.parse()?;
                    if wanted && inner.path.is_ident("serialize") {
                        found = Some(value.value());
                    }
                    Ok(())
                })?;
            } else if let Ok(value) = meta.value() {
                let value: syn::Expr = value.parse()?;
                if let (
                    true,
                    syn::Expr::Lit(syn::ExprLit {
                        lit: syn::Lit::Str(name),
                        ..
                    }),
                ) = (wanted, value)
                {
                    found = Some(name.value());
                }
            }
            Ok(())
        })?;
    }
    Ok(found)
}

/// A `snake_case` field name after a serde `rename_all` rule, or `None` for a
/// rule serde does not have.
fn renamed(field: &str, rule: Option<&str>) -> Option<String> {
    let capitalized = |word: &str| {
        let mut characters = word.chars();
        characters.next().map_or_else(String::new, |first| {
            first.to_uppercase().chain(characters).collect()
        })
    };
    let words: Vec<&str> = field.split('_').collect();
    Some(match rule {
        None | Some("snake_case") => field.to_owned(),
        Some("lowercase") => field.to_lowercase(),
        Some("UPPERCASE" | "SCREAMING_SNAKE_CASE") => field.to_uppercase(),
        Some("PascalCase") => words.iter().map(|w| capitalized(w)).collect(),
        Some("camelCase") => {
            let pascal: String = words.iter().map(|w| capitalized(w)).collect();
            let mut characters = pascal.chars();
            characters.next().map_or_else(String::new, |first| {
                first.to_lowercase().chain(characters).collect()
            })
        }
        Some("kebab-case") => words.join("-"),
        Some("SCREAMING-KEBAB-CASE") => words.join("-").to_uppercase(),
        Some(_) => return None,
    })
}

/// Whether a field is logged, and if so whether only its length is: `None`,
/// `Some(false)` for `logged`, or `Some(true)` for `logged(length_only)`.
fn logged(field: &Field) -> syn::Result<Option<bool>> {
    let mut logged = None;
    for attribute in field.attrs.iter().filter(|a| a.path().is_ident("task")) {
        attribute.parse_nested_meta(|meta| {
            if !meta.path.is_ident("logged") {
                return Err(meta.error("expected `logged`"));
            }
            logged = Some(false);
            if meta.input.is_empty() || meta.input.peek(syn::Token![,]) {
                return Ok(());
            }
            meta.parse_nested_meta(|inner| {
                if inner.path.is_ident("length_only") {
                    logged = Some(true);
                    Ok(())
                } else {
                    Err(inner.error("expected `length_only`"))
                }
            })
        })?;
    }
    Ok(logged)
}

#[cfg(test)]
mod tests;

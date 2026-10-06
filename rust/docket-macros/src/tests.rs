use quote::quote;
use rstest::rstest;
use syn::parse_quote;

use super::expand;

fn expanded(input: &syn::DeriveInput) -> String {
    expand(input).unwrap().to_string()
}

fn error(input: &syn::DeriveInput) -> String {
    expand(input).unwrap_err().to_string()
}

#[test]
fn implements_the_trait_with_the_name_and_output() {
    let input = parse_quote! {
        #[task(name = "charge", output = Receipt)]
        struct Charge { customer: u64 }
    };
    let expected = quote! {
        impl ::docket::Task for Charge {
            const NAME: &'static str = "charge";
            type Output = Receipt;
            const FIELDS: &'static [::docket::TaskField] = &[
                ::docket::TaskField { name: "customer", logged: ::docket::Logged::Hidden }
            ];
        }
    };
    assert_eq!(expanded(&input), expected.to_string());
}

#[test]
fn lists_how_each_field_is_logged() {
    let input = parse_quote! {
        #[task(name = "charge")]
        struct Charge {
            #[task(logged)]
            customer: u64,
            #[serde(default)]
            card: String,
            #[task(logged(length_only))]
            items: Vec<u64>,
        }
    };
    let expected = quote! {
        const FIELDS: &'static [::docket::TaskField] = &[
            ::docket::TaskField { name: "customer", logged: ::docket::Logged::Value },
            ::docket::TaskField { name: "card", logged: ::docket::Logged::Hidden },
            ::docket::TaskField { name: "items", logged: ::docket::Logged::Length }
        ];
    };
    assert!(expanded(&input).contains(&expected.to_string()));
}

#[rstest]
#[case::unit(parse_quote!(#[task(name = "t")] struct T;))]
#[case::tuple(parse_quote!(#[task(name = "t")] struct T(u64, String);))]
#[case::enumeration(parse_quote!(#[task(name = "t")] enum T { A { value: u64 }, B(u64) }))]
fn lists_no_fields_without_names(#[case] input: syn::DeriveInput) {
    assert!(!expanded(&input).contains("FIELDS"));
}

#[test]
fn output_defaults_to_unit() {
    let input = parse_quote! {
        #[task(name = "nightly-cleanup")]
        struct NightlyCleanup;
    };
    assert!(expanded(&input).contains("type Output = () ;"));
}

#[test]
fn crate_names_the_docket_path() {
    let input = parse_quote! {
        #[task(name = "charge", crate = my_docket)]
        struct Charge;
    };
    assert!(expanded(&input).starts_with("impl my_docket :: Task for Charge"));
}

#[test]
fn keeps_generics_and_bounds() {
    let input = parse_quote! {
        #[task(name = "wrap")]
        struct Wrap<T> where T: Clone { value: T }
    };
    assert!(
        expanded(&input).starts_with("impl < T > :: docket :: Task for Wrap < T > where T : Clone")
    );
}

#[test]
fn reads_several_task_attributes() {
    let input = parse_quote! {
        #[task(name = "charge")]
        #[doc = "ignored"]
        #[task(output = Receipt)]
        struct Charge;
    };
    assert!(expanded(&input).contains("type Output = Receipt ;"));
}

#[rstest]
#[case::no_name(parse_quote!(#[task(output = Receipt)] struct T;), "#[derive(Task)] needs #[task(name = \"...\")]")]
#[case::empty_name(parse_quote!(#[task(name = "")] struct T;), "a task name cannot be empty")]
#[case::unknown_setting(parse_quote!(#[task(name = "t", retries = 3)] struct T;), "expected `name`, `output`, or `crate`")]
#[case::name_not_a_string(parse_quote!(#[task(name = t)] struct T;), "expected string literal")]
#[case::name_without_value(parse_quote!(#[task(name)] struct T;), "expected `=`")]
#[case::output_without_value(parse_quote!(#[task(name = "t", output)] struct T;), "expected `=`")]
#[case::crate_without_value(parse_quote!(#[task(name = "t", crate)] struct T;), "expected `=`")]
#[case::output_not_a_type(parse_quote!(#[task(name = "t", output = 3)] struct T;), "expected one of:")]
#[case::crate_not_a_path(parse_quote!(#[task(name = "t", crate = "docket")] struct T;), "expected identifier")]
#[case::unknown_field_setting(parse_quote!(#[task(name = "t")] struct T { #[task(secret)] a: u64 }), "expected `logged`")]
#[case::unknown_logged_setting(parse_quote!(#[task(name = "t")] struct T { #[task(logged(short))] a: u64 }), "expected `length_only`")]
#[case::logged_tuple_field(parse_quote!(#[task(name = "t")] struct T(#[task(logged)] u64);), "`logged` needs a field of a struct with named fields")]
#[case::logged_variant_field(parse_quote!(#[task(name = "t")] enum T { A { #[task(logged)] a: u64 } }), "`logged` needs a field of a struct with named fields")]
#[case::logged_union_field(parse_quote!(#[task(name = "t")] union T { #[task(logged)] a: u64 }), "`logged` needs a field of a struct with named fields")]
fn rejects_bad_settings(#[case] input: syn::DeriveInput, #[case] message: &str) {
    assert!(error(&input).starts_with(message), "{}", error(&input));
}

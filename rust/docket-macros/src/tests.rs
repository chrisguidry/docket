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
        }
    };
    assert_eq!(expanded(&input), expected.to_string());
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
fn rejects_bad_settings(#[case] input: syn::DeriveInput, #[case] message: &str) {
    assert!(error(&input).starts_with(message), "{}", error(&input));
}

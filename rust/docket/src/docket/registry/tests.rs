use serde_json::json;

use super::Registered;
use crate::context::Context;
use crate::docket::Docket;

async fn call(registered: &Registered, args: &str) -> crate::behaviors::Outcome {
    let docket = Docket::connect("registry", format!("memory://{}", uuid::Uuid::now_v7()))
        .await
        .unwrap();
    (registered.handler)(Context::for_tests(&docket, "k", "anything"), args).await
}

fn echo_function() -> Registered {
    Registered::fallback(|ctx: Context, args: serde_json::Value| async move {
        Ok::<_, std::io::Error>(json!({ "function": ctx.function(), "args": args }))
    })
}

#[tokio::test]
async fn a_fallback_gets_the_arguments_as_json() {
    let output = call(&echo_function(), r#"{"customer":7}"#).await.unwrap();
    assert_eq!(
        output,
        json!({ "function": "anything", "args": { "customer": 7 } })
    );
}

#[tokio::test]
async fn a_fallback_fails_arguments_that_are_not_json() {
    let error = call(&echo_function(), "{").await.unwrap_err();
    assert!(error.to_string().contains("EOF"), "{error}");
}

#[tokio::test]
async fn a_fallback_passes_its_errors_through() {
    let failing = Registered::fallback(|_ctx: Context, _args: serde_json::Value| async move {
        Err::<serde_json::Value, _>(std::io::Error::other("no such task here"))
    });
    assert_eq!(
        call(&failing, "{}").await.unwrap_err().to_string(),
        "no such task here"
    );
}

#[test]
fn a_registration_debugs_without_its_handler() {
    assert_eq!(format!("{:?}", echo_function()), "Registered { .. }");
}

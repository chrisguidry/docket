use std::collections::HashMap;

use crate::docket::Docket;

async fn memory() -> Docket {
    Docket::connect("limits", format!("memory://{}", uuid::Uuid::now_v7()))
        .await
        .unwrap()
}

fn one_run(key: &str) -> HashMap<String, u32> {
    HashMap::from([(key.to_owned(), 1)])
}

#[tokio::test]
async fn a_later_limit_on_the_same_key_leaves_the_earlier_count_alone() {
    let docket = memory().await;
    let first = docket.limit_runs(&one_run("k"));
    first.count_run("k");

    let _second = docket.limit_runs(&HashMap::from([("k".to_owned(), 5)]));

    assert!(docket.has_run_out("k"));
}

#[tokio::test]
async fn dropping_one_limit_leaves_another_on_the_same_key() {
    let docket = memory().await;
    let first = docket.limit_runs(&one_run("k"));
    let second = docket.limit_runs(&one_run("k"));
    first.count_run("k");

    drop(second);

    assert!(docket.has_run_out("k"));
}

#[tokio::test]
async fn a_key_is_free_once_every_limit_on_it_is_dropped() {
    let docket = memory().await;
    let first = docket.limit_runs(&one_run("k"));
    first.count_run("k");

    drop(first);

    assert!(!docket.has_run_out("k"));
}

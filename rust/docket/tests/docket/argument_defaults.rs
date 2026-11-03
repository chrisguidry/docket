//! A per-field behavior counts a field the message left out as the value the
//! task runs with, its serde default, as pydocket counts an omitted argument
//! under its default.

use std::sync::{Arc, Mutex};
use std::time::Duration;

use docket::{ConcurrencyLimit, Cooldown, Docket, RateLimit, Registration, Task};
use rstest::rstest;
use serde::{Deserialize, Serialize};

use crate::support::{docket, within, worker};

/// The arguments a producer writes when it leaves the customer out.
#[derive(Serialize, Deserialize, Task)]
#[task(name = "per-customer-or-default")]
struct WithoutCustomer {}

#[derive(Serialize, Deserialize, Task)]
#[task(name = "per-customer-or-default")]
struct PerCustomerOrDefault {
    #[serde(default = "first_customer")]
    customer: u32,
}

fn first_customer() -> u32 {
    1
}

/// Registers the task, recording the customer of each run.
fn register(docket: &Docket) -> (Registration<PerCustomerOrDefault>, Arc<Mutex<Vec<u32>>>) {
    let runs = Arc::new(Mutex::new(Vec::new()));
    let ran = Arc::clone(&runs);
    let registration = docket.register(move |_ctx, args: PerCustomerOrDefault| {
        ran.lock().unwrap().push(args.customer);
        async { Ok::<_, std::io::Error>(()) }
    });
    (registration, runs)
}

type Limit = fn(Registration<PerCustomerOrDefault>, &str);

#[rstest]
#[case::cooldown(|registration: Registration<PerCustomerOrDefault>, scope: &str| {
    registration.with(Cooldown::per_field("customer", Duration::from_secs(60)).scope(scope));
})]
#[case::rate_limit(|registration: Registration<PerCustomerOrDefault>, scope: &str| {
    registration.with(
        RateLimit::per_field("customer", 1)
            .per(Duration::from_secs(60))
            .drop_excess()
            .scope(scope),
    );
})]
#[tokio::test]
async fn a_left_out_field_counts_as_its_default(#[case] limit: Limit) {
    let docket = docket().await;
    let (registration, runs) = register(&docket);
    limit(registration, docket.name());
    docket
        .add(PerCustomerOrDefault { customer: 1 })
        .await
        .unwrap();
    docket.add(WithoutCustomer {}).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(*runs.lock().unwrap(), [1]);
}

#[tokio::test]
async fn a_concurrency_limit_runs_a_left_out_field_under_its_default() {
    let docket = docket().await;
    let (registration, runs) = register(&docket);
    registration.with(ConcurrencyLimit::per_field("customer", 1));
    docket.add(WithoutCustomer {}).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(*runs.lock().unwrap(), [1]);
}

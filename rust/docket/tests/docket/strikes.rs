use std::time::Duration;

use docket::{Disposition, State, Strike, Task};
use serde::{Deserialize, Serialize};

use crate::support::{Noop, docket, within, worker};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize, Task)]
#[task(name = "charge")]
struct Charge {
    customer: u32,
}

#[tokio::test]
async fn a_struck_task_is_not_added() {
    let docket = docket().await;
    docket
        .strike(Strike::task::<Charge>().field("customer").eq(7))
        .await
        .unwrap();
    let struck = docket.add(Charge { customer: 7 }).await.unwrap();
    let allowed = docket.add(Charge { customer: 8 }).await.unwrap();
    assert_eq!(struck.disposition(), &Disposition::Struck);
    assert_eq!(allowed.disposition(), &Disposition::Scheduled);
}

#[tokio::test]
async fn a_strike_stops_a_task_that_was_already_added() {
    let docket = docket().await;
    docket.register(|_ctx, _: Noop| async { Ok::<_, std::io::Error>(()) });
    let execution = docket.add(Noop).await.unwrap();
    docket.strike(Strike::task::<Noop>()).await.unwrap();

    within(10, worker(&docket).run_until_finished())
        .await
        .unwrap();

    assert_eq!(
        execution.status().await.unwrap().unwrap().state,
        State::Cancelled
    );
}

#[tokio::test]
async fn restore_lifts_a_strike() {
    let docket = docket().await;
    docket.strike(Strike::task::<Noop>()).await.unwrap();
    docket.restore(Strike::task::<Noop>()).await.unwrap();
    let execution = docket.add(Noop).await.unwrap();
    assert_eq!(execution.disposition(), &Disposition::Scheduled);
}

#[tokio::test]
async fn another_connection_to_the_docket_loads_its_strikes() {
    let first = docket().await;
    first
        .strike(Strike::any_task().field("customer").ge(100))
        .await
        .unwrap();

    let second = docket::Docket::connect(first.name(), crate::support::shared_url(&first))
        .await
        .unwrap();
    within(10, second.strikes_loaded()).await;

    let struck = second.add(Charge { customer: 100 }).await.unwrap();
    assert_eq!(struck.disposition(), &Disposition::Struck);
}

#[tokio::test]
async fn a_strike_reaches_a_docket_that_is_already_connected() {
    let first = docket().await;
    let second = docket::Docket::connect(first.name(), crate::support::shared_url(&first))
        .await
        .unwrap();
    within(10, second.strikes_loaded()).await;

    first.strike(Strike::task::<Charge>()).await.unwrap();

    within(10, async {
        loop {
            if second
                .add(Charge { customer: 1 })
                .after(Duration::from_secs(60))
                .await
                .unwrap()
                .disposition()
                == &Disposition::Struck
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await;
}

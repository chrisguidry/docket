use std::time::Duration;

use redis::AsyncCommands;
use rstest::rstest;

use super::{cancel_unless_parked, hold};
use crate::behaviors::tests::{Attach, Logs, Noop, admit, context, memory, unreachable};
use crate::behaviors::{Behavior, ConcurrencyLimit, Released};
use crate::docket::Docket;

const ONE_AT_A_TIME: Attach = |hooks| ConcurrencyLimit::new(1).attach(hooks);

async fn scheduled_keys(docket: &Docket) -> Vec<String> {
    let snapshot = docket.snapshot().await.unwrap();
    snapshot.future.into_iter().map(|task| task.key).collect()
}

#[tokio::test]
async fn a_release_wakes_the_task_parked_behind_it() {
    let docket = memory().await;
    let holder = admit(ONE_AT_A_TIME, context(&docket, "a")).await;
    let parked = admit(ONE_AT_A_TIME, context(&docket, "b"))
        .await
        .err()
        .unwrap();
    assert_eq!(parked.reason(), "the concurrency limit is reached");
    assert!(parked.handled);
    assert_eq!(scheduled_keys(&docket).await, ["__safeguard__:b"]);

    let release = holder.ok().unwrap().release.unwrap();
    release(Released::Ran).await;

    assert_eq!(scheduled_keys(&docket).await, ["b"]);
}

/// The score that marks when the noop task's slot was last renewed.
async fn renewed_at(docket: &Docket) -> f64 {
    let mut connection = docket.connection().await.unwrap();
    let slots = docket.keys().concurrency(None, "noop");
    connection.zscore(slots, "a").await.unwrap()
}

#[tokio::test]
async fn a_held_slot_is_renewed() {
    let docket = memory().await;
    let _holder = admit(ONE_AT_A_TIME, context(&docket, "a")).await;
    let acquired = renewed_at(&docket).await;

    let renewal = async {
        while renewed_at(&docket).await <= acquired {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };

    tokio::time::timeout(Duration::from_secs(10), renewal)
        .await
        .expect("the slot is renewed in time");
}

#[tokio::test]
async fn a_slot_without_redis_warns_on_renewal_and_release() {
    let (logs, _guard) = Logs::capture();
    let docket = unreachable().await;
    let timeout = Duration::from_millis(40);
    let admitted = hold(
        docket,
        "slots".into(),
        "waiters".into(),
        "a".into(),
        1,
        timeout,
        1,
    );

    logs.wait_for("Concurrency lease renewal failed for").await;
    let release = admitted.release.unwrap();
    release(Released::Ran).await;

    assert!(logs.contains("releasing a concurrency slot failed"));
}

#[rstest]
#[case::still_parked(Some("scheduled"), vec!["__safeguard__:a".to_owned()])]
#[case::already_woken(Some("queued"), vec![])]
#[case::already_gone(None, vec![])]
#[tokio::test]
async fn a_safeguard_stays_only_while_its_task_is_parked(
    #[case] state: Option<&str>,
    #[case] expected: Vec<String>,
) {
    let docket = memory().await;
    docket
        .add(Noop)
        .key("__safeguard__:a")
        .after(Duration::from_secs(60))
        .await
        .unwrap();

    cancel_unless_parked(&docket, "__safeguard__:a", state)
        .await
        .unwrap();

    assert_eq!(scheduled_keys(&docket).await, expected);
}

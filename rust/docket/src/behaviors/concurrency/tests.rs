use std::collections::HashMap;
use std::time::Duration;

use redis::AsyncCommands;
use rstest::rstest;

use super::{cancel_unless_parked, hold, parked_depth};
use crate::behaviors::tests::{Attach, Logs, Noop, admit, blocked, context, memory, unreachable};
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
    let parked = blocked(admit(ONE_AT_A_TIME, context(&docket, "b")).await);
    assert_eq!(parked.reason(), "the concurrency limit is reached");
    assert!(parked.handled);
    assert_eq!(scheduled_keys(&docket).await, ["__safeguard__:b"]);

    let release = holder.ok().unwrap().release.unwrap();
    release(Released::Ran).await;

    assert_eq!(scheduled_keys(&docket).await, ["b"]);
}

/// The count of parked tasks, as a heartbeat reads it.
async fn depth(docket: &Docket) -> u64 {
    let mut connection = docket.connection().await.unwrap();
    parked_depth(docket, &mut connection).await.unwrap()
}

/// The count that the docket's index holds for each waiter stream.
async fn indexed(docket: &Docket) -> HashMap<String, u64> {
    let mut connection = docket.connection().await.unwrap();
    connection
        .hgetall(docket.keys().waiter_index())
        .await
        .unwrap()
}

/// Runs the safeguard's script on the `mixed` limit, as a worker on this
/// release does, and returns how many waiters it woke.
async fn scavenge(docket: &Docket) -> i64 {
    let keys = docket.keys();
    let slots = keys.concurrency(None, "mixed");
    let mut connection = docket.connection().await.unwrap();
    crate::scripts::ScavengeAndWake {
        waiters_stream: format!("{slots}:waiters"),
        slots_key: slots,
        stream_key: keys.stream(),
        queue_key: keys.queue(),
        waiter_index: keys.waiter_index(),
        parked_total: keys.parked_total(),
        max_concurrent: 1,
        stale_threshold: crate::wire::seconds(docket.now()) - 60.0,
        runs_prefix: keys.runs_prefix(),
        state_prefix: keys.state_prefix(),
        parked_prefix: keys.parked_prefix(),
    }
    .call()
    .run(&mut connection)
    .await
    .unwrap()
}

#[tokio::test]
async fn the_parked_depth_counts_the_tasks_behind_a_limit() {
    let docket = memory().await;
    assert_eq!(depth(&docket).await, 0);

    let holder = admit(ONE_AT_A_TIME, context(&docket, "a")).await;
    assert_eq!(depth(&docket).await, 0);
    blocked(admit(ONE_AT_A_TIME, context(&docket, "b")).await);
    blocked(admit(ONE_AT_A_TIME, context(&docket, "c")).await);
    assert_eq!(depth(&docket).await, 2);
    let waiters = format!("{}:waiters", docket.keys().concurrency(None, "noop"));
    assert_eq!(indexed(&docket).await, HashMap::from([(waiters, 2)]));

    // The release wakes one waiter for the one slot it frees.
    let release = holder.ok().unwrap().release.unwrap();
    release(Released::Ran).await;
    assert_eq!(depth(&docket).await, 1);

    // The woken task takes the slot, and its release wakes the last waiter.
    let woken = admit(ONE_AT_A_TIME, context(&docket, "b")).await;
    let release = woken.ok().unwrap().release.unwrap();
    release(Released::Ran).await;
    assert_eq!(depth(&docket).await, 0);
    assert_eq!(indexed(&docket).await, HashMap::new());
}

#[tokio::test]
async fn the_parked_depth_fails_without_redis() {
    let docket = unreachable().await;
    let mut connection = docket.handle();

    assert!(parked_depth(&docket, &mut connection).await.is_err());
}

/// A worker on a release that keeps no count wakes the tasks on a stream
/// and deletes it, and the count for the stream stays as it was.  The next
/// script on the stream corrects the count and the total.
#[tokio::test]
async fn a_script_corrects_the_count_that_another_release_left() {
    let docket = memory().await;
    let keys = docket.keys();
    let waiters = format!("{}:waiters", keys.concurrency(None, "mixed"));
    let mut connection = docket.connection().await.unwrap();
    let () = connection
        .hset(keys.waiter_index(), &waiters, 3)
        .await
        .unwrap();
    let () = connection.set(keys.parked_total(), 3).await.unwrap();
    assert_eq!(depth(&docket).await, 3);

    assert_eq!(scavenge(&docket).await, 0);

    assert_eq!(depth(&docket).await, 0);
    assert_eq!(indexed(&docket).await, HashMap::new());
}

/// A worker on a release that keeps no count parks a task without counting
/// it.  The next script on the stream counts it.
#[tokio::test]
async fn a_script_counts_the_task_that_another_release_parked() {
    let docket = memory().await;
    let slots = docket.keys().concurrency(None, "mixed");
    let mut connection = docket.connection().await.unwrap();
    let now = crate::wire::seconds(docket.now());
    let () = connection.zadd(&slots, "holder", now).await.unwrap();
    let _: String = connection
        .xadd(
            format!("{slots}:waiters"),
            "*",
            &[("key", "parked-by-another-release")],
        )
        .await
        .unwrap();
    assert_eq!(depth(&docket).await, 0);

    assert_eq!(scavenge(&docket).await, 0);

    assert_eq!(depth(&docket).await, 1);
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

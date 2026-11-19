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

/// The waiter streams that the docket's index names.
async fn indexed(docket: &Docket) -> Vec<String> {
    let mut connection = docket.connection().await.unwrap();
    connection
        .smembers(docket.keys().waiter_index())
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
    assert_eq!(indexed(&docket).await, [waiters]);

    // The release wakes one waiter for the one slot it frees.
    let release = holder.ok().unwrap().release.unwrap();
    release(Released::Ran).await;
    assert_eq!(depth(&docket).await, 1);

    // The woken task takes the slot, and its release wakes the last waiter.
    let woken = admit(ONE_AT_A_TIME, context(&docket, "b")).await;
    let release = woken.ok().unwrap().release.unwrap();
    release(Released::Ran).await;
    assert_eq!(depth(&docket).await, 0);
    assert_eq!(indexed(&docket).await, Vec::<String>::new());
}

/// A worker on a release without the index deletes a drained waiter stream
/// and leaves its name in the index.
#[tokio::test]
async fn the_parked_depth_forgets_a_stream_that_another_release_deleted() {
    let docket = memory().await;
    let mut connection = docket.connection().await.unwrap();
    let () = connection
        .sadd(
            docket.keys().waiter_index(),
            "memory:concurrency:gone:waiters",
        )
        .await
        .unwrap();

    assert_eq!(depth(&docket).await, 0);
    assert_eq!(indexed(&docket).await, Vec::<String>::new());
}

#[tokio::test]
async fn the_parked_depth_fails_without_redis() {
    let docket = unreachable().await;
    let mut connection = docket.handle();

    assert!(parked_depth(&docket, &mut connection).await.is_err());
}

#[tokio::test]
async fn the_parked_depth_fails_when_redis_refuses_a_streams_length() {
    let docket = memory().await;
    let mut connection = docket.connection().await.unwrap();
    let () = connection.set("not-a-stream", "text").await.unwrap();
    let () = connection
        .sadd(docket.keys().waiter_index(), "not-a-stream")
        .await
        .unwrap();

    assert!(parked_depth(&docket, &mut connection).await.is_err());
}

/// A worker on a release without the index parks a task without adding its
/// stream to the index.  A wake script that finds a task on the stream adds
/// the stream, so the task is counted.
#[tokio::test]
async fn a_wake_script_indexes_a_stream_that_another_release_parked_on() {
    let docket = memory().await;
    let keys = docket.keys();
    let slots = keys.concurrency(None, "mixed");
    let waiters = format!("{slots}:waiters");
    let mut connection = docket.connection().await.unwrap();
    let now = crate::wire::seconds(docket.now());
    let () = connection.zadd(&slots, "holder", now).await.unwrap();
    let _: String = connection
        .xadd(&waiters, "*", &[("key", "parked-by-another-release")])
        .await
        .unwrap();
    assert_eq!(depth(&docket).await, 0);

    let woken: i64 = crate::scripts::ScavengeAndWake {
        slots_key: slots,
        waiters_stream: waiters,
        stream_key: keys.stream(),
        queue_key: keys.queue(),
        waiter_index: keys.waiter_index(),
        max_concurrent: 1,
        stale_threshold: now - 60.0,
        runs_prefix: keys.runs_prefix(),
        state_prefix: keys.state_prefix(),
        parked_prefix: keys.parked_prefix(),
    }
    .call()
    .run(&mut connection)
    .await
    .unwrap();

    assert_eq!(woken, 0);
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

//! Stopping a running task when someone cancels it.

use std::sync::Arc;
use std::time::Duration;

use futures::StreamExt;
use tokio::sync::oneshot;
use tokio::task::JoinSet;

use super::session::Shared;
use crate::error::Result;

/// How long the listener waits to subscribe again after Redis drops it.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Starts the listener and waits until its subscription is confirmed, so
/// that no cancel sent after the worker starts can be missed.
pub(super) async fn start(worker: &Arc<Shared>, infrastructure: &mut JoinSet<()>) -> Result<()> {
    let (subscribed, confirmed) = oneshot::channel();
    infrastructure.spawn(listen(Arc::clone(worker), Some(subscribed)));
    // The listener's task holds the sender until it has subscribed or failed
    // its first attempt; either way the worker can go on.
    let _ = confirmed.await;
    Ok(())
}

async fn listen(worker: Arc<Shared>, mut subscribed: Option<oneshot::Sender<()>>) {
    let docket = &worker.docket;
    loop {
        let pubsub = async {
            let mut pubsub = docket.backend().pubsub().await?;
            pubsub.psubscribe(docket.keys().cancel_pattern()).await?;
            redis::RedisResult::Ok(pubsub)
        }
        .await;
        if let Some(subscribed) = subscribed.take() {
            let _ = subscribed.send(());
        }
        let Ok(pubsub) = pubsub else {
            tokio::time::sleep(RETRY_DELAY).await;
            continue;
        };
        let mut messages = pubsub.into_on_message();
        while let Some(message) = messages.next().await {
            if let Ok(key) = message.get_payload::<String>() {
                worker.cancel_key(&key);
                if let Err(error) = forget_waiter(docket, &key).await {
                    tracing::warn!(%error, %key, "cleaning up a cancelled waiter failed");
                }
            }
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// Removes a cancelled task from the concurrency waiter stream it is parked
/// on, and cancels the safeguard that would have woken it.  The wake scripts
/// skip cancelled waiters anyway, so this only tidies up.
async fn forget_waiter(docket: &crate::Docket, key: &str) -> Result<()> {
    let keys = docket.keys();
    let mut connection = docket.connection().await?;
    let runs: std::collections::HashMap<String, String> =
        redis::AsyncCommands::hgetall(&mut connection, keys.runs(key)).await?;
    let (Some(stream), Some(entry)) = (runs.get("waiter_stream"), runs.get("waiter_entry_id"))
    else {
        return Ok(());
    };
    let call = crate::scripts::CancelCleanup {
        waiters_stream: stream.clone(),
        progress_key: keys.progress(key),
        runs_key: keys.runs(key),
        waiter_entry_id: entry.clone(),
    }
    .call();
    call.run::<redis::Value, _>(&mut connection).await?;
    docket
        .cancel(&format!("{}{key}", crate::behaviors::SAFEGUARD_PREFIX))
        .await
}

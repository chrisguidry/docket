use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, Value};
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::{Condition, Operator, SharedStrikes, Strike};
use crate::connection::Backend;

/// How long one read of the strike stream waits for a new instruction.
const BLOCK: Duration = Duration::from_secs(60);

/// How long the monitor waits before it tries Redis again.
const RETRY_DELAY: Duration = Duration::from_secs(1);

/// Follows a docket's strike stream and keeps its [`Strikes`](super::Strikes)
/// current.  It stops when it is dropped.
pub(crate) struct Monitor {
    task: JoinHandle<()>,
    loaded: watch::Receiver<bool>,
}

impl Monitor {
    pub fn start(backend: Arc<Backend>, stream: String, strikes: SharedStrikes) -> Self {
        let (loaded_sender, loaded) = watch::channel(false);
        let task = tokio::spawn(follow(backend, stream, strikes, loaded_sender));
        Self { task, loaded }
    }

    /// Waits until the monitor has read every strike that existed when it
    /// started.
    pub async fn loaded(&self) {
        let mut loaded = self.loaded.clone();
        // The sender lives as long as the monitor's task, which only ends
        // when the monitor is dropped, so the wait cannot fail first.
        let _ = loaded.wait_for(|loaded| *loaded).await;
    }
}

impl Drop for Monitor {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn follow(
    backend: Arc<Backend>,
    stream: String,
    strikes: SharedStrikes,
    loaded: watch::Sender<bool>,
) {
    let mut last_id = "0-0".to_owned();
    loop {
        let Ok(mut connection) = backend.connect().await else {
            tokio::time::sleep(RETRY_DELAY).await;
            continue;
        };
        loop {
            let mut options = StreamReadOptions::default().count(100);
            if *loaded.borrow() {
                options = options.block(BLOCK.as_millis().try_into().unwrap_or(usize::MAX));
            }
            let reply: Option<StreamReadReply> = match connection
                .xread_options(&[stream.as_str()], &[last_id.as_str()], &options)
                .await
            {
                Ok(reply) => reply,
                Err(_) => break,
            };
            let entries = reply
                .into_iter()
                .flat_map(|reply| reply.keys)
                .flat_map(|key| key.ids)
                .collect::<Vec<_>>();
            match entries.last() {
                Some(last) => last_id.clone_from(&last.id),
                None => {
                    loaded.send_replace(true);
                }
            }
            // An entry that is not a strike instruction changes nothing.
            for (strike, restore) in entries.iter().filter_map(|entry| decode(&entry.map)) {
                strikes.apply(&strike, restore);
            }
        }
        tokio::time::sleep(RETRY_DELAY).await;
    }
}

/// The fields of a strike instruction, the same ones pydocket writes.  The
/// value is JSON, where pydocket writes a pickle.
pub(crate) fn encode(strike: &Strike, restore: bool) -> Vec<(&'static str, String)> {
    let mut fields = vec![(
        "direction",
        if restore { "restore" } else { "strike" }.to_owned(),
    )];
    if let Some(function) = &strike.function {
        fields.push(("function", function.clone()));
    }
    if let Some(condition) = &strike.condition {
        fields.push(("parameter", condition.field.clone()));
        fields.push(("operator", condition.operator.as_str().to_owned()));
        fields.push(("value", condition.value.to_string()));
    } else {
        fields.push(("operator", Operator::Equal.as_str().to_owned()));
        fields.push(("value", "null".to_owned()));
    }
    fields
}

pub(super) fn decode(fields: &HashMap<String, Value>) -> Option<(Strike, bool)> {
    let text = |name: &str| -> Option<String> {
        match fields.get(name)? {
            Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
            _ => None,
        }
    };
    let restore = text("direction")? == "restore";
    let condition = if let Some(field) = text("parameter") {
        Some(Condition {
            field,
            operator: Operator::parse(&text("operator")?)?,
            value: serde_json::from_str(&text("value")?).ok()?,
        })
    } else {
        None
    };
    Some((
        Strike {
            function: text("function"),
            condition,
        },
        restore,
    ))
}

//! A task's progress: how far it has come, of how far, and a message.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};

use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde_json::{Map, Value};

use crate::connection::Handle;
use crate::docket::Docket;
use crate::error::Result;
use crate::scripts;
use crate::wire::{iso, parse_iso};

/// The total a task's progress has until it sets its own.
const DEFAULT_TOTAL: i64 = 100;

/// Reports a running task's progress.  Every change is stored and published
/// to anyone following the task.
#[derive(Clone)]
pub struct Progress {
    docket: Docket,
    key: String,
    current: Arc<AtomicI64>,
}

impl Progress {
    pub(crate) fn new(docket: Docket, key: String) -> Self {
        Self {
            docket,
            key,
            current: Arc::new(AtomicI64::new(0)),
        }
    }

    /// Sets the total that progress counts toward.
    pub async fn set_total(&self, total: i64) -> Result<()> {
        let now = iso(self.docket.now());
        self.write(
            vec![("total", total.to_string()), ("updated_at", now.clone())],
            false,
            object([("total", total.into()), ("updated_at", now.into())]),
        )
        .await
    }

    /// Sets the message, or clears it with `None`.
    pub async fn set_message(&self, message: Option<&str>) -> Result<()> {
        let now = iso(self.docket.now());
        let mut fields = vec![("updated_at", now.clone())];
        if let Some(message) = message {
            fields.push(("message", message.to_owned()));
        }
        self.write(
            fields,
            message.is_none(),
            object([("message", message.into()), ("updated_at", now.into())]),
        )
        .await
    }

    /// Adds `amount` to the current progress.
    pub async fn increment(&self, amount: i64) -> Result<()> {
        let keys = self.docket.keys();
        let now = iso(self.docket.now());
        let mut connection = self.docket.handle();
        let (current, hash): (i64, HashMap<String, String>) = redis::pipe()
            .hincr(keys.progress(&self.key), "current", amount)
            .hset(keys.progress(&self.key), "updated_at", &now)
            .ignore()
            .hgetall(keys.progress(&self.key))
            .query_async(&mut connection)
            .await?;
        self.current.store(current, Ordering::Relaxed);
        let snapshot = snapshot(&hash);
        let payload = self.payload(object([
            ("current", current.into()),
            ("total", snapshot.total.into()),
            ("message", snapshot.message.into()),
            ("updated_at", now.into()),
        ]));
        let _: i64 = connection
            .publish(keys.progress(&self.key), payload.to_string())
            .await?;
        Ok(())
    }

    async fn write(
        &self,
        fields: Vec<(&str, String)>,
        clear_message: bool,
        changes: Map<String, Value>,
    ) -> Result<()> {
        let mut connection = self.docket.handle();
        let snapshot = read(&mut connection, &self.docket, &self.key).await?;
        let mut payload = object([
            ("current", self.current.load(Ordering::Relaxed).into()),
            ("total", snapshot.total.into()),
            ("message", snapshot.message.into()),
            ("updated_at", snapshot.updated_at.map(iso).into()),
        ]);
        payload.extend(changes);
        let call = scripts::ProgressWrite {
            progress_key: self.docket.keys().progress(&self.key),
            payload: self.payload(payload).to_string(),
            clear_message,
            fields: fields
                .into_iter()
                .map(|(field, value)| (field.to_owned(), value.into_bytes()))
                .collect(),
        }
        .call();
        call.run::<redis::Value, _>(&mut connection).await?;
        Ok(())
    }

    fn payload(&self, mut fields: Map<String, Value>) -> Value {
        fields.insert("type".to_owned(), "progress".into());
        fields.insert("key".to_owned(), self.key.clone().into());
        Value::Object(fields)
    }
}

fn object<const N: usize>(fields: [(&str, Value); N]) -> Map<String, Value> {
    fields
        .into_iter()
        .map(|(field, value)| (field.to_owned(), value))
        .collect()
}

/// A run's progress at one moment.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct ProgressSnapshot {
    /// How far the task has come, or `None` before it starts.
    pub current: Option<i64>,
    /// Where it is going.
    pub total: i64,
    /// What it says it is doing.
    pub message: Option<String>,
    /// When it last reported.
    pub updated_at: Option<DateTime<Utc>>,
}

pub(crate) async fn read(
    connection: &mut Handle,
    docket: &Docket,
    key: &str,
) -> Result<ProgressSnapshot> {
    let hash: HashMap<String, String> = connection.hgetall(docket.keys().progress(key)).await?;
    Ok(snapshot(&hash))
}

/// The progress in a run's progress hash.
pub(crate) fn snapshot(hash: &HashMap<String, String>) -> ProgressSnapshot {
    if hash.is_empty() {
        return ProgressSnapshot {
            current: None,
            total: DEFAULT_TOTAL,
            message: None,
            updated_at: None,
        };
    }
    let number = |field: &str, default: i64| {
        hash.get(field)
            .and_then(|n| n.parse().ok())
            .unwrap_or(default)
    };
    ProgressSnapshot {
        current: Some(number("current", 0)),
        total: number("total", DEFAULT_TOTAL),
        message: hash.get("message").cloned(),
        updated_at: hash.get("updated_at").and_then(|text| parse_iso(text)),
    }
}

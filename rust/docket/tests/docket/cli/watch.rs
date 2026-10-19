//! `watch`, which follows one run until it ends.

use std::io::Write;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use docket::{Docket, Error};
use redis::AsyncCommands;
use tokio::sync::Notify;

use super::command;
use crate::snapshots::raw;
use crate::support::{Noop, docket, docket_through, proxy, within, worker};

/// Output that a test reads while the command still writes it.
#[derive(Clone, Default)]
struct Shared(Arc<Mutex<Vec<u8>>>);

impl Shared {
    fn text(&self) -> String {
        String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
    }

    async fn wait_for(&self, text: &str) {
        within(10, async {
            while !self.text().contains(text) {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await;
    }
}

impl Write for Shared {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().write(bytes)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

fn spawn_watch(
    docket: &Docket,
    key: &str,
) -> (Shared, tokio::task::JoinHandle<docket::Result<()>>) {
    let out = Shared::default();
    let (docket, watch, mut writer) = (docket.clone(), command(&["watch", key]), out.clone());
    let handle = tokio::spawn(async move { watch.run_with(&docket, &mut writer).await });
    (out, handle)
}

/// Registers a `noop` that reports progress and waits for `release`.
fn register_progress(docket: &Docket) -> Arc<Notify> {
    let release = Arc::new(Notify::new());
    let on_release = Arc::clone(&release);
    docket.register(move |ctx: docket::Context, _: Noop| {
        let on_release = Arc::clone(&on_release);
        async move {
            let progress = ctx.progress();
            progress.set_total(10).await?;
            progress.increment(3).await?;
            on_release.notified().await;
            Ok::<_, docket::Error>(())
        }
    });
    release
}

#[tokio::test]
async fn watch_follows_a_run_until_it_ends() {
    let docket = docket().await;
    let release = register_progress(&docket);
    docket.add(Noop).key("watched").await.unwrap();
    let (out, watching) = spawn_watch(&docket, "watched");
    out.wait_for("State: QUEUED").await;

    let run_worker = tokio::spawn(worker(&docket).name("w-watch").run_until_finished());
    out.wait_for("Progress: 3/10 (30%) Processing...").await;
    release.notify_one();
    within(10, watching).await.unwrap().unwrap();
    within(10, run_worker).await.unwrap().unwrap();

    let output = out.text();
    assert!(
        output.starts_with(&format!(
            "Task: watched\nDocket: {}\nState: QUEUED\n",
            docket.name()
        )),
        "{output}"
    );
    assert!(
        output.contains("State: RUNNING\nWorker: w-watch\n"),
        "{output}"
    );
    assert!(
        output.ends_with("State: COMPLETED\n✓ Task completed successfully\n"),
        "{output}"
    );
}

#[tokio::test]
async fn watch_subscribes_again_when_its_subscription_drops() {
    let Some(proxy) = proxy().await else { return };
    let docket = docket_through(&proxy).await;
    let release = register_progress(&docket);
    docket.add(Noop).key("watched").await.unwrap();
    let (out, watching) = spawn_watch(&docket, "watched");
    out.wait_for("State: QUEUED").await;

    let subscribes = proxy.count("SUBSCRIBE");
    proxy.drop_subscribers();
    within(10, async {
        while proxy.count("SUBSCRIBE") == subscribes {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await;
    let run_worker = tokio::spawn(worker(&docket).run_until_finished());
    out.wait_for("Progress: 3/10").await;
    release.notify_one();
    within(10, watching).await.unwrap().unwrap();
    within(10, run_worker).await.unwrap().unwrap();
    assert_eq!(
        out.text().matches("State: QUEUED").count(),
        1,
        "{}",
        out.text()
    );
}

#[tokio::test]
async fn watch_fails_when_it_cannot_subscribe() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    docket.add(Noop).key("watched").await.unwrap();
    // Subscribing reads the progress hash, which is now a string.
    let _: () = raw
        .set(format!("{}:progress:watched", docket.name()), "not a hash")
        .await
        .unwrap();
    let error = command(&["watch", "watched"])
        .run_with(&docket, &mut Vec::new())
        .await
        .unwrap_err();
    assert!(error.to_string().contains("WRONGTYPE"), "{error}");
}

#[tokio::test]
async fn watch_fails_on_an_event_it_cannot_read() {
    let docket = docket().await;
    let Some(mut raw) = raw().await else { return };
    docket.add(Noop).key("watched").await.unwrap();
    let (out, watching) = spawn_watch(&docket, "watched");
    out.wait_for("State: QUEUED").await;
    let _: i64 = raw
        .publish(format!("{}:state:watched", docket.name()), "not json")
        .await
        .unwrap();
    let error = within(10, watching).await.unwrap().unwrap_err();
    assert!(matches!(error, Error::Json(_)), "{error}");
}

#[tokio::test]
async fn watch_fails_for_a_key_the_docket_does_not_know() {
    let docket = docket().await;
    let error = command(&["watch", "nobody"])
        .run_with(&docket, &mut Vec::new())
        .await
        .unwrap_err();
    assert_eq!(error.to_string(), "Task with key 'nobody' not found");
}

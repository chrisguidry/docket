use std::sync::{Arc, Mutex};

use crate::support::{Noop, docket, within, worker};
use docket::Timestamp;

/// The worker's name and the due time a handler saw.
type Seen = (String, Timestamp);

#[tokio::test]
async fn a_handler_knows_its_worker_and_when_it_was_due() {
    let docket = docket().await;
    let seen: Arc<Mutex<Option<Seen>>> = Arc::default();
    let recorded = Arc::clone(&seen);
    docket.register(move |ctx: docket::Context, _: Noop| {
        *recorded.lock().unwrap() = Some((ctx.worker().to_owned(), ctx.when()));
        async { Ok::<_, std::io::Error>(()) }
    });
    let execution = docket.add(Noop).await.unwrap();

    within(10, worker(&docket).name("the-worker").run_until_finished())
        .await
        .unwrap();

    // Messages carry times to the microsecond.
    let expected = Some((
        "the-worker".to_owned(),
        Timestamp::from_unix_micros(execution.when().as_unix_micros()).unwrap(),
    ));
    assert_eq!(*seen.lock().unwrap(), expected);
}

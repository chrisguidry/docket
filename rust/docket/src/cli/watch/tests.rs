use crate::execution::{Event, ProgressEvent, State, StateEvent};

use super::Display;

fn state(state: State, worker: Option<&str>, error: Option<&str>) -> Event {
    Event::State(StateEvent {
        state,
        when: None,
        worker: worker.map(str::to_owned),
        started_at: None,
        completed_at: None,
        error: error.map(str::to_owned),
    })
}

fn progress(current: Option<i64>, total: i64, message: Option<&str>) -> Event {
    Event::Progress(ProgressEvent {
        current,
        total,
        message: message.map(str::to_owned),
        updated_at: None,
    })
}

fn lines(display: &mut Display, events: &[Event]) -> (String, bool) {
    let ended = events.iter().fold(false, |_, event| display.update(event));
    (std::mem::take(&mut display.pending), ended)
}

#[test]
fn a_run_prints_each_change_once() {
    let mut display = Display::new("k", "orders");
    let (text, ended) = lines(
        &mut display,
        &[
            state(State::Queued, None, None),
            progress(None, 100, None),
            state(State::Queued, None, None),
            state(State::Running, Some("w1"), None),
            progress(Some(0), 0, None),
            progress(Some(3), 10, None),
            progress(Some(3), 10, None),
            progress(Some(5), 10, Some("halfway")),
            state(State::Running, Some("w1"), None),
        ],
    );
    assert_eq!(
        text,
        "Task: k\nDocket: orders\nState: QUEUED\nState: RUNNING\nWorker: w1\n\
         Progress: 3/10 (30%) Processing...\nProgress: 5/10 (50%) halfway\n"
    );
    assert!(!ended);

    let (text, ended) = lines(&mut display, &[state(State::Completed, Some("w1"), None)]);
    assert_eq!(text, "State: COMPLETED\n✓ Task completed successfully\n");
    assert!(ended);
}

#[test]
fn a_failed_run_prints_its_error() {
    let mut display = Display::new("k", "d");
    display.pending.clear();
    let (text, ended) = lines(
        &mut display,
        &[state(State::Failed, Some("w"), Some("boom"))],
    );
    assert_eq!(
        text,
        "State: FAILED\nWorker: w\nError: boom\n✗ Task failed\n"
    );
    assert!(ended);
}

#[test]
fn a_cancelled_run_ends_the_watch() {
    let mut display = Display::new("k", "d");
    display.pending.clear();
    let (text, ended) = lines(&mut display, &[state(State::Cancelled, None, None)]);
    assert_eq!(text, "State: CANCELLED\n✗ Task cancelled\n");
    assert!(ended);
}

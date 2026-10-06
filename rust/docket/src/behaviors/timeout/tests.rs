use std::sync::Mutex;
use std::time::Duration;

use super::TimeoutControl;

#[test]
fn extending_before_the_run_starts_keeps_the_full_time() {
    let control = TimeoutControl {
        base: Duration::from_secs(5),
        deadline: Mutex::new(None),
    };
    control.extend(Duration::from_secs(60));
    assert_eq!(control.remaining(), Duration::from_secs(5));
}

#[test]
fn extending_a_started_run_moves_its_deadline() {
    let control = TimeoutControl {
        base: Duration::from_secs(5),
        deadline: Mutex::new(None),
    };
    control.start();
    control.extend(Duration::from_secs(60));
    assert!(control.remaining() > Duration::from_secs(60));
}

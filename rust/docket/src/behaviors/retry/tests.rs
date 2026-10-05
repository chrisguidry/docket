use std::time::Duration;

use rstest::rstest;

use super::{ExponentialRetry, ForcedRetry, decide};
use crate::behaviors::{AfterFailure, BoxError};

fn plain_error() -> BoxError {
    "boom".into()
}

#[rstest]
#[case::first(1, Duration::from_millis(500))]
#[case::second(2, Duration::from_secs(1))]
#[case::third(3, Duration::from_secs(2))]
#[case::capped(10, Duration::from_secs(5))]
#[case::huge(100, Duration::from_secs(5))]
fn exponential_delays_double_up_to_the_maximum(#[case] attempt: u32, #[case] expected: Duration) {
    let retry = ExponentialRetry::attempts(100)
        .minimum_delay(Duration::from_millis(500))
        .maximum_delay(Duration::from_secs(5));
    assert_eq!(retry.delay_after(attempt), expected);
}

#[test]
fn stops_at_the_last_attempt() {
    assert_eq!(
        decide(Some(3), 3, &plain_error(), Duration::ZERO),
        AfterFailure::Fail
    );
}

#[test]
fn retries_before_the_last_attempt() {
    assert!(matches!(
        decide(Some(3), 2, &plain_error(), Duration::ZERO),
        AfterFailure::RetryAt(_)
    ));
}

#[test]
fn retries_forever_without_a_limit() {
    assert!(matches!(
        decide(None, u32::MAX, &plain_error(), Duration::ZERO),
        AfterFailure::RetryAt(_)
    ));
}

#[test]
fn a_forced_retry_chooses_the_delay() {
    let error: BoxError = Box::new(ForcedRetry::after(Duration::from_secs(3600)));
    let AfterFailure::RetryAt(when) = decide(None, 1, &error, Duration::ZERO) else {
        panic!("a forced retry retries");
    };
    assert!(when > chrono::Utc::now() + chrono::Duration::minutes(59));
}

#[test]
fn a_forced_retry_in_the_past_runs_now() {
    let forced = ForcedRetry::at(chrono::Utc::now() - chrono::Duration::hours(1));
    assert_eq!(forced, ForcedRetry::after(Duration::ZERO));
}

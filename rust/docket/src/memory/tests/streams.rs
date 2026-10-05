use rstest::rstest;

use super::{connect, last_reply, run};

/// Three entries with IDs 1-1, 1-2, and 2-0.
const THREE: [&str; 3] = ["XADD s 1-1 f a", "XADD s 1-2 f b", "XADD s 2-0 f c"];

fn after_three(line: &str) -> Vec<&str> {
    THREE.iter().copied().chain([line]).collect()
}

#[rstest]
#[case::xadd_explicit_id(&["XADD s 5-1 f v"], "\"5-1\"")]
#[case::xadd_bare_ms(&["XADD s 5 f v"], "\"5-0\"")]
#[case::xadd_zero_id(&["XADD s 0-0 f v"], "-ERR The ID specified in XADD must be greater than 0-0")]
#[case::xadd_smaller_id(&["XADD s 5-1 f v", "XADD s 5-1 f v"], "-ERR The ID specified in XADD is equal or smaller than the target stream top item")]
#[case::xadd_bad_id(&["XADD s soon f v"], "-ERR Invalid stream ID specified as stream command argument")]
#[case::xadd_odd_fields(&["XADD s * f"], "-ERR wrong number of arguments for 'xadd' command")]
#[case::xadd_no_id(&["XADD s"], "-ERR wrong number of arguments for 'xadd' command")]
#[case::xadd_wrong_type(&["SET s v", "XADD s * f v"], "-WRONGTYPE Operation against a key holding the wrong kind of value")]
#[case::xadd_maxlen(&["XADD s MAXLEN 1 1-1 f a", "XADD s MAXLEN = 1 1-2 f b", "XLEN s"], "1")]
#[case::xadd_approximate_maxlen(&["XADD s 1-1 f a", "XADD s MAXLEN ~ 1 1-2 f b", "XRANGE s - +"], "[[\"1-2\", [\"f\", \"b\"]]]")]
#[case::xadd_minid(&["XADD s 1-1 f a", "XADD s MINID 2 2-0 f b", "XLEN s"], "1")]
#[case::xadd_bad_maxlen(&["XADD s MAXLEN x * f v"], "-ERR value is not an integer or out of range")]
#[case::xadd_trim_wrong_type(&["SET s v", "XADD s MAXLEN 1 * f v"], "-WRONGTYPE Operation against a key holding the wrong kind of value")]
#[case::xlen_missing(&["XLEN s"], "0")]
#[case::xlen_extra(&["XLEN s t"], "-ERR syntax error")]
#[case::xrange_all(&after_three("XRANGE s - +"), "[[\"1-1\", [\"f\", \"a\"]], [\"1-2\", [\"f\", \"b\"]], [\"2-0\", [\"f\", \"c\"]]]")]
#[case::xrange_bare_ms_covers_millisecond(&after_three("XRANGE s 1 1"), "[[\"1-1\", [\"f\", \"a\"]], [\"1-2\", [\"f\", \"b\"]]]")]
#[case::xrange_exclusive(&after_three("XRANGE s (1-1 (2-0"), "[[\"1-2\", [\"f\", \"b\"]]]")]
#[case::xrange_exclusive_end(&after_three("XRANGE s - (1-2"), "[[\"1-1\", [\"f\", \"a\"]]]")]
#[case::xrange_exclusive_carries(&["XADD s 2-0 f c", "XRANGE s (1-18446744073709551615 +"], "[[\"2-0\", [\"f\", \"c\"]]]")]
#[case::xrange_exclusive_borrows(&["XADD s 1-5 f c", "XRANGE s - (2-0"], "[[\"1-5\", [\"f\", \"c\"]]]")]
#[case::xrange_count(&after_three("XRANGE s - + COUNT 1"), "[[\"1-1\", [\"f\", \"a\"]]]")]
#[case::xrange_backwards(&after_three("XRANGE s 2 1"), "[]")]
#[case::xrange_past_the_top(&["XRANGE s (+ +"], "-ERR invalid start ID for the interval")]
#[case::xrange_below_the_bottom(&["XRANGE s - (-"], "-ERR invalid end ID for the interval")]
#[case::xrange_bad_bound(&["XRANGE s x +"], "-ERR Invalid stream ID specified as stream command argument")]
#[case::xrange_unknown_option(&["XRANGE s - + LIMIT 1"], "-ERR syntax error")]
#[case::xrange_count_then_more(&["XRANGE s - + COUNT 1 x"], "-ERR syntax error")]
#[case::xrange_wrong_type(&["SET s v", "XRANGE s - +"], "-WRONGTYPE Operation against a key holding the wrong kind of value")]
#[case::xrevrange(&after_three("XREVRANGE s + - COUNT 2"), "[[\"2-0\", [\"f\", \"c\"]], [\"1-2\", [\"f\", \"b\"]]]")]
#[case::xdel(&after_three("XDEL s 1-1 9-9"), "1")]
#[case::xdel_bad_id(&["XDEL s x"], "-ERR Invalid stream ID specified as stream command argument")]
#[case::xtrim_maxlen(&after_three("XTRIM s MAXLEN 1"), "2")]
#[case::xtrim_minid(&after_three("XTRIM s MINID ~ 2"), "2")]
#[case::xtrim_unknown_strategy(&["XTRIM s LENGTH 1"], "-ERR syntax error")]
#[case::xtrim_extra(&["XTRIM s MAXLEN 1 LIMIT 5"], "-ERR syntax error")]
#[tokio::test]
async fn streams(#[case] lines: &[&str], #[case] expected: &str) {
    assert_eq!(last_reply(lines).await, expected);
}

#[tokio::test]
async fn xadd_generates_increasing_ids() {
    let (_server, mut connection) = connect().await;
    let first = run(&mut connection, "XADD s * f a").await;
    let second = run(&mut connection, "XADD s * f b").await;
    assert!(first < second, "{first} then {second}");
}

use rstest::rstest;

use super::{connect, last_reply, run};

/// A stream with entries 1-1 and 1-2, and group `g` that consumer `alice`
/// has read both from.
const DELIVERED: [&str; 4] = [
    "XADD s 1-1 f a",
    "XADD s 1-2 f b",
    "XGROUP CREATE s g 0",
    "XREADGROUP GROUP g alice STREAMS s >",
];

fn after_delivery<'a>(lines: &[&'a str]) -> Vec<&'a str> {
    DELIVERED.iter().chain(lines).copied().collect()
}

#[rstest]
#[case::create(&["XGROUP CREATE s g $ MKSTREAM"], "ok")]
#[case::create_without_stream(&["XGROUP CREATE s g $"], "-ERR The XGROUP subcommand requires the key to exist")]
#[case::create_twice(&["XGROUP CREATE s g 0 MKSTREAM", "XGROUP CREATE s g 0"], "-BUSYGROUP Consumer Group name already exists")]
#[case::create_entriesread(&["XGROUP CREATE s g 0 MKSTREAM ENTRIESREAD 0"], "ok")]
#[case::create_bad_id(&["XGROUP CREATE s g x MKSTREAM"], "-ERR Invalid stream ID specified as stream command argument")]
#[case::create_unknown_option(&["XGROUP CREATE s g 0 FAST"], "-ERR syntax error")]
#[case::create_from_last_id(&["XADD s 1-1 f a", "XGROUP CREATE s g $", "XREADGROUP GROUP g c STREAMS s >"], "nil")]
#[case::destroy(&["XGROUP CREATE s g 0 MKSTREAM", "XGROUP DESTROY s g"], "1")]
#[case::destroy_extra(&["XGROUP DESTROY s g h"], "-ERR syntax error")]
#[case::unknown_subcommand(&["XGROUP SETID s g 0"], "-ERR unknown subcommand 'SETID'. Try XGROUP HELP.")]
#[case::xack(&after_delivery(&["XACK s g 1-1 9-9"]), "1")]
#[case::xack_bad_id(&["XACK s g x"], "-ERR Invalid stream ID specified as stream command argument")]
#[case::pending_summary(&after_delivery(&["XPENDING s g"]), "[2, \"1-1\", \"1-2\", [[\"alice\", \"2\"]]]")]
#[case::pending_summary_empty(&["XGROUP CREATE s g 0 MKSTREAM", "XPENDING s g"], "[0, nil, nil, nil]")]
#[case::pending_summary_no_group(&["XPENDING s g"], "-NOGROUP No such key 's' or consumer group 'g'")]
#[case::pending_by_consumer(&after_delivery(&["XPENDING s g - + 10 bob"]), "[]")]
#[case::pending_too_many_consumers(&after_delivery(&["XPENDING s g - + 10 alice bob"]), "-ERR syntax error")]
#[case::pending_bad_count(&["XPENDING s g - + many"], "-ERR value is not an integer or out of range")]
#[case::pending_idle(&after_delivery(&["XPENDING s g IDLE 60000 - + 10"]), "[]")]
#[case::autoclaim_justid(&after_delivery(&["XAUTOCLAIM s g bob 0 0 JUSTID"]), "[\"0-0\", [\"1-1\", \"1-2\"], []]")]
#[case::autoclaim_count(&after_delivery(&["XAUTOCLAIM s g bob 0 - COUNT 1"]), "[\"1-2\", [[\"1-1\", [\"f\", \"a\"]]], []]")]
#[case::autoclaim_deleted(&after_delivery(&["XDEL s 1-1", "XAUTOCLAIM s g bob 0 0 COUNT 1"]), "[\"1-2\", [], [\"1-1\"]]")]
#[case::autoclaim_unknown_option(&after_delivery(&["XAUTOCLAIM s g bob 0 0 FORCE"]), "-ERR syntax error")]
#[case::claim(&after_delivery(&["XCLAIM s g bob 0 1-1"]), "[[\"1-1\", [\"f\", \"a\"]]]")]
#[case::claim_justid(&after_delivery(&["XCLAIM s g bob 0 1-1 1-2 JUSTID"]), "[\"1-1\", \"1-2\"]")]
#[case::claim_too_young(&after_delivery(&["XCLAIM s g bob 60000 1-1"]), "[]")]
#[case::claim_options(&after_delivery(&["XCLAIM s g bob 0 1-1 IDLE 60000 RETRYCOUNT 7 FORCE LASTID 1-2 JUSTID", "XAUTOCLAIM s g carol 60000 0 JUSTID"]), "[\"0-0\", [\"1-1\"], []]")]
#[case::claim_time(&after_delivery(&["XCLAIM s g bob 0 1-1 TIME 0 JUSTID"]), "[\"1-1\"]")]
#[case::claim_no_ids(&["XCLAIM s g bob 0 JUSTID"], "-ERR wrong number of arguments for 'xclaim' command")]
#[case::claim_unknown_option(&after_delivery(&["XCLAIM s g bob 0 1-1 FAST"]), "-ERR syntax error")]
#[case::info_groups(&after_delivery(&["XGROUP CREATE s h $", "XINFO GROUPS s"]), "[[\"name\", \"g\", \"consumers\", 1, \"pending\", 2, \"last-delivered-id\", \"1-2\"], [\"name\", \"h\", \"consumers\", 0, \"pending\", 0, \"last-delivered-id\", \"1-2\"]]")]
#[case::info_extra(&["XINFO GROUPS s t"], "-ERR syntax error")]
#[case::info_stream(&["XINFO STREAM s"], "-ERR unknown subcommand 'STREAM'. Try XINFO HELP.")]
#[tokio::test]
async fn stream_groups(#[case] lines: &[&str], #[case] expected: &str) {
    assert_eq!(last_reply(lines).await, expected);
}

#[tokio::test]
async fn pending_entries_report_their_consumer_and_deliveries() {
    let (_server, mut connection) = connect().await;
    for line in DELIVERED {
        run(&mut connection, line).await;
    }
    let pending = run(&mut connection, "XPENDING s g - + 1").await;
    assert!(
        pending.starts_with("[[\"1-1\", \"alice\", ") && pending.ends_with(", 1]]"),
        "{pending}"
    );
}

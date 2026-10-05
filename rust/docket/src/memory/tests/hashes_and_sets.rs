use rstest::rstest;

use super::last_reply;

#[rstest]
#[case::hset_counts_new_fields(&["HSET h a 1 b 2", "HSET h a 3 c 4"], "1")]
#[case::hset_odd_pairs(&["HSET h a 1 b"], "-ERR wrong number of arguments for 'hset' command")]
#[case::hset_no_pairs(&["HSET h"], "-ERR wrong number of arguments for 'hset' command")]
#[case::hset_wrong_type(&["SET h v", "HSET h a 1"], "-WRONGTYPE Operation against a key holding the wrong kind of value")]
#[case::hget(&["HSET h a 1", "HGET h a"], "\"1\"")]
#[case::hget_missing_field(&["HSET h a 1", "HGET h b"], "nil")]
#[case::hget_extra(&["HGET h a b"], "-ERR syntax error")]
#[case::hgetall(&["HSET h a 1", "HGETALL h"], "[\"a\", \"1\"]")]
#[case::hgetall_missing(&["HGETALL h"], "[]")]
#[case::hgetall_extra(&["HGETALL h a"], "-ERR syntax error")]
#[case::hdel(&["HSET h a 1 b 2", "HDEL h a c"], "1")]
#[case::hincrby(&["HSET h a 1", "HINCRBY h a 5"], "6")]
#[case::hincrby_new_field(&["HINCRBY h a -2"], "-2")]
#[case::hincrby_not_a_number(&["HINCRBY h a x"], "-ERR value is not an integer or out of range")]
#[case::hincrby_extra(&["HINCRBY h a 1 2"], "-ERR syntax error")]
#[case::hexists(&["HSET h a 1", "HEXISTS h a"], "1")]
#[case::hexists_missing(&["HEXISTS h a"], "0")]
#[case::hexists_extra(&["HEXISTS h a b"], "-ERR syntax error")]
#[case::sadd(&["SADD s a b", "SADD s b c"], "1")]
#[case::sadd_no_members(&["SADD s"], "-ERR wrong number of arguments for 'sadd' command")]
#[case::srem(&["SADD s a b", "SREM s a z"], "1")]
#[case::smembers(&["SADD s a", "SMEMBERS s"], "[\"a\"]")]
#[case::smembers_extra(&["SMEMBERS s t"], "-ERR syntax error")]
#[tokio::test]
async fn hashes_and_sets(#[case] lines: &[&str], #[case] expected: &str) {
    assert_eq!(last_reply(lines).await, expected);
}

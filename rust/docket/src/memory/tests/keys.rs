use std::time::Duration;

use rstest::rstest;

use super::{connect, last_reply, run};

#[rstest]
#[case::set_then_get(&["SET k v", "GET k"], "\"v\"")]
#[case::get_missing(&["GET k"], "nil")]
#[case::get_wrong_type(&["HSET k f v", "GET k"], "-WRONGTYPE Operation against a key holding the wrong kind of value")]
#[case::get_extra_argument(&["GET k extra"], "-ERR syntax error")]
#[case::get_no_key(&["GET"], "-ERR wrong number of arguments for 'get' command")]
#[case::set_reply(&["SET k v"], "ok")]
#[case::set_nx_on_missing(&["SET k v NX"], "ok")]
#[case::set_nx_on_existing(&["SET k v", "SET k w NX"], "nil")]
#[case::set_xx_on_missing(&["SET k v XX"], "nil")]
#[case::set_xx_on_existing(&["SET k v", "SET k w XX", "GET k"], "\"w\"")]
#[case::set_nx_and_xx(&["SET k v NX XX"], "-ERR syntax error")]
#[case::set_xx_and_nx(&["SET k v XX NX"], "-ERR syntax error")]
#[case::set_ex_and_px(&["SET k v EX 1 PX 1"], "-ERR syntax error")]
#[case::set_unknown_option(&["SET k v KEEPTTL"], "-ERR syntax error")]
#[case::set_zero_ex(&["SET k v EX 0"], "-ERR invalid expire time in 'set' command")]
#[case::set_negative_px(&["SET k v PX -5"], "-ERR invalid expire time in 'set' command")]
#[case::set_ex_not_a_number(&["SET k v EX soon"], "-ERR value is not an integer or out of range")]
#[case::set_ex_ttl(&["SET k v EX 100", "TTL k"], "99")]
#[case::setex(&["SETEX k 100 v", "GET k"], "\"v\"")]
#[case::setex_ttl(&["SETEX k 100 v", "TTL k"], "99")]
#[case::setex_zero(&["SETEX k 0 v"], "-ERR invalid expire time in 'setex' command")]
#[case::setex_extra(&["SETEX k 1 v w"], "-ERR syntax error")]
#[case::psetex_ttl(&["PSETEX k 100000 v", "TTL k"], "99")]
#[case::psetex_zero(&["PSETEX k 0 v"], "-ERR invalid expire time in 'psetex' command")]
#[case::psetex_extra(&["PSETEX k 1 v w"], "-ERR syntax error")]
#[case::mget(&["SET a 1", "HSET h f v", "MGET a b h"], "[\"1\", nil, nil]")]
#[case::mget_no_keys(&["MGET"], "-ERR wrong number of arguments for 'mget' command")]
#[case::del(&["SET a 1", "SET b 2", "DEL a b c"], "2")]
#[case::exists(&["SET a 1", "EXISTS a a b"], "2")]
#[case::expire_existing(&["SET k v", "EXPIRE k 100"], "1")]
#[case::expire_missing(&["EXPIRE k 100"], "0")]
#[case::expire_sets_ttl(&["SET k v", "EXPIRE k 100", "TTL k"], "99")]
#[case::expire_zero_deletes(&["SET k v", "EXPIRE k 0", "EXISTS k"], "0")]
#[case::expire_negative_reports_deletion(&["SET k v", "EXPIRE k -1"], "1")]
#[case::expire_option(&["SET k v", "EXPIRE k 10 NX"], "-ERR syntax error")]
#[case::pexpire_existing(&["SET k v", "PEXPIRE k 100000"], "1")]
#[case::pexpire_missing(&["PEXPIRE k 100000"], "0")]
#[case::pexpire_sets_ttl(&["SET k v", "PEXPIRE k 100000", "TTL k"], "99")]
#[case::pexpire_zero_deletes(&["SET k v", "PEXPIRE k 0", "EXISTS k"], "0")]
#[case::pexpire_option(&["SET k v", "PEXPIRE k 10 NX"], "-ERR syntax error")]
#[case::ttl_without_timeout(&["SET k v", "TTL k"], "-1")]
#[case::ttl_missing(&["TTL k"], "-2")]
#[case::ttl_extra(&["TTL k x"], "-ERR syntax error")]
#[case::pttl_without_timeout(&["SET k v", "PTTL k"], "-1")]
#[case::pttl_missing(&["PTTL k"], "-2")]
#[case::pttl_extra(&["PTTL k x"], "-ERR syntax error")]
#[tokio::test]
async fn keys(#[case] lines: &[&str], #[case] expected: &str) {
    assert_eq!(last_reply(lines).await, expected);
}

#[tokio::test]
async fn pttl_counts_milliseconds() {
    let (_server, mut connection) = connect().await;
    run(&mut connection, "SET k v PX 5000").await;
    let remaining: i64 = run(&mut connection, "PTTL k").await.parse().unwrap();
    assert!((4000..=5000).contains(&remaining), "{remaining}");
}

#[tokio::test]
async fn a_key_with_a_timeout_expires() {
    let (_server, mut connection) = connect().await;
    run(&mut connection, "SET k v PX 20").await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert_eq!(run(&mut connection, "GET k").await, "nil");
}

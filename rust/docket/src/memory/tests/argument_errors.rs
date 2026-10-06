use rstest::rstest;

use super::{connect, last_reply, send};

#[rstest]
#[case("SET")]
#[case("SET k")]
#[case("SET k v EX")]
#[case("SETEX")]
#[case("SETEX k 1")]
#[case("PSETEX")]
#[case("PSETEX k 1")]
#[case("DEL")]
#[case("EXISTS")]
#[case("EXPIRE")]
#[case("EXPIRE k")]
#[case("PEXPIRE")]
#[case("PEXPIRE k")]
#[case("TTL")]
#[case("PTTL")]
#[case("HSET")]
#[case("HGET")]
#[case("HGET h")]
#[case("HGETALL")]
#[case("HDEL")]
#[case("HDEL h")]
#[case("HINCRBY")]
#[case("HINCRBY h")]
#[case("HINCRBY h f")]
#[case("HEXISTS")]
#[case("HEXISTS h")]
#[case("SADD")]
#[case("SREM")]
#[case("SREM s")]
#[case("SMEMBERS")]
#[case("ZADD")]
#[case("ZREM")]
#[case("ZREM z")]
#[case("ZCARD")]
#[case("ZCOUNT")]
#[case("ZCOUNT z")]
#[case("ZCOUNT z 1")]
#[case("ZSCORE")]
#[case("ZSCORE z")]
#[case("ZRANGE")]
#[case("ZRANGE z")]
#[case("ZRANGE z 0")]
#[case("ZRANGEBYSCORE")]
#[case("ZRANGEBYSCORE z 0")]
#[case("ZRANGEBYSCORE z 0 1 LIMIT")]
#[case("ZRANGEBYSCORE z 0 1 LIMIT 0")]
#[case("ZREMRANGEBYSCORE")]
#[case("ZREMRANGEBYSCORE z 0")]
#[case("XADD")]
#[case("XLEN")]
#[case("XRANGE")]
#[case("XRANGE s")]
#[case("XRANGE s -")]
#[case("XRANGE s - + COUNT")]
#[case("XREVRANGE")]
#[case("XREVRANGE s")]
#[case("XREVRANGE s +")]
#[case("XDEL")]
#[case("XDEL s")]
#[case("XTRIM")]
#[case("XTRIM s")]
#[case("XTRIM s MAXLEN")]
#[case("XGROUP")]
#[case("XGROUP CREATE")]
#[case("XGROUP CREATE s")]
#[case("XGROUP CREATE s g")]
#[case("XGROUP CREATE s g 0 ENTRIESREAD")]
#[case("XGROUP DESTROY")]
#[case("XGROUP DESTROY s")]
#[case("XACK")]
#[case("XACK s")]
#[case("XACK s g")]
#[case("XPENDING")]
#[case("XPENDING s")]
#[case("XPENDING s g IDLE")]
#[case("XPENDING s g IDLE 10")]
#[case("XPENDING s g -")]
#[case("XPENDING s g - +")]
#[case("XAUTOCLAIM")]
#[case("XAUTOCLAIM s")]
#[case("XAUTOCLAIM s g")]
#[case("XAUTOCLAIM s g c")]
#[case("XAUTOCLAIM s g c 0")]
#[case("XAUTOCLAIM s g c 0 0 COUNT")]
#[case("XCLAIM")]
#[case("XCLAIM s")]
#[case("XCLAIM s g")]
#[case("XCLAIM s g c")]
#[case("XCLAIM s g c 0 1-1 IDLE")]
#[case("XCLAIM s g c 0 1-1 TIME")]
#[case("XCLAIM s g c 0 1-1 RETRYCOUNT")]
#[case("XCLAIM s g c 0 1-1 LASTID")]
#[case("XINFO")]
#[case("XINFO GROUPS")]
#[case("XREAD COUNT")]
#[case("XREAD BLOCK")]
#[case("XREADGROUP GROUP")]
#[case("XREADGROUP GROUP g")]
#[case("EVAL")]
#[case("EVAL return")]
#[case("EVALSHA")]
#[case("EVALSHA abc")]
#[case("SCRIPT")]
#[case("SCRIPT LOAD")]
#[case("PUBLISH")]
#[case("PUBLISH c")]
#[case("ECHO")]
#[case("CLIENT")]
#[case("SELECT")]
#[tokio::test]
async fn a_missing_argument_is_a_wrong_number_error(#[case] line: &str) {
    let name = line.split(' ').next().unwrap().to_ascii_lowercase();
    assert_eq!(
        last_reply(&[line]).await,
        format!("-ERR wrong number of arguments for '{name}' command")
    );
}

#[rstest]
#[case("HGETALL k")]
#[case("HDEL k f")]
#[case("HINCRBY k f 1")]
#[case("HEXISTS k f")]
#[case("SADD k m")]
#[case("SREM k m")]
#[case("SMEMBERS k")]
#[case("ZREM k m")]
#[case("ZCARD k")]
#[case("ZCOUNT k 0 1")]
#[case("ZSCORE k m")]
#[case("ZRANGE k 0 1")]
#[case("ZRANGEBYSCORE k 0 1")]
#[case("ZREMRANGEBYSCORE k 0 1")]
#[case("XLEN k")]
#[case("XREVRANGE k + -")]
#[case("XDEL k 1-1")]
#[case("XTRIM k MAXLEN 1")]
#[case("XTRIM k MINID 1")]
#[case("XGROUP DESTROY k g")]
#[case("XACK k g 1-1")]
#[case("XPENDING k g - + 1")]
#[case("XAUTOCLAIM k g c 0 0")]
#[case("XCLAIM k g c 0 1-1")]
#[case("XINFO GROUPS k")]
#[tokio::test]
async fn a_command_on_a_string_is_a_wrong_type_error(#[case] line: &str) {
    assert_eq!(
        last_reply(&["SET k v", line]).await,
        "-WRONGTYPE Operation against a key holding the wrong kind of value"
    );
}

#[rstest]
#[case::number(&["ZADD", "z", "\u{ff}", "m"], "-ERR value is not a valid float")]
#[case::stream_id(&["XDEL", "s", "\u{ff}"], "-ERR Invalid stream ID specified as stream command argument")]
#[tokio::test]
async fn an_argument_that_is_not_text_is_invalid(#[case] words: &[&str], #[case] expected: &str) {
    let (_server, mut connection) = connect().await;
    let mut command = redis::cmd(words[0]);
    for word in &words[1..] {
        // Latin-1 bytes, so U+00FF becomes the lone byte 0xFF.
        command.arg(word.chars().map(|c| c as u8).collect::<Vec<u8>>());
    }
    assert_eq!(send(&mut connection, &command).await, expected);
}

#[rstest]
#[case::zcount_max("ZCOUNT z 1 x", "-ERR min or max is not a float")]
#[case::xpending_start(
    "XPENDING s g x + 1",
    "-ERR Invalid stream ID specified as stream command argument"
)]
#[case::xpending_end(
    "XPENDING s g - x 1",
    "-ERR Invalid stream ID specified as stream command argument"
)]
#[case::xautoclaim_start(
    "XAUTOCLAIM s g c 0 x",
    "-ERR Invalid stream ID specified as stream command argument"
)]
#[case::xrevrange_end(
    "XREVRANGE s x -",
    "-ERR Invalid stream ID specified as stream command argument"
)]
#[case::xrevrange_start(
    "XREVRANGE s + x",
    "-ERR Invalid stream ID specified as stream command argument"
)]
#[case::xrevrange_option("XREVRANGE s + - LIMIT 1", "-ERR syntax error")]
#[tokio::test]
async fn a_malformed_bound_is_rejected(#[case] line: &str, #[case] expected: &str) {
    assert_eq!(last_reply(&[line]).await, expected);
}

#[rstest]
#[case::bad_milliseconds("XDEL s x-1")]
#[case::bad_sequence("XDEL s 1-x")]
#[tokio::test]
async fn a_malformed_stream_id_is_invalid(#[case] line: &str) {
    assert_eq!(
        last_reply(&[line]).await,
        "-ERR Invalid stream ID specified as stream command argument"
    );
}

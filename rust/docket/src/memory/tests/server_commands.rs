use rstest::rstest;

use super::{connect, last_reply};

#[rstest]
#[case::ping(&["PING"], "PONG")]
#[case::ping_message(&["PING hello"], "\"hello\"")]
#[case::ping_extra(&["PING a b"], "-ERR wrong number of arguments for 'ping' command")]
#[case::echo(&["ECHO hi"], "\"hi\"")]
#[case::echo_extra(&["ECHO a b"], "-ERR syntax error")]
#[case::time_extra(&["TIME now"], "-ERR syntax error")]
#[case::client_setname(&["CLIENT SETNAME worker"], "ok")]
#[case::client_setinfo(&["CLIENT SETINFO LIB-NAME docket"], "ok")]
#[case::client_getname(&["CLIENT SETNAME worker", "CLIENT GETNAME"], "nil")]
#[case::client_wrong_count(&["CLIENT SETNAME"], "-ERR wrong number of arguments for 'client|setname' command")]
#[case::client_unknown(&["CLIENT KILL x"], "-ERR unknown subcommand 'KILL'. Try CLIENT HELP.")]
#[case::select_zero(&["SELECT 0"], "ok")]
#[case::select_other(&["SELECT 1"], "-ERR DB index is out of range")]
#[case::select_extra(&["SELECT 0 1"], "-ERR syntax error")]
#[case::info(&["INFO server"], "\"# Server\\r\\nredis_version:7.4.0\\r\\n\"")]
#[case::flushall(&["SET a 1", "HSET b f v", "FLUSHALL", "EXISTS a b"], "0")]
#[case::flushall_async(&["FLUSHALL ASYNC"], "ok")]
#[case::flushall_extra(&["FLUSHALL SYNC now"], "-ERR syntax error")]
#[case::flushall_unknown(&["FLUSHALL LATER"], "-ERR syntax error")]
#[case::publish_without_subscribers(&["PUBLISH c hello"], "0")]
#[case::publish_extra(&["PUBLISH c hello again"], "-ERR syntax error")]
#[case::unknown_command(&["FROBNICATE x"], "-ERR unknown command 'frobnicate'")]
#[case::lower_case_command(&["set k v", "get k"], "\"v\"")]
#[tokio::test]
async fn server_commands(#[case] lines: &[&str], #[case] expected: &str) {
    assert_eq!(last_reply(lines).await, expected);
}

#[tokio::test]
async fn time_is_seconds_and_microseconds() {
    let (_server, mut connection) = connect().await;
    let (seconds, micros): (u64, u64) = redis::cmd("TIME")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert!(seconds > 1_700_000_000);
    assert!(micros < 1_000_000);
}

use redis::Value;
use rstest::rstest;

use super::{connect, render, replies};

#[rstest]
#[case::exec(&["MULTI", "SET a 1", "GET a", "EXEC"], &["ok", "QUEUED", "QUEUED", "[ok, \"1\"]"])]
#[case::errors_stay_in_place(&["SET s v", "MULTI", "HGET s f", "GET s", "EXEC"], &["ok", "ok", "QUEUED", "QUEUED", "[-WRONGTYPE Operation against a key holding the wrong kind of value, \"v\"]"])]
#[case::reads_do_not_wait(&["MULTI", "XREAD BLOCK 0 STREAMS s $", "EXEC"], &["ok", "QUEUED", "[nil]"])]
#[case::subscribe_runs_in_exec(&["MULTI", "SUBSCRIBE c", "EXEC"], &["ok", "QUEUED", "[[\"subscribe\", \"c\", 1]]"])]
#[case::discard(&["MULTI", "SET a 1", "DISCARD", "GET a"], &["ok", "QUEUED", "ok", "nil"])]
#[case::unknown_command_aborts(&["MULTI", "FROB", "SET a 1", "EXEC", "GET a"], &["ok", "-ERR unknown command 'frob'", "QUEUED", "-EXECABORT Transaction discarded because of previous errors.", "nil"])]
#[case::nested(&["MULTI", "MULTI", "EXEC"], &["ok", "-ERR MULTI calls can not be nested", "[]"])]
#[case::exec_without_multi(&["EXEC"], &["-ERR EXEC without MULTI"])]
#[case::discard_without_multi(&["DISCARD"], &["-ERR DISCARD without MULTI"])]
#[tokio::test]
async fn transactions(#[case] lines: &[&str], #[case] expected: &[&str]) {
    assert_eq!(replies(lines).await, expected);
}

#[tokio::test]
async fn an_atomic_pipeline_runs_as_a_transaction() {
    let (_server, mut connection) = connect().await;
    let results: Value = redis::pipe()
        .atomic()
        .cmd("SET")
        .arg("a")
        .arg("1")
        .cmd("HSET")
        .arg("h")
        .arg("f")
        .arg("v")
        .ignore()
        .cmd("GET")
        .arg("a")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(render(results), "[ok, \"1\"]");
}

#[tokio::test]
async fn a_pipeline_gets_every_reply_in_order() {
    let (_server, mut connection) = connect().await;
    let results: (String, i64, Option<String>) = redis::pipe()
        .cmd("SET")
        .arg("a")
        .arg("1")
        .cmd("DEL")
        .arg("a")
        .cmd("GET")
        .arg("a")
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(results, ("OK".to_owned(), 1, None));
}

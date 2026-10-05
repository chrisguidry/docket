use redis::{ErrorKind, Script, ServerErrorKind};
use rstest::rstest;

use super::{connect, run, send};

fn eval(script: &str, keys: &[&str], args: &[&str]) -> redis::Cmd {
    let mut command = redis::cmd("EVAL");
    command.arg(script).arg(keys.len()).arg(keys).arg(args);
    command
}

#[rstest]
#[case::integer("return 7", "7")]
#[case::string("return 'seven'", "\"seven\"")]
#[case::table("return {1, 'two', {3}}", "[1, \"two\", [3]]")]
#[case::nil("return nil", "nil")]
#[case::status("return {ok = 'FINE'}", "FINE")]
#[case::error_table("return {err = 'NOPE not today'}", "-NOPE not today")]
#[case::error_without_code("return {err = 'not today'}", "-ERR not today")]
#[case::failed_call(
    "return redis.call('HGET', KEYS[1], 'f')",
    "-WRONGTYPE Operation against a key holding the wrong kind of value"
)]
#[case::keys_and_args("return {KEYS[1], ARGV[1]}", "[\"k\", \"a\"]")]
#[tokio::test]
async fn eval_replies(#[case] script: &str, #[case] expected: &str) {
    let (_server, mut connection) = connect().await;
    run(&mut connection, "SET k v").await;
    assert_eq!(
        send(&mut connection, &eval(script, &["k"], &["a"])).await,
        expected
    );
}

#[tokio::test]
async fn a_raised_lua_error_is_an_err_reply() {
    let (_server, mut connection) = connect().await;
    let reply = send(&mut connection, &eval("error('broken')", &[], &[])).await;
    assert!(
        reply.starts_with("-ERR ") && reply.ends_with(":1: broken"),
        "{reply}"
    );
}

#[rstest]
#[case::negative_keys("EVAL return -1", "-ERR Number of keys can't be negative")]
#[case::too_many_keys(
    "EVAL return 2 k",
    "-ERR Number of keys can't be greater than number of args"
)]
#[case::evalsha_negative_keys("EVALSHA abc -1", "-ERR Number of keys can't be negative")]
#[case::evalsha_unknown(
    "EVALSHA 0123456789abcdef0123456789abcdef01234567 0",
    "-NOSCRIPT No matching script. Use EVAL."
)]
#[case::script_exists_unknown("SCRIPT EXISTS 0123456789abcdef0123456789abcdef01234567", "[0]")]
#[case::script_exists_none("SCRIPT EXISTS", "-ERR wrong number of arguments for 'script' command")]
#[case::script_load_extra("SCRIPT LOAD return x", "-ERR syntax error")]
#[case::script_flush("SCRIPT FLUSH", "-ERR unknown subcommand 'FLUSH'. Try SCRIPT HELP.")]
#[tokio::test]
async fn script_errors(#[case] line: &str, #[case] expected: &str) {
    let (_server, mut connection) = connect().await;
    assert_eq!(run(&mut connection, line).await, expected);
}

#[tokio::test]
async fn a_loaded_script_runs_by_its_sha_in_either_case() {
    let (_server, mut connection) = connect().await;
    let load = redis::cmd("SCRIPT").arg("LOAD").arg("return 7").clone();
    let sha = send(&mut connection, &load).await;
    let sha = sha.trim_matches('"').to_ascii_uppercase();
    assert_eq!(
        run(&mut connection, &format!("SCRIPT EXISTS {sha}")).await,
        "[1]"
    );
    assert_eq!(run(&mut connection, &format!("EVALSHA {sha} 0")).await, "7");
}

#[tokio::test]
async fn script_invoke_loads_the_script_on_noscript() {
    let (_server, mut connection) = connect().await;
    let script = Script::new("return ARGV[1] + 1");
    let reply: i64 = script.arg(41).invoke_async(&mut connection).await.unwrap();
    assert_eq!(reply, 42);
}

#[tokio::test]
async fn evalsha_of_an_unknown_script_is_a_noscript_error() {
    let (_server, mut connection) = connect().await;
    let error = redis::cmd("EVALSHA")
        .arg("0123456789abcdef0123456789abcdef01234567")
        .arg(0)
        .query_async::<()>(&mut connection)
        .await
        .unwrap_err();
    assert_eq!(error.kind(), ErrorKind::Server(ServerErrorKind::NoScript));
}

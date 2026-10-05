use rstest::rstest;

use super::{SentinelUrl, Target, parse};

fn sentinel(sentinels: &[(&str, u16)], service: &str) -> SentinelUrl {
    SentinelUrl {
        sentinels: sentinels
            .iter()
            .map(|(h, p)| ((*h).to_owned(), *p))
            .collect(),
        service: service.to_owned(),
        db: 0,
        tls: false,
        username: None,
        password: None,
        daemon_username: None,
        daemon_password: None,
    }
}

#[rstest]
#[case::redis("redis://localhost:6379/0", Target::Standalone("redis://localhost:6379/0".into()))]
#[case::tls("rediss://host:6380/1", Target::Standalone("rediss://host:6380/1".into()))]
#[case::unix("unix:///tmp/redis.sock", Target::Standalone("unix:///tmp/redis.sock".into()))]
#[case::cluster("redis+cluster://node:7000", Target::Cluster("redis://node:7000".into()))]
#[case::cluster_tls("rediss+cluster://node:7000", Target::Cluster("rediss://node:7000".into()))]
#[case::memory("memory://tests", Target::Memory("memory://tests".into()))]
#[case::sentinel("redis+sentinel://s1/mymaster", Target::Sentinel(sentinel(&[("s1", 26379)], "mymaster")))]
fn parses_each_scheme(#[case] url: &str, #[case] expected: Target) {
    assert_eq!(parse(url).unwrap(), expected);
}

#[test]
fn parses_every_part_of_a_sentinel_url() {
    let url = "rediss+sentinel://me:p%40ss@s1:1,[::1]:2,s3,/svc/3?sentinel_username=sen&sentinel_password=pw&ssl_cert_reqs=none";
    let Target::Sentinel(parsed) = parse(url).unwrap() else {
        panic!("not a sentinel target");
    };
    assert_eq!(
        parsed,
        SentinelUrl {
            sentinels: vec![("s1".into(), 1), ("::1".into(), 2), ("s3".into(), 26379)],
            service: "svc".into(),
            db: 3,
            tls: true,
            username: Some("me".into()),
            password: Some("p@ss".into()),
            daemon_username: Some("sen".into()),
            daemon_password: Some("pw".into()),
        }
    );
}

#[test]
fn a_sentinel_bracketed_host_may_omit_its_port() {
    let Target::Sentinel(parsed) = parse("redis+sentinel://[fe80::1]/svc").unwrap() else {
        panic!("not a sentinel target");
    };
    assert_eq!(parsed.sentinels, vec![("fe80::1".to_owned(), 26379)]);
}

#[test]
fn a_sentinel_user_without_a_password() {
    let Target::Sentinel(parsed) = parse("redis+sentinel://me@s1/svc?flag").unwrap() else {
        panic!("not a sentinel target");
    };
    assert_eq!(
        (parsed.username, parsed.password),
        (Some("me".into()), None)
    );
}

#[rstest]
#[case::no_scheme("localhost:6379", "it has no scheme")]
#[case::unknown_scheme("http://localhost", "docket does not know http://")]
#[case::no_sentinels("redis+sentinel:///svc", "it names no sentinel host")]
#[case::no_service("redis+sentinel://s1", "it names no service")]
#[case::bad_db("redis+sentinel://s1/svc/x", "x is not a database number")]
#[case::bad_port("redis+sentinel://s1:port/svc", "s1:port has no valid port")]
#[case::empty_host("redis+sentinel://:1/svc", ":1 has no host")]
#[case::open_bracket("redis+sentinel://[::1/svc", "[::1 has no closing bracket")]
fn rejects_bad_urls(#[case] url: &str, #[case] reason: &str) {
    let message = parse(url).unwrap_err().to_string();
    assert!(message.contains(reason), "{message}");
}

//! The proxy's rules: which commands it answers itself, and with what.

use std::fmt::Write;
use std::time::Duration;

use sha1::{Digest, Sha1};

use super::Proxy;

type Matcher = Box<dyn Fn(&[Vec<u8>]) -> bool + Send>;

/// Answers the next `times` commands that `matches` picks with `reply`, a
/// RESP reply, or with a generic error when `reply` is `None`.
pub(super) struct Rule {
    pub(super) matches: Matcher,
    pub(super) times: usize,
    pub(super) reply: Option<Vec<u8>>,
    /// Closes the connection after the reply, as Redis does when it goes
    /// away partway through one.
    pub(super) close: bool,
    /// How long the reply waits, which holds up the connection's later
    /// commands too.
    pub(super) delay: Duration,
    /// Sends the command to Redis, and gives the client `reply` in place of
    /// Redis's reply.
    pub(super) forward: bool,
    /// Picks only commands on connections that have subscribed.
    pub(super) subscribers_only: bool,
}

/// What the proxy does for one command that a rule picked.
pub(super) struct Injection {
    pub(super) reply: Vec<u8>,
    pub(super) close: bool,
    pub(super) delay: Duration,
    pub(super) forward: bool,
}

impl Rule {
    fn new(
        times: usize,
        reply: Option<Vec<u8>>,
        matches: impl Fn(&[Vec<u8>]) -> bool + Send + 'static,
    ) -> Self {
        Self {
            matches: Box::new(matches),
            times,
            reply,
            close: false,
            delay: Duration::ZERO,
            forward: false,
            subscribers_only: false,
        }
    }
}

/// The start of a reply whose last bulk string never finishes.
const TRUNCATED: &[u8] = b"*2\r\n$4\r\nargs\r\n$10\r\nabc";

/// A rule for the runs of the Lua script with this source.
fn script_rule(source: &str, times: usize, reply: Option<Vec<u8>>) -> Rule {
    let sha = Sha1::digest(source.as_bytes())
        .iter()
        .fold(String::new(), |mut hex, byte| {
            let _ = write!(hex, "{byte:02x}");
            hex
        });
    Rule::new(times, reply, move |args| {
        args[0].eq_ignore_ascii_case(b"EVALSHA")
            && args
                .get(1)
                .is_some_and(|arg| arg.eq_ignore_ascii_case(sha.as_bytes()))
    })
}

impl Proxy {
    /// Answers the next `times` commands named `command` with an error,
    /// without sending them to Redis.
    pub fn fail(&self, command: &str, times: usize) {
        self.fail_named(command, times, false);
    }

    /// Answers the next `times` commands named `command` on connections
    /// that have subscribed with an error, without sending them to Redis.
    /// Only a RESP3 connection sends commands once it has subscribed.
    pub fn fail_on_subscribers(&self, command: &str, times: usize) {
        self.fail_named(command, times, true);
    }

    fn fail_named(&self, command: &str, times: usize, subscribers_only: bool) {
        let command = command.to_ascii_uppercase();
        let rule = Rule::new(times, None, move |args| {
            args[0].eq_ignore_ascii_case(command.as_bytes())
        });
        self.add(Rule {
            subscribers_only,
            ..rule
        });
    }

    /// Answers the next command named `command` with the start of a reply,
    /// then closes the connection, as when Redis goes away partway through a
    /// reply.
    pub fn cut_reply(&self, command: &str) {
        let command = command.to_ascii_uppercase();
        let rule = Rule::new(1, Some(TRUNCATED.to_vec()), move |args| {
            args[0].eq_ignore_ascii_case(command.as_bytes())
        });
        self.add(Rule {
            close: true,
            ..rule
        });
    }

    /// Answers the next run of the Lua script with this source with the
    /// start of a reply, then closes the connection.
    pub fn cut_script_reply(&self, source: &str) {
        let rule = script_rule(source, 1, Some(TRUNCATED.to_vec()));
        self.add(Rule {
            close: true,
            ..rule
        });
    }

    /// Answers the next run of the Lua script with this source with `reply`
    /// once `delay` has passed.  The connection's later commands wait too.
    pub fn answer_script_after(&self, source: &str, reply: &[u8], delay: Duration) {
        let rule = script_rule(source, 1, Some(reply.to_vec()));
        self.add(Rule { delay, ..rule });
    }

    /// Sends the next run of the Lua script with this source to Redis, and
    /// gives the client `reply` in place of Redis's reply.
    pub fn replace_script_reply(&self, source: &str, reply: &[u8]) {
        let rule = script_rule(source, 1, Some(reply.to_vec()));
        self.add(Rule {
            forward: true,
            ..rule
        });
    }

    /// Answers the next `times` runs of the Lua script with this source with
    /// an error.
    pub fn fail_script(&self, source: &str, times: usize) {
        self.answer_script(source, times, None);
    }

    /// Answers the next `times` runs of the Lua script with this source with
    /// `error`, such as `READONLY You can't write against a read only
    /// replica.`
    pub fn fail_script_with(&self, source: &str, times: usize, error: &str) {
        self.answer_script(source, times, Some(format!("-{error}\r\n").into_bytes()));
    }

    /// Answers the next `times` runs of the Lua script with this source with
    /// `reply`, a RESP reply, or with an error when `reply` is `None`.
    pub fn answer_script(&self, source: &str, times: usize, reply: Option<Vec<u8>>) {
        self.add(script_rule(source, times, reply));
    }

    fn add(&self, rule: Rule) {
        self.state.lock().unwrap().rules.push(rule);
    }
}

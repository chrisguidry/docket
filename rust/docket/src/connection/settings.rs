//! How docket's connections to Redis time out, retry, and detect a dead
//! peer.

use std::time::Duration;

use redis::io::tcp::TcpSettings;
use redis::io::tcp::socket2::TcpKeepalive;

use crate::error::{Error, Result};

/// The connection settings a docket applies to the connections it opens.
/// redis-rs takes only TCP settings for its connections to the Sentinels,
/// so of these, only keepalive reaches the Sentinels themselves.
#[derive(Clone, Debug)]
pub(crate) struct Settings {
    pub connection_timeout: Duration,
    pub response_timeout: Option<Duration>,
    pub retries: u32,
    pub min_retry_wait: Duration,
    pub max_retry_wait: Duration,
    pub keepalive: Keepalive,
}

/// TCP keepalive: probes start after `idle` without traffic, repeat every
/// `interval`, and the kernel drops the connection after `probes` of them
/// go unanswered.
#[derive(Clone, Debug)]
pub(crate) struct Keepalive {
    pub idle: Duration,
    pub interval: Duration,
    pub probes: u32,
}

/// The shortest block of a blocking read.  Redis takes a block in whole
/// milliseconds and reads `BLOCK 0` as a block with no end.  A response
/// timeout under twice this leaves a block no room to end before the
/// timeout, so every blocking read would time out.
const SHORTEST_BLOCK: Duration = Duration::from_millis(1);

/// The longest keepalive idle time and interval Linux takes, in seconds.
const MAX_KEEPALIVE_SECONDS: u64 = 32767;

/// The most keepalive probes Linux takes.
const MAX_KEEPALIVE_PROBES: u32 = 127;

impl Keepalive {
    /// Refuses timers the kernel would refuse.  The kernel takes the idle
    /// time and interval in whole seconds, so they are checked as whole
    /// seconds: 500 ms would be 0 s, which Linux refuses on every connect.
    /// The limits are Linux's, and docket applies them everywhere, so a
    /// setting that works on one system works on the others.
    fn validate(&self) -> Result<()> {
        let seconds = |name: &str, timer: Duration| {
            if (1..=MAX_KEEPALIVE_SECONDS).contains(&timer.as_secs()) {
                Ok(())
            } else {
                Err(Error::Invalid(format!(
                    "the TCP keepalive {name} must be from 1 to {MAX_KEEPALIVE_SECONDS} \
                     whole seconds, not {timer:?}"
                )))
            }
        };
        seconds("idle time", self.idle)?;
        seconds("interval", self.interval)?;
        if !(1..=MAX_KEEPALIVE_PROBES).contains(&self.probes) {
            return Err(Error::Invalid(format!(
                "the TCP keepalive probes must be from 1 to {MAX_KEEPALIVE_PROBES}, not {}",
                self.probes
            )));
        }
        Ok(())
    }
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            // A connect that stalls, for example on dropped SYN packets, has
            // no server work to wait for.
            connection_timeout: Duration::from_secs(10),
            // Commands wait for Redis as long as it takes, as in pydocket,
            // because some of docket's Lua scripts, such as clearing a
            // docket, run as long as the backlog is large.  A timeout would
            // report them as failed while Redis still finished the work.
            // TCP keepalive finds a server that is gone.
            response_timeout: None,
            // A slot migration answers with a burst of redirects, and a
            // failover takes a few seconds; these ride out both.
            retries: 10,
            min_retry_wait: Duration::from_millis(10),
            max_retry_wait: Duration::from_secs(1),
            // A peer that vanished without closing the socket is found in
            // 30 + 3 * 5 = 45 seconds, even on a connection that is waiting
            // for pub/sub messages and sends nothing.
            keepalive: Keepalive {
                idle: Duration::from_secs(30),
                interval: Duration::from_secs(5),
                probes: 3,
            },
        }
    }
}

impl Settings {
    /// Refuses settings that would make every connection fail.
    pub fn validate(&self) -> Result<()> {
        if self.connection_timeout.is_zero() {
            return Err(Error::Invalid(
                "the connection timeout must be longer than zero".into(),
            ));
        }
        if let Some(timeout) = self.response_timeout
            && timeout < SHORTEST_BLOCK * 2
        {
            return Err(Error::Invalid(format!(
                "the response timeout must be at least {:?}, twice the shortest block \
                 of a blocking read, not {timeout:?}",
                SHORTEST_BLOCK * 2
            )));
        }
        if self.min_retry_wait > self.max_retry_wait {
            return Err(Error::Invalid(
                "the shortest retry wait must not be longer than the longest".into(),
            ));
        }
        self.keepalive.validate()
    }

    /// How long a blocking read may block when it would like to block for
    /// `wanted`.  A block at least as long as the response timeout fails as
    /// a timeout before Redis answers, so blocks stop at half of it, which
    /// leaves the other half for the reply to arrive.  A block is never
    /// shorter than [`SHORTEST_BLOCK`].
    pub fn block(&self, wanted: Duration) -> Duration {
        let wanted = match self.response_timeout {
            Some(timeout) => wanted.min(timeout / 2),
            None => wanted,
        };
        wanted.max(SHORTEST_BLOCK)
    }

    /// The TCP settings for every TCP connection to Redis.
    pub fn tcp(&self) -> TcpSettings {
        TcpSettings::default().set_keepalive(self.keepalive())
    }

    fn keepalive(&self) -> TcpKeepalive {
        let keepalive = TcpKeepalive::new().with_time(self.keepalive.idle);
        #[cfg(any(
            target_os = "android",
            target_os = "freebsd",
            target_os = "fuchsia",
            target_os = "illumos",
            target_os = "ios",
            target_os = "linux",
            target_os = "macos",
            target_os = "netbsd",
            target_os = "windows",
        ))]
        let keepalive = keepalive
            .with_interval(self.keepalive.interval)
            .with_retries(self.keepalive.probes);
        keepalive
    }

    /// Retry waits in milliseconds, the unit redis-rs takes them in.
    pub fn retry_millis(&self) -> (u64, u64) {
        let millis = |wait: Duration| u64::try_from(wait.as_millis()).unwrap_or(u64::MAX);
        (millis(self.min_retry_wait), millis(self.max_retry_wait))
    }
}

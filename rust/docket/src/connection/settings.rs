//! How docket's connections to Redis time out, retry, and detect a dead
//! peer.

use std::time::Duration;

use redis::io::tcp::TcpSettings;
use redis::io::tcp::socket2::TcpKeepalive;

use crate::error::{Error, Result};

/// The connection settings a docket applies to every connection it opens.
#[derive(Clone, Debug)]
pub(crate) struct Settings {
    pub connection_timeout: Duration,
    pub response_timeout: Duration,
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

impl Default for Settings {
    fn default() -> Self {
        Self {
            // A connect that stalls, for example on dropped SYN packets, has
            // no server work to wait for.
            connection_timeout: Duration::from_secs(10),
            // A server that does not answer in this long is gone or stuck.
            // Blocking reads stay under it, so it bounds every command.
            response_timeout: Duration::from_secs(10),
            // A slot migration answers with a burst of redirects, and a
            // failover takes a few seconds; these ride out both within the
            // response timeout.
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
        if self.response_timeout.is_zero() {
            return Err(Error::Invalid(
                "the response timeout must be longer than zero".into(),
            ));
        }
        if self.min_retry_wait > self.max_retry_wait {
            return Err(Error::Invalid(
                "the shortest retry wait must not be longer than the longest".into(),
            ));
        }
        if self.keepalive.idle.is_zero()
            || self.keepalive.interval.is_zero()
            || self.keepalive.probes == 0
        {
            return Err(Error::Invalid(
                "the keepalive idle time, interval, and probes must be more than zero".into(),
            ));
        }
        Ok(())
    }

    /// How long a blocking read may block when it would like to block for
    /// `wanted`.  A block at least as long as the response timeout fails as
    /// a timeout before Redis answers, so blocks stop at half of it, which
    /// leaves the other half for the reply to arrive.  A block is never
    /// shorter than a millisecond, because Redis reads `BLOCK 0` as a block
    /// with no end.
    pub fn block(&self, wanted: Duration) -> Duration {
        wanted
            .min(self.response_timeout / 2)
            .max(Duration::from_millis(1))
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

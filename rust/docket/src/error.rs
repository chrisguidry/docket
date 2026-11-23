/// Everything that can go wrong in docket.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// Redis refused a command or could not be reached.
    #[error(transparent)]
    Redis(#[from] redis::RedisError),

    /// A docket URL that docket cannot connect to.
    #[error("{url} is not a docket URL: {reason}")]
    Url {
        /// The URL as given, with `***` in place of its passwords.
        url: String,
        /// What is wrong with it.
        reason: String,
    },

    /// Task arguments or a result that do not convert to or from JSON.
    #[error(transparent)]
    Json(#[from] serde_json::Error),

    /// The task ended as failed.  This is the error of
    /// [`Execution::result`](crate::Execution::result).
    #[error("task {key} failed: {message}")]
    TaskFailed {
        /// The task's key.
        key: String,
        /// The error the task's handler returned.
        message: String,
    },

    /// The task was cancelled before it finished.
    #[error("task {key} was cancelled")]
    TaskCancelled {
        /// The task's key.
        key: String,
    },

    /// A setting or argument that docket cannot use, such as a
    /// concurrency of zero.
    #[error("{0}")]
    Invalid(String),

    /// A command of [`docket::cli`](crate::cli) could not write its output.
    #[cfg(feature = "cli")]
    #[error("could not write the output: {0}")]
    Output(std::io::Error),
}

/// A `Result` whose error is docket's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub(crate) fn url(url: &str, reason: impl Into<String>) -> Self {
        Self::Url {
            url: crate::connection::redact(url),
            reason: reason.into(),
        }
    }

    /// Whether the error means that Redis is unavailable, so that a worker
    /// should reconnect instead of stopping.  Every error from Redis counts,
    /// because a server that refuses commands (for example a read-only
    /// replica after a failover) recovers the same way a lost one does.  A
    /// reply that docket cannot decode, such as a hash value that is not
    /// UTF-8 text, does not count: a reconnect reads the same reply again.
    #[must_use]
    pub fn is_redis_unavailable(&self) -> bool {
        matches!(self, Self::Redis(_)) && !self.is_undecodable_reply()
    }

    /// Whether Redis sent a reply that did not convert to the type docket
    /// asked for.
    pub(crate) fn is_undecodable_reply(&self) -> bool {
        // redis-rs gives a malformed reply on the wire the same kind, but it
        // also drops that connection, so the next command reports Redis as
        // unavailable.
        matches!(self, Self::Redis(error) if error.kind() == redis::ErrorKind::Parse)
    }

    /// Whether Redis answered a command with an error that concerns that
    /// command alone, such as a script error or a key of the wrong type.
    /// The connection still works, so the other commands on it go on.  A
    /// lost connection, a timeout, and a reply that says the server cannot
    /// serve now (`READONLY` after a failover, `LOADING`, `CLUSTERDOWN`,
    /// `MASTERDOWN`, `TRYAGAIN`, or a redirect the cluster client did not
    /// follow) do not count, because they need a reconnect.  Nor does `OOM`:
    /// a server at its `maxmemory` refuses every write but still serves a
    /// worker's reads, so a worker that went on would read every ready task
    /// into its pending list without a pause and run none of them.  Redis
    /// 6.2 reports an `OOM` inside a script as an `ERR` whose text holds
    /// `-OOM`, so the text counts too.
    pub(crate) fn is_refused_command(&self) -> bool {
        matches!(
            self,
            Self::Redis(error) if matches!(
                error.kind(),
                redis::ErrorKind::Server(_) | redis::ErrorKind::Extension
            ) && matches!(error.retry_method(), redis::RetryMethod::NoRetry)
                && error.code() != Some("OOM")
                && !error.detail().is_some_and(|detail| detail.contains("-OOM "))
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::Error;

    /// The error redis-rs returns for a hash whose value is not UTF-8, as a
    /// hash that pydocket wrote with pickled arguments is.
    fn undecodable_hash() -> Error {
        let reply = redis::Value::Map(vec![(
            redis::Value::BulkString(b"args".to_vec()),
            redis::Value::BulkString(vec![0x80, 0x05, 0x95]),
        )]);
        redis::from_redis_value::<HashMap<String, String>>(reply)
            .map_err(redis::RedisError::from)
            .unwrap_err()
            .into()
    }

    #[test]
    fn an_undecodable_reply_is_not_redis_being_unavailable() {
        let error = undecodable_hash();
        assert!(
            error.to_string().contains("Cannot convert from UTF-8"),
            "{error}"
        );
        assert!(error.is_undecodable_reply());
        assert!(!error.is_redis_unavailable());
    }

    #[test]
    fn a_refused_connection_is_redis_being_unavailable() {
        let refused = std::io::Error::from(std::io::ErrorKind::ConnectionRefused);
        let error = Error::from(redis::RedisError::from(refused));
        assert!(error.is_redis_unavailable());
        assert!(!error.is_undecodable_reply());
    }

    #[test]
    fn an_error_from_the_server_is_redis_being_unavailable() {
        let error = Error::from(redis::RedisError::from((
            redis::ErrorKind::Server(redis::ServerErrorKind::ReadOnly),
            "READONLY",
        )));
        assert!(error.is_redis_unavailable());
        assert!(!error.is_undecodable_reply());
    }
}

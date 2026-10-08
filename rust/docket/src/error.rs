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

    /// A setting that docket cannot use, such as a concurrency of zero.
    #[error("{0}")]
    Invalid(String),
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
    /// replica after a failover) recovers the same way a lost one does.
    #[must_use]
    pub fn is_redis_unavailable(&self) -> bool {
        matches!(self, Self::Redis(_))
    }

    /// Whether Redis answered a command with an error that concerns that
    /// command alone, such as a script error, a key of the wrong type, or
    /// a refusal for lack of memory.  The connection still works, so the
    /// other commands on it go on.  A lost connection, a timeout, and a
    /// reply that says the server cannot serve now (`READONLY` after a
    /// failover, `LOADING`, `CLUSTERDOWN`, `MASTERDOWN`, `TRYAGAIN`, or a
    /// redirect the cluster client did not follow) do not count, because
    /// they need a reconnect.
    pub(crate) fn is_refused_command(&self) -> bool {
        matches!(
            self,
            Self::Redis(error) if matches!(
                error.kind(),
                redis::ErrorKind::Server(_) | redis::ErrorKind::Extension
            ) && matches!(error.retry_method(), redis::RetryMethod::NoRetry)
        )
    }
}

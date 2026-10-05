//! The Redis key of everything in a docket.  The names match pydocket's, so
//! the shared Lua scripts and the conformance driver find the same data.

/// The consumer group that every worker of every docket reads with.
pub(crate) const WORKER_GROUP: &str = "docket-workers";

#[derive(Clone, Debug)]
pub(crate) struct Keys {
    prefix: String,
}

impl Keys {
    pub fn new(prefix: String) -> Self {
        Self { prefix }
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    fn key(&self, suffix: &str) -> String {
        format!("{}:{suffix}", self.prefix)
    }

    /// The stream of tasks that are due now.
    pub fn stream(&self) -> String {
        self.key("stream")
    }

    /// The sorted set of future tasks, scored by when they are due.
    pub fn queue(&self) -> String {
        self.key("queue")
    }

    /// The hash that holds a future task's message until it is due.
    pub fn parked(&self, key: &str) -> String {
        self.key(key)
    }

    /// The hash of a task's run state.
    pub fn runs(&self, key: &str) -> String {
        self.key(&format!("runs:{key}"))
    }

    pub fn runs_prefix(&self) -> String {
        self.key("runs:")
    }

    /// The hash of a task's progress, and the channel its progress events go to.
    pub fn progress(&self, key: &str) -> String {
        self.key(&format!("progress:{key}"))
    }

    /// A key that no current docket writes.  The scripts still declare it.
    pub fn known(&self, key: &str) -> String {
        self.key(&format!("known:{key}"))
    }

    /// A key that no current docket writes.  The scripts still declare it.
    pub fn stream_id(&self, key: &str) -> String {
        self.key(&format!("stream-id:{key}"))
    }

    /// The channel of a task's state events.
    pub fn state(&self, key: &str) -> String {
        self.key(&format!("state:{key}"))
    }

    pub fn state_prefix(&self) -> String {
        self.key("state:")
    }

    /// The channel that tells workers to stop a running task.
    pub fn cancel(&self, key: &str) -> String {
        self.key(&format!("cancel:{key}"))
    }

    pub fn cancel_pattern(&self) -> String {
        self.key("cancel:*")
    }

    /// The prefix that `stream_due_tasks.lua` and the wake scripts build
    /// parked keys from.
    pub fn parked_prefix(&self) -> String {
        self.key("")
    }

    /// The sorted set of workers, scored by their last heartbeat.
    pub fn workers(&self) -> String {
        self.key("workers")
    }

    /// The set of task names that a worker can run.
    pub fn worker_tasks(&self, worker: &str) -> String {
        self.key(&format!("worker-tasks:{worker}"))
    }

    /// The sorted set of workers that can run a task, scored by heartbeat.
    pub fn task_workers(&self, task: &str) -> String {
        self.key(&format!("task-workers:{task}"))
    }

    /// The lease that lets one worker at a time sweep for lost deliveries.
    pub fn sweep_lease(&self) -> String {
        self.key("leases:redelivery-sweep")
    }

    /// The lock that lets one worker at a time seed automatic perpetual tasks.
    pub fn perpetual_lock(&self) -> String {
        self.key("perpetual:lock")
    }

    /// The stream of strike and restore instructions.
    pub fn strikes(&self) -> String {
        self.key("strikes")
    }

    /// A task's stored result.
    pub fn result(&self, key: &str) -> String {
        self.key(&format!("results:{key}"))
    }

    /// A concurrency limit's sorted set of slots.  `scope` replaces nothing:
    /// it adds a segment, so limits with different scopes never share slots.
    pub fn concurrency(&self, scope: Option<&str>, tag: &str) -> String {
        match scope {
            Some(scope) => self.key(&format!("{scope}:concurrency:{tag}")),
            None => self.key(&format!("concurrency:{tag}")),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Keys;

    #[test]
    fn every_key_starts_with_the_prefix() {
        let keys = Keys::new("{orders}".to_owned());
        assert_eq!(keys.prefix(), "{orders}");
        let all = [
            (keys.stream(), "{orders}:stream"),
            (keys.queue(), "{orders}:queue"),
            (keys.parked("k"), "{orders}:k"),
            (keys.runs("k"), "{orders}:runs:k"),
            (keys.runs_prefix(), "{orders}:runs:"),
            (keys.progress("k"), "{orders}:progress:k"),
            (keys.known("k"), "{orders}:known:k"),
            (keys.stream_id("k"), "{orders}:stream-id:k"),
            (keys.state("k"), "{orders}:state:k"),
            (keys.state_prefix(), "{orders}:state:"),
            (keys.cancel("k"), "{orders}:cancel:k"),
            (keys.cancel_pattern(), "{orders}:cancel:*"),
            (keys.parked_prefix(), "{orders}:"),
            (keys.workers(), "{orders}:workers"),
            (keys.worker_tasks("w"), "{orders}:worker-tasks:w"),
            (keys.task_workers("t"), "{orders}:task-workers:t"),
            (keys.sweep_lease(), "{orders}:leases:redelivery-sweep"),
            (keys.perpetual_lock(), "{orders}:perpetual:lock"),
            (keys.strikes(), "{orders}:strikes"),
            (keys.result("k"), "{orders}:results:k"),
            (keys.concurrency(None, "t"), "{orders}:concurrency:t"),
            (keys.concurrency(Some("s"), "t"), "{orders}:s:concurrency:t"),
        ];
        for (key, expected) in all {
            assert_eq!(key, expected);
        }
    }
}

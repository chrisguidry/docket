//! The shared Lua scripts in `protocol/`, each declared as a typed struct.
//!
//! A declaration names every `KEYS` and `ARGV` slot in order, with its kind,
//! and the test below checks it against the script's header.  Every language
//! binds the slots the way the header says, so a declaration that drifts from
//! the header would send values to the wrong slots.

#![expect(dead_code, reason = "the docket and worker modules call these")]

use std::sync::LazyLock;

use redis::aio::ConnectionLike;
use redis::{Cmd, FromRedisValue, Pipeline, RedisResult, Script};

/// How a script's header decodes one `ARGV` slot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Kind {
    /// The string as it is.
    Text,
    /// `tonumber(...)`, sent as a decimal.
    Number,
    /// `tonumber(...)`, sent as a whole number.
    Integer,
    /// `... == '1'`.
    Flag,
}

/// A script's slots, in the order of its header.
pub(crate) struct Slots {
    pub keys: &'static [&'static str],
    pub args: &'static [(&'static str, Kind)],
    /// The variadic field and value pairs that fill the rest of `ARGV`.
    pub rest: Option<&'static str>,
}

impl Slots {
    /// The header that the script must start with.
    pub fn header(&self) -> String {
        let mut lines: Vec<String> = Vec::new();
        for (index, key) in self.keys.iter().enumerate() {
            lines.push(format!("local {key} = KEYS[{}]", index + 1));
        }
        for (index, (name, kind)) in self.args.iter().enumerate() {
            let slot = format!("ARGV[{}]", index + 1);
            lines.push(match kind {
                Kind::Text => format!("local {name} = {slot}"),
                Kind::Number | Kind::Integer => format!("local {name} = tonumber({slot})"),
                Kind::Flag => format!("local {name} = {slot} == '1'"),
            });
        }
        if let Some(rest) = self.rest {
            lines.push(format!("local {rest}_start = {}", self.args.len() + 1));
        }
        lines.join("\n")
    }
}

/// One script call, ready to run alone or to queue on a pipeline.
pub(crate) struct Call {
    script: &'static LazyLock<Script>,
    source: &'static str,
    keys: Vec<Vec<u8>>,
    args: Vec<Vec<u8>>,
}

impl Call {
    fn command(&self, name: &str, script: &str) -> Cmd {
        let mut command = redis::cmd(name);
        command
            .arg(script)
            .arg(self.keys.len())
            .arg(&self.keys)
            .arg(&self.args);
        command
    }

    /// Runs the script, loading it into Redis first if Redis does not have it.
    pub async fn run<T: FromRedisValue, C: ConnectionLike + Send>(
        &self,
        connection: &mut C,
    ) -> RedisResult<T> {
        let mut invocation = self.script.prepare_invoke();
        for key in &self.keys {
            invocation.key(key);
        }
        for arg in &self.args {
            invocation.arg(arg);
        }
        invocation.invoke_async(connection).await
    }

    /// Queues an `EVALSHA` on a pipeline.  The caller loads the script
    /// first, because a pipelined `EVALSHA` cannot reload it.
    pub fn queue(&self, pipeline: &mut Pipeline) {
        pipeline.add_command(self.command("EVALSHA", self.script.get_hash()));
    }

    /// Queues an `EVAL` with the full source, for servers whose script cache
    /// cannot be relied on, such as the nodes of a cluster.
    pub fn queue_eval(&self, pipeline: &mut Pipeline) {
        pipeline.add_command(self.command("EVAL", self.source));
    }

    /// Loads the script into Redis.
    pub async fn load<C: ConnectionLike + Send>(&self, connection: &mut C) -> RedisResult<()> {
        self.script.load_async(connection).await.map(|_| ())
    }
}

/// The text of one argument, as the header decodes it.
pub(crate) trait Encode {
    fn encode(&self) -> Vec<u8>;
}

impl Encode for String {
    fn encode(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }
}

impl Encode for f64 {
    fn encode(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

impl Encode for i64 {
    fn encode(&self) -> Vec<u8> {
        self.to_string().into_bytes()
    }
}

impl Encode for bool {
    fn encode(&self) -> Vec<u8> {
        if *self { b"1".to_vec() } else { b"0".to_vec() }
    }
}

macro_rules! kind_type {
    (Text) => {
        String
    };
    (Number) => {
        f64
    };
    (Integer) => {
        i64
    };
    (Flag) => {
        bool
    };
}

macro_rules! scripts {
    ($(
        $(#[$doc:meta])*
        $name:ident = $file:literal {
            keys: [$($key:ident),* $(,)?],
            args: [$($arg:ident: $kind:ident),* $(,)?]
            $(, rest: $rest:ident)?
            $(,)?
        }
    )*) => {
        $(
            $(#[$doc])*
            pub(crate) struct $name {
                $(pub $key: String,)*
                $(pub $arg: kind_type!($kind),)*
                $(pub $rest: Vec<(String, Vec<u8>)>,)?
            }

            impl $name {
                pub const FILE: &'static str = $file;
                pub const SLOTS: Slots = Slots {
                    keys: &[$(stringify!($key)),*],
                    args: &[$((stringify!($arg), Kind::$kind)),*],
                    rest: scripts!(@rest $($rest)?),
                };

                const SOURCE: &'static str = include_str!(concat!("../lua/", $file, ".lua"));

                fn script() -> &'static LazyLock<Script> {
                    static SCRIPT: LazyLock<Script> = LazyLock::new(|| Script::new($name::SOURCE));
                    &SCRIPT
                }

                pub fn call(&self) -> Call {
                    #[allow(unused_mut)]
                    let mut args: Vec<Vec<u8>> = vec![$(Encode::encode(&self.$arg)),*];
                    $(for (field, value) in &self.$rest {
                        args.push(field.as_bytes().to_vec());
                        args.push(value.clone());
                    })?
                    Call {
                        script: Self::script(),
                        source: Self::SOURCE,
                        keys: vec![$(self.$key.as_bytes().to_vec()),*],
                        args,
                    }
                }
            }
        )*

        /// Every declared script, for the header check.
        #[cfg(test)]
        pub(crate) const ALL: &[(&str, Slots, &str)] = &[
            $(($file, $name::SLOTS, $name::SOURCE)),*
        ];
    };
    (@rest $rest:ident) => { Some(stringify!($rest)) };
    (@rest) => { None };
}

scripts! {
    /// Takes a concurrency slot, or parks the task as a waiter.
    AcquireOrPark = "acquire_or_park" {
        keys: [slots_key, waiters_stream, stream_key, runs_key],
        args: [
            max_concurrent: Integer,
            task_key: Text,
            current_time: Number,
            is_redelivery: Flag,
            stale_threshold: Number,
            key_ttl: Integer,
            message_id: Text,
            worker_group_name: Text,
            state_channel: Text,
            state_payload: Text,
        ],
        rest: message,
    }

    /// Removes a cancelled task's waiter entry and progress.
    CancelCleanup = "cancel_cleanup" {
        keys: [waiters_stream, progress_key, runs_key],
        args: [waiter_entry_id: Text],
    }

    /// Cancels a scheduled or queued task.
    CancelTask = "cancel_task" {
        keys: [
            stream_key,
            known_key,
            parked_key,
            queue_key,
            stream_id_key,
            runs_key,
            progress_key,
            state_channel,
        ],
        args: [
            task_key: Text,
            completed_at: Text,
            state_payload: Text,
            expected_generation: Integer,
        ],
    }

    /// Cancels every task that has not started, and empties the stream and
    /// the queue.
    Clear = "clear" {
        keys: [stream_key, queue_key],
        args: [docket_prefix: Text, completed_at: Text, ttl_seconds: Integer],
    }

    /// Marks a delivered task as running on a worker.
    Claim = "claim" {
        keys: [runs_key, progress_key, known_key, stream_id_key, state_channel, stream_key],
        args: [
            worker: Text,
            started_at: Text,
            generation: Integer,
            state_payload: Text,
            key_json: Text,
            worker_group_name: Text,
            message_id: Text,
        ],
    }

    /// Decides whether a debounced call wins its window.
    Debounce = "debounce" {
        keys: [winner_key, seen_key],
        args: [execution_key: Text, settle_ms: Integer, now_ms: Integer, ttl_ms: Integer],
    }

    /// Writes a task's progress fields.
    ProgressWrite = "progress_write" {
        keys: [progress_key],
        args: [payload: Text, clear_message: Flag],
        rest: fields,
    }

    /// Counts a call against a sliding rate limit window.
    RateLimit = "ratelimit" {
        keys: [ratelimit_key],
        args: [member: Text, now_ms: Integer, window_ms: Integer, limit: Integer, ttl_ms: Integer],
    }

    /// Extends a lease that this holder owns.
    RefreshLease = "refresh_lease" {
        keys: [lease_key],
        args: [holder: Text, duration_ms: Integer],
    }

    /// Gives up a concurrency slot and wakes the next waiter.
    ReleaseAndWake = "release_and_wake" {
        keys: [slots_key, waiters_stream, stream_key, queue_key],
        args: [
            task_key: Text,
            max_concurrent: Integer,
            stale_threshold: Number,
            runs_prefix: Text,
            state_prefix: Text,
            parked_prefix: Text,
        ],
    }

    /// Frees stale concurrency slots and wakes waiters for them.
    ScavengeAndWake = "scavenge_and_wake" {
        keys: [slots_key, waiters_stream, stream_key, queue_key],
        args: [
            max_concurrent: Integer,
            stale_threshold: Number,
            runs_prefix: Text,
            state_prefix: Text,
            parked_prefix: Text,
        ],
    }

    /// Schedules a task now or later, or replaces or reschedules one.
    Schedule = "schedule" {
        keys: [
            stream_key,
            known_key,
            parked_key,
            queue_key,
            stream_id_key,
            runs_key,
            state_channel,
        ],
        args: [
            task_key: Text,
            when_timestamp: Number,
            is_immediate: Flag,
            replace: Flag,
            reschedule_message_id: Text,
            expected_generation: Integer,
            worker_group_name: Text,
            state_payload: Text,
        ],
        rest: message,
    }

    /// Moves due tasks from the queue to the stream.
    StreamDueTasks = "stream_due_tasks" {
        keys: [queue_key, stream_key],
        args: [now_timestamp: Number, docket_prefix: Text],
    }

    /// Records a task's terminal state and acknowledges its message.
    Terminal = "terminal" {
        keys: [runs_key, state_channel, progress_key, stream_key],
        args: [
            generation: Integer,
            state: Text,
            completed_at: Text,
            ttl_seconds: Integer,
            state_payload: Text,
            worker_group_name: Text,
            message_id: Text,
        ],
        rest: extra_fields,
    }
}

#[cfg(test)]
mod tests;

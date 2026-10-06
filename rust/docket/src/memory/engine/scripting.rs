use bytes::Bytes;
use mlua::prelude::*;
use ordered_float::OrderedFloat;
use sha1::{Digest, Sha1};
use std::cell::RefCell;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::broadcast;

use crate::memory::engine::store::{PubSubMessage, ValueData, ValueEntry};
use std::collections::VecDeque;

/// Represents a Redis command return value for Lua-Redis type conversion.
#[derive(Debug, Clone)]
pub enum RedisValue {
    BulkString(Bytes),
    Integer(i64),
    Array(Vec<RedisValue>),
    Nil,
    Error(String),
    Status(String),
}

impl IntoLua for RedisValue {
    fn into_lua(self, lua: &Lua) -> LuaResult<LuaValue> {
        match self {
            RedisValue::BulkString(b) => {
                let s = lua.create_string(b.as_ref())?;
                Ok(LuaValue::String(s))
            }
            RedisValue::Integer(n) => Ok(LuaValue::Integer(n)),
            RedisValue::Array(items) => {
                let table = lua.create_table()?;
                for (i, item) in items.into_iter().enumerate() {
                    table.set(i + 1, item.into_lua(lua)?)?;
                }
                Ok(LuaValue::Table(table))
            }
            RedisValue::Nil => Ok(LuaValue::Boolean(false)),
            RedisValue::Error(msg) => {
                // Errors should be handled by the caller (redis.call raises, redis.pcall wraps)
                // If we get here, wrap as a table with err field
                let table = lua.create_table()?;
                table.set("err", msg)?;
                Ok(LuaValue::Table(table))
            }
            RedisValue::Status(s) => {
                let table = lua.create_table()?;
                table.set("ok", s)?;
                Ok(LuaValue::Table(table))
            }
        }
    }
}

/// Convert a Lua value back to a RedisValue.
fn lua_to_redis_value(val: LuaValue) -> RedisValue {
    match val {
        LuaValue::String(s) => RedisValue::BulkString(Bytes::from(s.as_bytes().to_vec())),
        LuaValue::Integer(n) => RedisValue::Integer(n),
        LuaValue::Number(n) => RedisValue::Integer(n as i64),
        LuaValue::Boolean(true) => RedisValue::Integer(1),
        LuaValue::Boolean(false) => RedisValue::Nil,
        LuaValue::Nil => RedisValue::Nil,
        LuaValue::Table(table) => {
            // Check for err key first
            if let Ok(err) = table.get::<LuaValue>("err".to_string()) {
                if let LuaValue::String(s) = err {
                    return RedisValue::Error(String::from_utf8_lossy(&s.as_bytes()).to_string());
                }
            }
            // Check for ok key
            if let Ok(ok) = table.get::<LuaValue>("ok".to_string()) {
                if let LuaValue::String(s) = ok {
                    return RedisValue::Status(String::from_utf8_lossy(&s.as_bytes()).to_string());
                }
            }
            // Treat as array: iterate sequential integer keys 1..n
            let mut arr = Vec::new();
            let mut i = 1;
            loop {
                match table.get::<LuaValue>(i) {
                    Ok(LuaValue::Nil) => break,
                    Ok(v) => arr.push(lua_to_redis_value(v)),
                    Err(_) => break,
                }
                i += 1;
            }
            RedisValue::Array(arr)
        }
        _ => RedisValue::Nil,
    }
}

pub struct LuaEngine;

/// Outcome of a Lua script execution.
///
/// `result` carries the script's return value (or error string). `had_xadd` and
/// `had_list_mutation` are *cumulative* flags: they reflect whether any
/// `XADD` / list-grow command ran successfully BEFORE the script ended,
/// regardless of whether the script ultimately succeeded or raised.
///
/// Real Redis does not roll back earlier writes when a script errors mid-way,
/// so any waiters parked on those mutations (BRPOP / XREAD) must still be
/// woken up. Callers (`Store::eval`/`Store::evalsha`) MUST consult these
/// flags on both the `Ok` and `Err` branches of `result`. (P2-08)
pub struct EvalOutcome {
    pub result: Result<RedisValue, String>,
    pub had_xadd: bool,
    pub had_list_mutation: bool,
}

impl LuaEngine {
    /// Compute SHA1 hex digest of a script.
    pub fn sha1_hex(script: &str) -> String {
        let mut hasher = Sha1::new();
        hasher.update(script.as_bytes());
        let result = hasher.finalize();
        result.iter().map(|byte| format!("{byte:02x}")).collect()
    }

    /// Execute a Lua script with access to KEYS, ARGV, and redis.call()/redis.pcall().
    ///
    /// `data` is the ALREADY WRITE-LOCKED store data HashMap -- the caller (Store::eval)
    /// acquires the write lock and passes the mutable reference. This ensures atomicity.
    /// `pubsub_tx` is an optional broadcast sender for PUBLISH command support in Lua scripts.
    ///
    /// Returns an [`EvalOutcome`] containing the script result alongside the
    /// `had_xadd` / `had_list_mutation` flags. The flags are populated even
    /// when the script raises mid-way — this is the P2-08 fix: a script that
    /// pushed onto a list and THEN errored has still grown the list, and any
    /// `BRPOP` / `BLPOP` waiters parked on that key must be woken regardless
    /// of the script's outcome.
    pub fn execute(
        script: &str,
        keys: Vec<Bytes>,
        args: Vec<Bytes>,
        data: &mut HashMap<Bytes, ValueEntry>,
        pubsub_tx: Option<&broadcast::Sender<PubSubMessage>>,
    ) -> EvalOutcome {
        // Create a fresh Lua VM per execution (isolation, no state leakage)
        let lua = Lua::new();

        // Use RefCell to allow mutable access from within Lua callbacks
        let data_cell = RefCell::new(data);
        // Clone the broadcast sender for use inside Lua closures
        let pubsub_tx_clone = pubsub_tx.cloned();
        // Track if any XADD occurred during script execution
        let had_xadd = std::cell::Cell::new(false);
        // Track if any list-growing mutation (LPUSH/RPUSH/LMOVE/RPOPLPUSH/LINSERT) occurred.
        // Used by Store::eval/evalsha to fire list_notify.notify_waiters() after dropping
        // the data lock — the Phase-11-class-of-bug fix for BRPOP waiters missing a Lua LPUSH.
        let had_list_mutation = std::cell::Cell::new(false);

        let scope_result: LuaResult<RedisValue> = lua.scope(|scope| {
            // Set up KEYS global
            let keys_table = lua.create_table()?;
            for (i, key) in keys.iter().enumerate() {
                keys_table.set(i + 1, lua.create_string(key.as_ref())?)?;
            }
            lua.globals().set("KEYS", keys_table)?;

            // Set up ARGV global
            let argv_table = lua.create_table()?;
            for (i, arg) in args.iter().enumerate() {
                argv_table.set(i + 1, lua.create_string(arg.as_ref())?)?;
            }
            lua.globals().set("ARGV", argv_table)?;

            // Create redis table with call and pcall
            let redis_table = lua.create_table()?;

            // redis.call() - raises Lua error on command failure
            let call_fn = scope
                .create_function_mut(|lua_ctx, args: LuaMultiValue| {
                    let args_vec: Vec<LuaValue> = args.into_vec();
                    if args_vec.is_empty() {
                        return Err(LuaError::RuntimeError(
                            "ERR wrong number of arguments for 'redis.call'".to_string(),
                        ));
                    }

                    // First arg is command name
                    let cmd_name = match &args_vec[0] {
                        LuaValue::String(s) => String::from_utf8_lossy(&s.as_bytes()).to_uppercase(),
                        _ => {
                            return Err(LuaError::RuntimeError(
                                "ERR first argument must be a string".to_string(),
                            ))
                        }
                    };

                    // Remaining args are command arguments
                    let cmd_args: Vec<Bytes> = args_vec[1..]
                        .iter()
                        .map(|v| match v {
                            LuaValue::String(s) => Bytes::from(s.as_bytes().to_vec()),
                            LuaValue::Integer(n) => Bytes::from(n.to_string().into_bytes()),
                            LuaValue::Number(n) => Bytes::from(n.to_string().into_bytes()),
                            _ => Bytes::new(),
                        })
                        .collect();

                    let mut data_ref = data_cell.borrow_mut();
                    let pubsub_tx_ref = pubsub_tx_clone.as_ref();
                    let result = dispatch_command(&cmd_name, &cmd_args, *data_ref, pubsub_tx_ref);

                    match result {
                        Ok((RedisValue::Error(msg), _, _)) => Err(LuaError::RuntimeError(msg)),
                        Ok((val, xadd_flag, list_mut_flag)) => {
                            if xadd_flag { had_xadd.set(true); }
                            if list_mut_flag { had_list_mutation.set(true); }
                            val.into_lua(lua_ctx)
                        },
                        Err(msg) => Err(LuaError::RuntimeError(msg)),
                    }
                })?;

            redis_table.set("call", call_fn)?;

            // redis.pcall() - returns error table instead of raising
            let pcall_fn = scope
                .create_function_mut(|lua_ctx, args: LuaMultiValue| {
                    let args_vec: Vec<LuaValue> = args.into_vec();
                    if args_vec.is_empty() {
                        let table = lua_ctx.create_table()?;
                        table.set("err", "ERR wrong number of arguments for 'redis.pcall'")?;
                        return Ok(LuaValue::Table(table));
                    }

                    // First arg is command name
                    let cmd_name = match &args_vec[0] {
                        LuaValue::String(s) => String::from_utf8_lossy(&s.as_bytes()).to_uppercase(),
                        _ => {
                            let table = lua_ctx.create_table()?;
                            table.set("err", "ERR first argument must be a string")?;
                            return Ok(LuaValue::Table(table));
                        }
                    };

                    // Remaining args are command arguments
                    let cmd_args: Vec<Bytes> = args_vec[1..]
                        .iter()
                        .map(|v| match v {
                            LuaValue::String(s) => Bytes::from(s.as_bytes().to_vec()),
                            LuaValue::Integer(n) => Bytes::from(n.to_string().into_bytes()),
                            LuaValue::Number(n) => Bytes::from(n.to_string().into_bytes()),
                            _ => Bytes::new(),
                        })
                        .collect();

                    let mut data_ref = data_cell.borrow_mut();
                    let pubsub_tx_ref = pubsub_tx_clone.as_ref();
                    let result = dispatch_command(&cmd_name, &cmd_args, *data_ref, pubsub_tx_ref);

                    match result {
                        Ok((RedisValue::Error(msg), _, _)) => {
                            let table = lua_ctx.create_table()?;
                            table.set("err", msg)?;
                            Ok(LuaValue::Table(table))
                        }
                        Ok((val, xadd_flag, list_mut_flag)) => {
                            if xadd_flag { had_xadd.set(true); }
                            if list_mut_flag { had_list_mutation.set(true); }
                            val.into_lua(lua_ctx)
                        },
                        Err(msg) => {
                            let table = lua_ctx.create_table()?;
                            table.set("err", msg)?;
                            Ok(LuaValue::Table(table))
                        }
                    }
                })?;

            redis_table.set("pcall", pcall_fn)?;

            lua.globals().set("redis", redis_table)?;

            // Lua 5.4 compatibility: Redis uses Lua 5.1 where `unpack` is a global.
            // In Lua 5.4, it was moved to `table.unpack`. Provide the global alias.
            lua.load("unpack = unpack or table.unpack").exec()?;

            // Execute the script and capture return value
            let result: LuaValue = lua.load(script).eval()?;

            Ok(lua_to_redis_value(result))
        });

        // P2-08: capture the mutation flags BEFORE consuming `scope_result`.
        // Real Redis does not roll back writes inside a script that errors
        // mid-way, so any pushes/XADDs that already ran must wake their
        // waiters whether the script returns normally or raises.
        EvalOutcome {
            result: scope_result.map_err(|e| e.to_string()),
            had_xadd: had_xadd.get(),
            had_list_mutation: had_list_mutation.get(),
        }
    }
}

/// Dispatch a Redis command to the appropriate operation on the raw data HashMap.
/// This operates directly on the write-locked data for atomicity during Lua execution.
/// `pubsub_tx` is an optional broadcast sender for PUBLISH support in Lua scripts.
/// Returns (RedisValue, had_xadd, had_list_mutation) where the flags indicate whether
/// a successful XADD / list-growing mutation occurred (so callers can fire the matching
/// notify_waiters() outside the data lock).
///
/// Per RESEARCH.md Assumptions Log A2: only list-GROW operations mark had_list_mutation.
/// LINSERT is included because it can grow a non-empty list (pivot match inserts a new
/// element mid-list, increasing length). LPOP/RPOP/LREM/LTRIM/LSET never grow a list.
///
/// M-04: refined per-command — LINSERT triggers only when it actually inserted
/// (return value > 0; -1 = pivot not found, 0 = key missing → no mutation),
/// and LMOVE/RPOPLPUSH trigger only when they actually moved an element
/// (BulkString result; Nil = empty source → no mutation). LPUSH/RPUSH always
/// grow the list on a successful Integer return. This avoids spurious wakes
/// of BLPOP/BRPOP waiters in the case where the script command was a no-op.
fn dispatch_command(
    cmd: &str,
    args: &[Bytes],
    data: &mut HashMap<Bytes, ValueEntry>,
    pubsub_tx: Option<&broadcast::Sender<PubSubMessage>>,
) -> Result<(RedisValue, bool, bool), String> {
    let is_xadd = cmd == "XADD";
    let result = dispatch_command_inner(cmd, args, data, pubsub_tx)?;
    let success = !matches!(result, RedisValue::Error(_));
    let had_xadd = is_xadd && success;
    let had_list_mutation = match cmd {
        // LPUSH/RPUSH always grow the destination on a successful Integer
        // return. (Errors short-circuit above; non-Integer returns would be
        // a bug in dispatch_command_inner.)
        "LPUSH" | "RPUSH" => matches!(result, RedisValue::Integer(_)),
        // LINSERT returns the new list length on success, -1 if pivot not
        // found, 0 if the key is missing. Only positive values represent an
        // actual insertion.
        "LINSERT" => matches!(result, RedisValue::Integer(n) if n > 0),
        // LMOVE/RPOPLPUSH return the moved element as BulkString on success,
        // Nil if the source was empty/missing.
        "LMOVE" | "RPOPLPUSH" => matches!(result, RedisValue::BulkString(_)),
        _ => false,
    };
    Ok((result, had_xadd, had_list_mutation))
}

/// Inner dispatch that returns just the RedisValue.
fn dispatch_command_inner(
    cmd: &str,
    args: &[Bytes],
    data: &mut HashMap<Bytes, ValueEntry>,
    pubsub_tx: Option<&broadcast::Sender<PubSubMessage>>,
) -> Result<RedisValue, String> {
    match cmd {
        // ── String commands ──────────────────────────────────────────
        "GET" => {
            if args.len() != 1 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'get' command".to_string(),
                ));
            }
            let key = &args[0];
            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Nil);
                }
            }
            match data.get(key) {
                None => Ok(RedisValue::Nil),
                Some(entry) => match &entry.data {
                    ValueData::String(v) => Ok(RedisValue::BulkString(v.clone())),
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "SET" => {
            if args.len() < 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'set' command".to_string(),
                ));
            }
            let key = args[0].clone();
            let value = args[1].clone();
            let mut nx = false;
            let mut xx = false;
            let mut ttl: Option<Duration> = None;

            // Parse optional flags from args[2..]
            let mut i = 2;
            while i < args.len() {
                let flag = String::from_utf8_lossy(&args[i]).to_uppercase();
                match flag.as_str() {
                    "NX" => {
                        nx = true;
                        i += 1;
                    }
                    "XX" => {
                        xx = true;
                        i += 1;
                    }
                    "EX" => {
                        i += 1;
                        if i >= args.len() {
                            return Ok(RedisValue::Error(
                                "ERR syntax error".to_string(),
                            ));
                        }
                        let secs: u64 = String::from_utf8_lossy(&args[i])
                            .parse()
                            .map_err(|_| "ERR value is not an integer or out of range".to_string())?;
                        ttl = Some(Duration::from_secs(secs));
                        i += 1;
                    }
                    "PX" => {
                        i += 1;
                        if i >= args.len() {
                            return Ok(RedisValue::Error(
                                "ERR syntax error".to_string(),
                            ));
                        }
                        let ms: u64 = String::from_utf8_lossy(&args[i])
                            .parse()
                            .map_err(|_| "ERR value is not an integer or out of range".to_string())?;
                        ttl = Some(Duration::from_millis(ms));
                        i += 1;
                    }
                    _ => {
                        i += 1;
                    }
                }
            }

            // Check NX/XX conditions
            let key_exists = data
                .get(&key)
                .map(|e| !e.is_expired())
                .unwrap_or(false);

            if nx && key_exists {
                return Ok(RedisValue::Nil);
            }
            if xx && !key_exists {
                return Ok(RedisValue::Nil);
            }

            // Remove expired key if present
            if let Some(entry) = data.get(&key) {
                if entry.is_expired() {
                    data.remove(&key);
                }
            }

            let entry = ValueEntry::new(value, ttl);
            data.insert(key, entry);
            Ok(RedisValue::Status("OK".to_string()))
        }

        "DEL" => {
            if args.is_empty() {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'del' command".to_string(),
                ));
            }
            let mut count = 0i64;
            for key in args {
                if let Some(entry) = data.get(key) {
                    if !entry.is_expired() {
                        count += 1;
                    }
                }
                data.remove(key);
            }
            Ok(RedisValue::Integer(count))
        }

        "EXISTS" => {
            if args.is_empty() {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'exists' command".to_string(),
                ));
            }
            let mut count = 0i64;
            for key in args {
                // Passive expiration
                if let Some(entry) = data.get(key) {
                    if entry.is_expired() {
                        data.remove(key);
                        continue;
                    }
                    count += 1;
                }
            }
            Ok(RedisValue::Integer(count))
        }

        // ── Hash commands ────────────────────────────────────────────
        "HSET" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'hset' command".to_string(),
                ));
            }
            let key = args[0].clone();

            // Passive expiration
            if let Some(entry) = data.get(&key) {
                if entry.is_expired() {
                    data.remove(&key);
                }
            }

            let entry = data.entry(key).or_insert_with(ValueEntry::new_hash);
            match entry.data {
                ValueData::Hash(ref mut map) => {
                    let mut new_count = 0i64;
                    let mut i = 1;
                    while i < args.len() {
                        let field = args[i].clone();
                        let value = args[i + 1].clone();
                        if !map.contains_key(&field) {
                            new_count += 1;
                        }
                        map.insert(field, value);
                        i += 2;
                    }
                    Ok(RedisValue::Integer(new_count))
                }
                _ => Ok(RedisValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                )),
            }
        }

        "HGET" => {
            if args.len() != 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'hget' command".to_string(),
                ));
            }
            let key = &args[0];
            let field = &args[1];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Nil);
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Nil),
                Some(entry) => match &entry.data {
                    ValueData::Hash(map) => match map.get(field) {
                        Some(v) => Ok(RedisValue::BulkString(v.clone())),
                        None => Ok(RedisValue::Nil),
                    },
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "HDEL" => {
            if args.len() < 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'hdel' command".to_string(),
                ));
            }
            let key = &args[0];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get_mut(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => match entry.data {
                    ValueData::Hash(ref mut map) => {
                        let mut count = 0i64;
                        for field in &args[1..] {
                            if map.remove(field).is_some() {
                                count += 1;
                            }
                        }
                        Ok(RedisValue::Integer(count))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "HVALS" => {
            if args.len() != 1 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'hvals' command".to_string(),
                ));
            }
            let key = &args[0];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Array(Vec::new()));
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Array(Vec::new())),
                Some(entry) => match &entry.data {
                    ValueData::Hash(map) => {
                        let vals: Vec<RedisValue> = map
                            .values()
                            .map(|v| RedisValue::BulkString(v.clone()))
                            .collect();
                        Ok(RedisValue::Array(vals))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "HGETALL" => {
            if args.len() != 1 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'hgetall' command".to_string(),
                ));
            }
            let key = &args[0];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Array(Vec::new()));
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Array(Vec::new())),
                Some(entry) => match &entry.data {
                    ValueData::Hash(map) => {
                        // Return alternating field/value list (Redis wire format for Lua)
                        let mut result = Vec::new();
                        for (field, value) in map {
                            result.push(RedisValue::BulkString(field.clone()));
                            result.push(RedisValue::BulkString(value.clone()));
                        }
                        Ok(RedisValue::Array(result))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "HEXISTS" => {
            if args.len() != 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'hexists' command".to_string(),
                ));
            }
            let key = &args[0];
            let field = &args[1];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => match &entry.data {
                    ValueData::Hash(map) => {
                        if map.contains_key(field) {
                            Ok(RedisValue::Integer(1))
                        } else {
                            Ok(RedisValue::Integer(0))
                        }
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "HINCRBY" => {
            if args.len() != 3 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'hincrby' command".to_string(),
                ));
            }
            let key = args[0].clone();
            let field = &args[1];
            let increment: i64 = String::from_utf8_lossy(&args[2])
                .parse()
                .map_err(|_| "ERR value is not an integer or out of range".to_string())?;

            // Passive expiration
            if let Some(entry) = data.get(&key) {
                if entry.is_expired() {
                    data.remove(&key);
                }
            }

            let entry = data.entry(key).or_insert_with(ValueEntry::new_hash);
            match entry.data {
                ValueData::Hash(ref mut map) => {
                    let current = map
                        .get(field)
                        .and_then(|v| String::from_utf8_lossy(v).parse::<i64>().ok())
                        .unwrap_or(0);
                    let new_val = current + increment;
                    map.insert(field.clone(), Bytes::from(new_val.to_string()));
                    Ok(RedisValue::Integer(new_val))
                }
                _ => Ok(RedisValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                )),
            }
        }

        // ── Set commands ─────────────────────────────────────────────
        "SADD" => {
            if args.len() < 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'sadd' command".to_string(),
                ));
            }
            let key = args[0].clone();

            // Passive expiration
            if let Some(entry) = data.get(&key) {
                if entry.is_expired() {
                    data.remove(&key);
                }
            }

            let entry = data.entry(key).or_insert_with(ValueEntry::new_set);
            match entry.data {
                ValueData::Set(ref mut set) => {
                    let mut new_count = 0i64;
                    for member in &args[1..] {
                        if set.insert(member.clone()) {
                            new_count += 1;
                        }
                    }
                    Ok(RedisValue::Integer(new_count))
                }
                _ => Ok(RedisValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                )),
            }
        }

        "SMEMBERS" => {
            if args.len() != 1 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'smembers' command".to_string(),
                ));
            }
            let key = &args[0];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Array(Vec::new()));
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Array(Vec::new())),
                Some(entry) => match &entry.data {
                    ValueData::Set(set) => {
                        let members: Vec<RedisValue> = set
                            .iter()
                            .map(|m| RedisValue::BulkString(m.clone()))
                            .collect();
                        Ok(RedisValue::Array(members))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "SISMEMBER" => {
            if args.len() != 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'sismember' command".to_string(),
                ));
            }
            let key = &args[0];
            let member = &args[1];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => match &entry.data {
                    ValueData::Set(set) => {
                        if set.contains(member) {
                            Ok(RedisValue::Integer(1))
                        } else {
                            Ok(RedisValue::Integer(0))
                        }
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "SREM" => {
            if args.len() < 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'srem' command".to_string(),
                ));
            }
            let key = &args[0];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get_mut(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => match entry.data {
                    ValueData::Set(ref mut set) => {
                        let mut count = 0i64;
                        for member in &args[1..] {
                            if set.remove(member) {
                                count += 1;
                            }
                        }
                        Ok(RedisValue::Integer(count))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        // ── Sorted set commands ──────────────────────────────────────
        "ZADD" => {
            if args.len() < 3 || (args.len() - 1) % 2 != 0 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'zadd' command".to_string(),
                ));
            }
            let key = args[0].clone();

            // Passive expiration
            if let Some(entry) = data.get(&key) {
                if entry.is_expired() {
                    data.remove(&key);
                }
            }

            // Parse score-member pairs from args[1..]
            let entry = data
                .entry(key)
                .or_insert_with(ValueEntry::new_sorted_set);
            match entry.data {
                ValueData::SortedSet(ref mut zset) => {
                    let mut added = 0i64;
                    let mut i = 1;
                    while i + 1 < args.len() {
                        let score_str = String::from_utf8_lossy(&args[i]);
                        let score: f64 = score_str.parse().map_err(|_| {
                            "ERR value is not a valid float".to_string()
                        })?;
                        let member = args[i + 1].clone();
                        if zset.insert(member, score) {
                            added += 1;
                        }
                        i += 2;
                    }
                    Ok(RedisValue::Integer(added))
                }
                _ => Ok(RedisValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                )),
            }
        }

        "ZREM" => {
            if args.len() < 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'zrem' command".to_string(),
                ));
            }
            let key = &args[0];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get_mut(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => match entry.data {
                    ValueData::SortedSet(ref mut zset) => {
                        let mut count = 0i64;
                        for member in &args[1..] {
                            if zset.remove(member) {
                                count += 1;
                            }
                        }
                        Ok(RedisValue::Integer(count))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "ZRANGE" => {
            if args.len() < 3 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'zrange' command".to_string(),
                ));
            }
            let key = &args[0];
            let start: i64 = String::from_utf8_lossy(&args[1])
                .parse()
                .map_err(|_| "ERR value is not an integer or out of range".to_string())?;
            let stop: i64 = String::from_utf8_lossy(&args[2])
                .parse()
                .map_err(|_| "ERR value is not an integer or out of range".to_string())?;

            // Parse optional WITHSCORES flag
            let mut zrange_withscores = false;
            for i in 3..args.len() {
                if String::from_utf8_lossy(&args[i]).to_uppercase() == "WITHSCORES" {
                    zrange_withscores = true;
                }
            }

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Array(Vec::new()));
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Array(Vec::new())),
                Some(entry) => match &entry.data {
                    ValueData::SortedSet(zset) => {
                        let len = zset.len() as i64;
                        if len == 0 {
                            return Ok(RedisValue::Array(Vec::new()));
                        }

                        let mut real_start = if start < 0 { len + start } else { start };
                        let mut real_stop = if stop < 0 { len + stop } else { stop };

                        if real_start < 0 {
                            real_start = 0;
                        }
                        if real_stop >= len {
                            real_stop = len - 1;
                        }
                        if real_start > real_stop || real_start >= len {
                            return Ok(RedisValue::Array(Vec::new()));
                        }

                        let result: Vec<RedisValue> = zset
                            .by_score
                            .iter()
                            .skip(real_start as usize)
                            .take((real_stop - real_start + 1) as usize)
                            .flat_map(|((score, member), _)| {
                                if zrange_withscores {
                                    vec![
                                        RedisValue::BulkString(member.clone()),
                                        RedisValue::BulkString(Bytes::from(format_redis_score(score.0))),
                                    ]
                                } else {
                                    vec![RedisValue::BulkString(member.clone())]
                                }
                            })
                            .collect();

                        Ok(RedisValue::Array(result))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "ZRANGEBYSCORE" => {
            if args.len() < 3 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'zrangebyscore' command".to_string(),
                ));
            }
            let key = &args[0];
            let min = parse_score_arg(&args[1]);
            let max = parse_score_arg(&args[2]);

            // Parse optional WITHSCORES and LIMIT flags
            let mut zbs_withscores = false;
            let mut zbs_limit: Option<(usize, i64)> = None;
            let mut i = 3;
            while i < args.len() {
                let flag = String::from_utf8_lossy(&args[i]).to_uppercase();
                match flag.as_str() {
                    "WITHSCORES" => {
                        zbs_withscores = true;
                        i += 1;
                    }
                    "LIMIT" => {
                        if i + 2 < args.len() {
                            let offset: usize =
                                String::from_utf8_lossy(&args[i + 1]).parse().unwrap_or(0);
                            let count: i64 =
                                String::from_utf8_lossy(&args[i + 2]).parse().unwrap_or(-1);
                            zbs_limit = Some((offset, count));
                            i += 3;
                        } else {
                            i += 1;
                        }
                    }
                    _ => {
                        i += 1;
                    }
                }
            }

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Array(Vec::new()));
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Array(Vec::new())),
                Some(entry) => match &entry.data {
                    ValueData::SortedSet(zset) => {
                        let lower = min.lower_btree_bound();
                        let max_val = max.value();
                        let max_inclusive = max.is_inclusive();
                        let base: Vec<_> = zset
                            .by_score
                            .range((lower, std::ops::Bound::Unbounded))
                            // Skip members at the lower-bound score when it is exclusive
                            .skip_while(|((score, _), _)| {
                                !min.is_inclusive() && score.0 == min.value()
                            })
                            .take_while(|((score, _), _)| {
                                if max_inclusive {
                                    score.0 <= max_val
                                } else {
                                    score.0 < max_val
                                }
                            })
                            .collect();

                        // Apply LIMIT if present
                        let limited: Box<dyn Iterator<Item = &_>> = match zbs_limit {
                            Some((offset, count)) => {
                                if count < 0 {
                                    Box::new(base.iter().skip(offset))
                                } else {
                                    Box::new(base.iter().skip(offset).take(count as usize))
                                }
                            }
                            None => Box::new(base.iter()),
                        };

                        let result: Vec<RedisValue> = limited
                            .flat_map(|((score, member), _)| {
                                if zbs_withscores {
                                    vec![
                                        RedisValue::BulkString(member.clone()),
                                        RedisValue::BulkString(Bytes::from(
                                            format_redis_score(score.0),
                                        )),
                                    ]
                                } else {
                                    vec![RedisValue::BulkString(member.clone())]
                                }
                            })
                            .collect();
                        Ok(RedisValue::Array(result))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "ZREMRANGEBYSCORE" => {
            if args.len() < 3 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'zremrangebyscore' command".to_string(),
                ));
            }
            let key = &args[0];
            let min = parse_score_arg(&args[1]);
            let max = parse_score_arg(&args[2]);

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get_mut(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => match entry.data {
                    ValueData::SortedSet(ref mut zset) => {
                        let lower = min.lower_btree_bound();
                        let max_val = max.value();
                        let max_inclusive = max.is_inclusive();
                        let to_remove: Vec<Bytes> = zset
                            .by_score
                            .range((lower, std::ops::Bound::Unbounded))
                            // Skip members at the lower-bound score when it is exclusive
                            .skip_while(|((score, _), _)| {
                                !min.is_inclusive() && score.0 == min.value()
                            })
                            .take_while(|((score, _), _)| {
                                if max_inclusive {
                                    score.0 <= max_val
                                } else {
                                    score.0 < max_val
                                }
                            })
                            .map(|((_, member), _)| member.clone())
                            .collect();

                        let count = to_remove.len() as i64;
                        for member in &to_remove {
                            zset.remove(member);
                        }
                        Ok(RedisValue::Integer(count))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "ZCARD" => {
            if args.len() != 1 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'zcard' command".to_string(),
                ));
            }
            let key = &args[0];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => match &entry.data {
                    ValueData::SortedSet(zset) => Ok(RedisValue::Integer(zset.len() as i64)),
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "ZSCORE" => {
            if args.len() != 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'zscore' command".to_string(),
                ));
            }
            let key = &args[0];
            let member = &args[1];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Nil);
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Nil),
                Some(entry) => match &entry.data {
                    ValueData::SortedSet(zset) => match zset.by_member.get(member) {
                        Some(&score) => Ok(RedisValue::BulkString(Bytes::from(score.to_string()))),
                        None => Ok(RedisValue::Nil),
                    },
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        // ── Key commands ────────────────────────────────────────────
        "EXPIRE" => {
            if args.len() != 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'expire' command".to_string(),
                ));
            }
            let key = &args[0];
            let seconds: u64 = String::from_utf8_lossy(&args[1])
                .parse()
                .map_err(|_| "ERR value is not an integer or out of range".to_string())?;

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get_mut(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => {
                    entry.expires_at =
                        Some(std::time::Instant::now() + Duration::from_secs(seconds));
                    Ok(RedisValue::Integer(1))
                }
            }
        }

        "PEXPIRE" => {
            if args.len() != 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'pexpire' command".to_string(),
                ));
            }
            let key = &args[0];
            let ms: u64 = String::from_utf8_lossy(&args[1])
                .parse()
                .map_err(|_| "ERR value is not an integer or out of range".to_string())?;

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get_mut(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => {
                    entry.expires_at =
                        Some(std::time::Instant::now() + Duration::from_millis(ms));
                    Ok(RedisValue::Integer(1))
                }
            }
        }

        // ── Stream commands ──────────────────────────────────────────
        "XADD" => {
            if args.len() < 4 || (args.len() - 2) % 2 != 0 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'xadd' command".to_string(),
                ));
            }
            let key = args[0].clone();
            let id_str = String::from_utf8_lossy(&args[1]);

            // Passive expiration
            if let Some(entry) = data.get(&key) {
                if entry.is_expired() {
                    data.remove(&key);
                }
            }

            let entry = data
                .entry(key)
                .or_insert_with(ValueEntry::new_stream);

            match entry.data {
                ValueData::Stream(ref mut stream) => {
                    let new_id = if id_str == "*" {
                        // Auto-generate ID
                        let ms = std::time::SystemTime::UNIX_EPOCH
                            .elapsed()
                            .unwrap()
                            .as_millis() as u64;
                        if ms > stream.last_id.0 {
                            (ms, 0)
                        } else {
                            (stream.last_id.0, stream.last_id.1 + 1)
                        }
                    } else {
                        // Parse explicit ID
                        let parts: Vec<&str> = id_str.splitn(2, '-').collect();
                        if parts.len() != 2 {
                            return Ok(RedisValue::Error(
                                "ERR Invalid stream ID specified as stream command argument"
                                    .to_string(),
                            ));
                        }
                        let ms: u64 = parts[0].parse().map_err(|_| {
                            "ERR Invalid stream ID specified as stream command argument".to_string()
                        })?;
                        let seq: u64 = parts[1].parse().map_err(|_| {
                            "ERR Invalid stream ID specified as stream command argument".to_string()
                        })?;
                        let explicit_id = (ms, seq);
                        if explicit_id <= stream.last_id {
                            return Ok(RedisValue::Error(
                                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
                                    .to_string(),
                            ));
                        }
                        explicit_id
                    };

                    // Parse fields
                    let mut fields = HashMap::new();
                    let mut i = 2;
                    while i + 1 < args.len() {
                        fields.insert(args[i].clone(), args[i + 1].clone());
                        i += 2;
                    }

                    stream.entries.insert(new_id, fields);
                    stream.last_id = new_id;

                    let id_string = format!("{}-{}", new_id.0, new_id.1);
                    Ok(RedisValue::BulkString(Bytes::from(id_string)))
                }
                _ => Ok(RedisValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                )),
            }
        }

        "XREAD" => {
            // XREAD [COUNT n] STREAMS key1 [key2 ...] id1 [id2 ...]
            if args.is_empty() {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'xread' command".to_string(),
                ));
            }

            let mut count: Option<usize> = None;
            let mut i = 0;

            // Parse optional COUNT
            while i < args.len() {
                let token = String::from_utf8_lossy(&args[i]).to_uppercase();
                if token == "COUNT" {
                    i += 1;
                    if i >= args.len() {
                        return Ok(RedisValue::Error("ERR syntax error".to_string()));
                    }
                    count = Some(
                        String::from_utf8_lossy(&args[i])
                            .parse::<usize>()
                            .map_err(|_| "ERR value is not an integer or out of range".to_string())?,
                    );
                    i += 1;
                } else if token == "STREAMS" {
                    i += 1;
                    break;
                } else {
                    i += 1;
                }
            }

            // Remaining args after STREAMS: first half are keys, second half are IDs
            let remaining = &args[i..];
            if remaining.is_empty() || remaining.len() % 2 != 0 {
                return Ok(RedisValue::Error(
                    "ERR Unbalanced XREAD list of streams: for each stream key an ID must be specified"
                        .to_string(),
                ));
            }

            let half = remaining.len() / 2;
            let keys_slice = &remaining[..half];
            let ids_slice = &remaining[half..];

            let mut result = Vec::new();
            for (key, id_bytes) in keys_slice.iter().zip(ids_slice.iter()) {
                let id_str = String::from_utf8_lossy(id_bytes);
                let start_id = if id_str == "0" || id_str == "0-0" {
                    (0u64, 0u64)
                } else {
                    let parts: Vec<&str> = id_str.splitn(2, '-').collect();
                    if parts.len() == 2 {
                        let ms: u64 = parts[0].parse().unwrap_or(0);
                        let seq: u64 = parts[1].parse().unwrap_or(0);
                        (ms, seq)
                    } else {
                        let ms: u64 = parts[0].parse().unwrap_or(0);
                        (ms, 0)
                    }
                };

                // Passive expiration
                if let Some(entry) = data.get(key) {
                    if entry.is_expired() {
                        data.remove(key);
                        continue;
                    }
                }

                match data.get(key) {
                    None => continue,
                    Some(entry) => match &entry.data {
                        ValueData::Stream(stream) => {
                            let entries: Vec<RedisValue> = stream
                                .entries
                                .range((
                                    std::ops::Bound::Excluded(start_id),
                                    std::ops::Bound::Unbounded,
                                ))
                                .take(count.unwrap_or(usize::MAX))
                                .map(|(id, fields)| {
                                    let id_str = format!("{}-{}", id.0, id.1);
                                    let mut field_arr = Vec::new();
                                    for (k, v) in fields {
                                        field_arr.push(RedisValue::BulkString(k.clone()));
                                        field_arr.push(RedisValue::BulkString(v.clone()));
                                    }
                                    RedisValue::Array(vec![
                                        RedisValue::BulkString(Bytes::from(id_str)),
                                        RedisValue::Array(field_arr),
                                    ])
                                })
                                .collect();

                            if !entries.is_empty() {
                                result.push(RedisValue::Array(vec![
                                    RedisValue::BulkString(key.clone()),
                                    RedisValue::Array(entries),
                                ]));
                            }
                        }
                        _ => {
                            return Ok(RedisValue::Error(
                                "WRONGTYPE Operation against a key holding the wrong kind of value"
                                    .to_string(),
                            ))
                        }
                    },
                }
            }

            if result.is_empty() {
                Ok(RedisValue::Nil)
            } else {
                Ok(RedisValue::Array(result))
            }
        }

        "XDEL" => {
            if args.len() < 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'xdel' command".to_string(),
                ));
            }
            let key = &args[0];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get_mut(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => match entry.data {
                    ValueData::Stream(ref mut stream) => {
                        let mut count = 0i64;
                        for id_bytes in &args[1..] {
                            let id_str = String::from_utf8_lossy(id_bytes);
                            let parts: Vec<&str> = id_str.splitn(2, '-').collect();
                            if parts.len() == 2 {
                                let ms: u64 = parts[0].parse().unwrap_or(0);
                                let seq: u64 = parts[1].parse().unwrap_or(0);
                                if stream.entries.remove(&(ms, seq)).is_some() {
                                    count += 1;
                                }
                            }
                        }
                        Ok(RedisValue::Integer(count))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "XACK" => {
            // XACK key group id [id ...]
            if args.len() < 3 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'xack' command".to_string(),
                ));
            }
            let key = &args[0];
            let group = &args[1];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get_mut(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => match entry.data {
                    ValueData::Stream(ref mut stream) => {
                        let cg = match stream.groups.get_mut(group) {
                            Some(g) => g,
                            None => return Ok(RedisValue::Integer(0)),
                        };
                        let mut count = 0i64;
                        for id_bytes in &args[2..] {
                            let id_str = String::from_utf8_lossy(id_bytes);
                            let parts: Vec<&str> = id_str.splitn(2, '-').collect();
                            if parts.len() == 2 {
                                let ms: u64 = parts[0].parse().unwrap_or(0);
                                let seq: u64 = parts[1].parse().unwrap_or(0);
                                let stream_id = (ms, seq);
                                // Search all consumers for this pending entry
                                for consumer in cg.consumers.values_mut() {
                                    if consumer.pending.remove(&stream_id).is_some() {
                                        count += 1;
                                        break;
                                    }
                                }
                            }
                        }
                        Ok(RedisValue::Integer(count))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "XLEN" => {
            // XLEN key
            if args.len() != 1 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'xlen' command".to_string(),
                ));
            }
            let key = &args[0];

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => match &entry.data {
                    ValueData::Stream(stream) => {
                        Ok(RedisValue::Integer(stream.entries.len() as i64))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "XRANGE" | "XREVRANGE" => {
            // XRANGE key start end [COUNT n]
            // XREVRANGE key end start [COUNT n]   (note: reversed start/end)
            if args.len() < 3 {
                return Ok(RedisValue::Error(format!(
                    "ERR wrong number of arguments for '{}' command",
                    cmd.to_lowercase()
                )));
            }
            let key = &args[0];
            let reverse = cmd == "XREVRANGE";

            // For XRANGE: args[1] = min, args[2] = max
            // For XREVRANGE: args[1] = max (high), args[2] = min (low)
            let (lo_bytes, hi_bytes) = if reverse {
                (&args[2], &args[1])
            } else {
                (&args[1], &args[2])
            };

            // Parse a range bound.  Sentinels:
            //   "-"  -> (0, 0)                          (inclusive)
            //   "+"  -> (u64::MAX, u64::MAX)            (inclusive)
            //   "ms" alone (no '-') -> low: (ms, 0); high: (ms, u64::MAX)
            //   "ms-seq" -> explicit
            //   leading "(" -> exclusive bound (e.g. "(1500-0" or "(+")
            //   See https://redis.io/docs/latest/commands/xrange/#exclusive-ranges
            fn parse_bound(
                s: &str,
                is_low: bool,
            ) -> Option<(crate::memory::engine::streams::StreamId, bool /* exclusive */)> {
                let (s, exclusive) = match s.strip_prefix('(') {
                    Some(rest) => (rest, true),
                    None => (s, false),
                };
                if s == "-" {
                    return Some(((0, 0), exclusive));
                }
                if s == "+" {
                    return Some(((u64::MAX, u64::MAX), exclusive));
                }
                let parts: Vec<&str> = s.splitn(2, '-').collect();
                let ms: u64 = parts[0].parse().ok()?;
                if parts.len() == 2 {
                    let seq: u64 = parts[1].parse().ok()?;
                    Some(((ms, seq), exclusive))
                } else if is_low {
                    Some(((ms, 0), exclusive))
                } else {
                    Some(((ms, u64::MAX), exclusive))
                }
            }

            let lo_str = String::from_utf8_lossy(lo_bytes);
            let hi_str = String::from_utf8_lossy(hi_bytes);
            let (lo, lo_excl) = match parse_bound(&lo_str, true) {
                Some(v) => v,
                None => {
                    return Ok(RedisValue::Error(
                        "ERR Invalid stream ID specified as stream command argument"
                            .to_string(),
                    ));
                }
            };
            let (hi, hi_excl) = match parse_bound(&hi_str, false) {
                Some(v) => v,
                None => {
                    return Ok(RedisValue::Error(
                        "ERR Invalid stream ID specified as stream command argument"
                            .to_string(),
                    ));
                }
            };

            // Optional COUNT n
            let mut count: Option<usize> = None;
            let mut i = 3;
            while i < args.len() {
                let token = String::from_utf8_lossy(&args[i]).to_uppercase();
                if token == "COUNT" {
                    if i + 1 >= args.len() {
                        return Ok(RedisValue::Error("ERR syntax error".to_string()));
                    }
                    match String::from_utf8_lossy(&args[i + 1]).parse::<usize>() {
                        Ok(n) => count = Some(n),
                        Err(_) => {
                            return Ok(RedisValue::Error(
                                "ERR value is not an integer or out of range"
                                    .to_string(),
                            ));
                        }
                    }
                    i += 2;
                } else {
                    return Ok(RedisValue::Error("ERR syntax error".to_string()));
                }
            }

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Array(Vec::new()));
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Array(Vec::new())),
                Some(entry) => match &entry.data {
                    ValueData::Stream(stream) => {
                        // Inverted bounds (lo > hi, or equal-and-either-exclusive) are
                        // valid in real Redis -- they just produce an empty result.
                        // BTreeMap::range would panic on such input, so short-circuit
                        // after type-checking the key.
                        if lo > hi || (lo == hi && (lo_excl || hi_excl)) {
                            return Ok(RedisValue::Array(Vec::new()));
                        }

                        let lo_bound = if lo_excl {
                            std::ops::Bound::Excluded(lo)
                        } else {
                            std::ops::Bound::Included(lo)
                        };
                        let hi_bound = if hi_excl {
                            std::ops::Bound::Excluded(hi)
                        } else {
                            std::ops::Bound::Included(hi)
                        };
                        let limit = count.unwrap_or(usize::MAX);

                        // Format as [[id, [k1, v1, k2, v2, ...]], ...]
                        let result: Vec<RedisValue> = if reverse {
                            stream
                                .entries
                                .range((lo_bound, hi_bound))
                                .rev()
                                .take(limit)
                                .map(|(id, fields)| stream_entry_to_lua_array(*id, fields))
                                .collect()
                        } else {
                            stream
                                .entries
                                .range((lo_bound, hi_bound))
                                .take(limit)
                                .map(|(id, fields)| stream_entry_to_lua_array(*id, fields))
                                .collect()
                        };
                        Ok(RedisValue::Array(result))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "XCLAIM" => {
            // XCLAIM key group consumer min-idle-time id [id ...] [IDLE ms] [FORCE] [JUSTID] [RETRYCOUNT n]
            if args.len() < 5 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'xclaim' command".to_string(),
                ));
            }
            let key = &args[0];
            let group = &args[1];
            let consumer = args[2].clone();
            let min_idle_str = String::from_utf8_lossy(&args[3]);
            let min_idle_time: u64 = min_idle_str.parse().map_err(|_| {
                "ERR Invalid min-idle-time argument for XCLAIM".to_string()
            })?;

            // Parse remaining args: IDs and optional flags
            let mut ids = Vec::new();
            let mut idle: Option<u64> = None;
            let mut force = false;
            let mut justid = false;
            let mut retrycount: Option<u64> = None;
            let mut i = 4;
            while i < args.len() {
                let arg_upper = String::from_utf8_lossy(&args[i]).to_uppercase();
                match arg_upper.as_str() {
                    "IDLE" => {
                        if i + 1 < args.len() {
                            idle = Some(
                                String::from_utf8_lossy(&args[i + 1])
                                    .parse()
                                    .unwrap_or(0),
                            );
                            i += 2;
                            continue;
                        }
                        i += 1;
                        continue;
                    }
                    "RETRYCOUNT" => {
                        if i + 1 < args.len() {
                            retrycount = Some(
                                String::from_utf8_lossy(&args[i + 1])
                                    .parse()
                                    .unwrap_or(0),
                            );
                            i += 2;
                            continue;
                        }
                        i += 1;
                        continue;
                    }
                    "FORCE" => {
                        force = true;
                        i += 1;
                        continue;
                    }
                    "JUSTID" => {
                        justid = true;
                        i += 1;
                        continue;
                    }
                    _ => {
                        // Must be a stream ID
                        let id_str = String::from_utf8_lossy(&args[i]);
                        if let Some(parsed) = crate::memory::engine::streams::parse_stream_id(&id_str) {
                            ids.push(parsed);
                        }
                        i += 1;
                    }
                }
            }

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Error(format!(
                        "NOGROUP No such key '{}' or consumer group '{}' in XCLAIM",
                        String::from_utf8_lossy(key),
                        String::from_utf8_lossy(group),
                    )));
                }
            }

            match data.get_mut(key) {
                None => Ok(RedisValue::Error(format!(
                    "NOGROUP No such key '{}' or consumer group '{}' in XCLAIM",
                    String::from_utf8_lossy(key),
                    String::from_utf8_lossy(group),
                ))),
                Some(entry) => match entry.data {
                    ValueData::Stream(ref mut stream) => {
                        let cg = match stream.groups.get_mut(group) {
                            Some(g) => g,
                            None => {
                                return Ok(RedisValue::Error(format!(
                                    "NOGROUP No such key '{}' or consumer group '{}' in XCLAIM",
                                    String::from_utf8_lossy(key),
                                    String::from_utf8_lossy(group),
                                )));
                            }
                        };

                        let now = std::time::Instant::now();
                        let min_idle = std::time::Duration::from_millis(min_idle_time);
                        let mut claimed = Vec::new();

                        for &id in &ids {
                            // Find entry in any consumer's PEL
                            let mut found_consumer: Option<Bytes> = None;
                            let mut found_entry: Option<crate::memory::engine::store::PendingEntry> = None;
                            for (cname, c) in cg.consumers.iter() {
                                if let Some(pe) = c.pending.get(&id) {
                                    let idle_dur = now.duration_since(pe.delivery_time);
                                    if idle_dur >= min_idle || force {
                                        found_consumer = Some(cname.clone());
                                        found_entry = Some(pe.clone());
                                    }
                                    break;
                                }
                            }

                            // Force create if not found
                            if found_consumer.is_none() && force {
                                if stream.entries.contains_key(&id) {
                                    let new_dt = match idle {
                                        Some(ms) => now - std::time::Duration::from_millis(ms),
                                        None => now,
                                    };
                                    let target = cg
                                        .consumers
                                        .entry(consumer.clone())
                                        .or_insert_with(|| crate::memory::engine::store::Consumer {
                                            pending: HashMap::new(),
                                        });
                                    target.pending.insert(
                                        id,
                                        crate::memory::engine::store::PendingEntry {
                                            delivery_time: new_dt,
                                            delivery_count: retrycount.unwrap_or(1),
                                        },
                                    );
                                    if justid {
                                        claimed.push(RedisValue::BulkString(Bytes::from(
                                            format!("{}-{}", id.0, id.1),
                                        )));
                                    } else if let Some(fields) = stream.entries.get(&id) {
                                        let mut items = Vec::new();
                                        items.push(RedisValue::BulkString(Bytes::from(
                                            format!("{}-{}", id.0, id.1),
                                        )));
                                        let mut field_items = Vec::new();
                                        for (fk, fv) in fields {
                                            field_items
                                                .push(RedisValue::BulkString(fk.clone()));
                                            field_items
                                                .push(RedisValue::BulkString(fv.clone()));
                                        }
                                        items.push(RedisValue::Array(field_items));
                                        claimed.push(RedisValue::Array(items));
                                    }
                                }
                                continue;
                            }

                            if let (Some(from_consumer), Some(pe)) =
                                (found_consumer, found_entry)
                            {
                                if let Some(orig) = cg.consumers.get_mut(&from_consumer) {
                                    orig.pending.remove(&id);
                                }
                                let new_dt = match idle {
                                    Some(ms) => now - std::time::Duration::from_millis(ms),
                                    None => pe.delivery_time,
                                };
                                let new_dc = retrycount.unwrap_or(pe.delivery_count + 1);
                                let target = cg
                                    .consumers
                                    .entry(consumer.clone())
                                    .or_insert_with(|| crate::memory::engine::store::Consumer {
                                        pending: HashMap::new(),
                                    });
                                target.pending.insert(
                                    id,
                                    crate::memory::engine::store::PendingEntry {
                                        delivery_time: new_dt,
                                        delivery_count: new_dc,
                                    },
                                );
                                if justid {
                                    claimed.push(RedisValue::BulkString(Bytes::from(
                                        format!("{}-{}", id.0, id.1),
                                    )));
                                } else if let Some(fields) = stream.entries.get(&id) {
                                    let mut items = Vec::new();
                                    items.push(RedisValue::BulkString(Bytes::from(
                                        format!("{}-{}", id.0, id.1),
                                    )));
                                    let mut field_items = Vec::new();
                                    for (fk, fv) in fields {
                                        field_items.push(RedisValue::BulkString(fk.clone()));
                                        field_items.push(RedisValue::BulkString(fv.clone()));
                                    }
                                    items.push(RedisValue::Array(field_items));
                                    claimed.push(RedisValue::Array(items));
                                }
                            }
                        }

                        Ok(RedisValue::Array(claimed))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        // ── Pub/Sub commands ─────────────────────────────────────────
        "PUBLISH" => {
            if args.len() != 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'publish' command".to_string(),
                ));
            }
            let channel = &args[0];
            let message = &args[1];

            match pubsub_tx {
                Some(tx) => {
                    // Send the message through the broadcast channel.
                    // We return 0 here rather than tx.receiver_count(), because
                    // receiver_count() reflects ALL active broadcast receivers
                    // (including those subscribed to different channels/patterns),
                    // which inflates the count compared to what Store::publish()
                    // returns (only matching channel + pattern subscribers).
                    // Accurate counting would require passing the PubSubRegistry
                    // into the Lua dispatch context — a future improvement.
                    let _ = tx.send(PubSubMessage {
                        kind: "message".to_string(),
                        pattern: None,
                        channel: channel.clone(),
                        data: message.clone(),
                    });
                    Ok(RedisValue::Integer(0))
                }
                None => {
                    // No pubsub sender available
                    Ok(RedisValue::Integer(0))
                }
            }
        }

        // ── List commands (non-blocking) ─────────────────────────────
        "LPUSH" => {
            if args.len() < 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'lpush' command".to_string(),
                ));
            }
            let key = args[0].clone();

            // Passive expiration
            if let Some(entry) = data.get(&key) {
                if entry.is_expired() {
                    data.remove(&key);
                }
            }

            let entry = data.entry(key).or_insert_with(ValueEntry::new_list);
            match entry.data {
                ValueData::List(ref mut list) => {
                    for v in &args[1..] {
                        list.push_front(v.clone());
                    }
                    Ok(RedisValue::Integer(list.len() as i64))
                }
                _ => Ok(RedisValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                )),
            }
        }

        "RPUSH" => {
            if args.len() < 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'rpush' command".to_string(),
                ));
            }
            let key = args[0].clone();

            if let Some(entry) = data.get(&key) {
                if entry.is_expired() {
                    data.remove(&key);
                }
            }

            let entry = data.entry(key).or_insert_with(ValueEntry::new_list);
            match entry.data {
                ValueData::List(ref mut list) => {
                    for v in &args[1..] {
                        list.push_back(v.clone());
                    }
                    Ok(RedisValue::Integer(list.len() as i64))
                }
                _ => Ok(RedisValue::Error(
                    "WRONGTYPE Operation against a key holding the wrong kind of value".to_string(),
                )),
            }
        }

        "LPOP" => {
            if args.is_empty() || args.len() > 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'lpop' command".to_string(),
                ));
            }
            let key = &args[0];
            let count: Option<usize> = if args.len() == 2 {
                let n: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                    Ok(n) => n,
                    Err(_) => {
                        return Ok(RedisValue::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        ));
                    }
                };
                if n < 0 {
                    return Ok(RedisValue::Error(
                        "ERR value is out of range, must be positive".to_string(),
                    ));
                }
                Some(n as usize)
            } else {
                None
            };

            // Passive expiration
            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                }
            }

            // Type-check BEFORE count=0 fast-return so WRONGTYPE still propagates.
            match data.get(key) {
                None => return Ok(RedisValue::Nil),
                Some(entry) => match &entry.data {
                    ValueData::List(_) => {}
                    _ => {
                        return Ok(RedisValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ));
                    }
                },
            }

            if count == Some(0) {
                return Ok(RedisValue::Array(Vec::new()));
            }

            let entry = data.get_mut(key).expect("entry exists (checked above)");
            let list = match &mut entry.data {
                ValueData::List(l) => l,
                _ => unreachable!(),
            };

            let result = match count {
                None => match list.pop_front() {
                    Some(v) => RedisValue::BulkString(v),
                    None => RedisValue::Nil,
                },
                Some(n) => {
                    let actual = n.min(list.len());
                    let popped: Vec<RedisValue> = (0..actual)
                        .map(|_| RedisValue::BulkString(list.pop_front().expect("len checked")))
                        .collect();
                    RedisValue::Array(popped)
                }
            };

            // D-03: delete key if empty.
            if let Some(entry) = data.get(key) {
                if let ValueData::List(l) = &entry.data {
                    if l.is_empty() {
                        data.remove(key);
                    }
                }
            }
            Ok(result)
        }

        "RPOP" => {
            if args.is_empty() || args.len() > 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'rpop' command".to_string(),
                ));
            }
            let key = &args[0];
            let count: Option<usize> = if args.len() == 2 {
                let n: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                    Ok(n) => n,
                    Err(_) => {
                        return Ok(RedisValue::Error(
                            "ERR value is not an integer or out of range".to_string(),
                        ));
                    }
                };
                if n < 0 {
                    return Ok(RedisValue::Error(
                        "ERR value is out of range, must be positive".to_string(),
                    ));
                }
                Some(n as usize)
            } else {
                None
            };

            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                }
            }

            match data.get(key) {
                None => return Ok(RedisValue::Nil),
                Some(entry) => match &entry.data {
                    ValueData::List(_) => {}
                    _ => {
                        return Ok(RedisValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ));
                    }
                },
            }

            if count == Some(0) {
                return Ok(RedisValue::Array(Vec::new()));
            }

            let entry = data.get_mut(key).expect("entry exists (checked above)");
            let list = match &mut entry.data {
                ValueData::List(l) => l,
                _ => unreachable!(),
            };

            let result = match count {
                None => match list.pop_back() {
                    Some(v) => RedisValue::BulkString(v),
                    None => RedisValue::Nil,
                },
                Some(n) => {
                    let actual = n.min(list.len());
                    let popped: Vec<RedisValue> = (0..actual)
                        .map(|_| RedisValue::BulkString(list.pop_back().expect("len checked")))
                        .collect();
                    RedisValue::Array(popped)
                }
            };

            if let Some(entry) = data.get(key) {
                if let ValueData::List(l) = &entry.data {
                    if l.is_empty() {
                        data.remove(key);
                    }
                }
            }
            Ok(result)
        }

        "LRANGE" => {
            if args.len() != 3 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'lrange' command".to_string(),
                ));
            }
            let key = &args[0];
            let start: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                Ok(n) => n,
                Err(_) => {
                    return Ok(RedisValue::Error(
                        "ERR value is not an integer or out of range".to_string(),
                    ));
                }
            };
            let stop: i64 = match String::from_utf8_lossy(&args[2]).parse() {
                Ok(n) => n,
                Err(_) => {
                    return Ok(RedisValue::Error(
                        "ERR value is not an integer or out of range".to_string(),
                    ));
                }
            };

            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Array(Vec::new()));
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Array(Vec::new())),
                Some(entry) => match &entry.data {
                    ValueData::List(list) => {
                        let (s, e) = match crate::memory::engine::lists::normalize_range_indices(
                            start,
                            stop,
                            list.len(),
                        ) {
                            None => return Ok(RedisValue::Array(Vec::new())),
                            Some(pair) => pair,
                        };
                        let items: Vec<RedisValue> = list
                            .iter()
                            .skip(s)
                            .take(e - s + 1)
                            .map(|b| RedisValue::BulkString(b.clone()))
                            .collect();
                        Ok(RedisValue::Array(items))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "LLEN" => {
            if args.len() != 1 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'llen' command".to_string(),
                ));
            }
            let key = &args[0];

            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => match &entry.data {
                    ValueData::List(list) => Ok(RedisValue::Integer(list.len() as i64)),
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "LINDEX" => {
            if args.len() != 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'lindex' command".to_string(),
                ));
            }
            let key = &args[0];
            let index: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                Ok(n) => n,
                Err(_) => {
                    return Ok(RedisValue::Error(
                        "ERR value is not an integer or out of range".to_string(),
                    ));
                }
            };

            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Nil);
                }
            }

            match data.get(key) {
                None => Ok(RedisValue::Nil),
                Some(entry) => match &entry.data {
                    ValueData::List(list) => {
                        let n = list.len() as i64;
                        let actual = if index < 0 { index + n } else { index };
                        if actual < 0 || actual >= n {
                            Ok(RedisValue::Nil)
                        } else {
                            match list.get(actual as usize) {
                                Some(b) => Ok(RedisValue::BulkString(b.clone())),
                                None => Ok(RedisValue::Nil),
                            }
                        }
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "LINSERT" => {
            // LINSERT key BEFORE|AFTER pivot value
            if args.len() != 4 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'linsert' command".to_string(),
                ));
            }
            let key = &args[0];
            let where_str = String::from_utf8_lossy(&args[1]);
            let position = match crate::memory::engine::lists::parse_linsert_where(&where_str) {
                Ok(p) => p,
                Err(e) => return Ok(RedisValue::Error(e.to_string())),
            };
            let pivot = &args[2];
            let value = args[3].clone();

            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            match data.get_mut(key) {
                None => Ok(RedisValue::Integer(0)),
                Some(entry) => match &mut entry.data {
                    ValueData::List(list) => {
                        let pos = match list.iter().position(|v| v == pivot) {
                            None => return Ok(RedisValue::Integer(-1)),
                            Some(p) => p,
                        };
                        let insert_at = match position {
                            crate::memory::engine::lists::InsertPosition::Before => pos,
                            crate::memory::engine::lists::InsertPosition::After => pos + 1,
                        };
                        list.insert(insert_at, value);
                        Ok(RedisValue::Integer(list.len() as i64))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "LREM" => {
            if args.len() != 3 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'lrem' command".to_string(),
                ));
            }
            let key = &args[0];
            let count: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                Ok(n) => n,
                Err(_) => {
                    return Ok(RedisValue::Error(
                        "ERR value is not an integer or out of range".to_string(),
                    ));
                }
            };
            let value = args[2].clone();

            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Integer(0));
                }
            }

            let mut removed: i64 = 0;
            let became_empty;
            match data.get_mut(key) {
                None => return Ok(RedisValue::Integer(0)),
                Some(entry) => match &mut entry.data {
                    ValueData::List(list) => {
                        match crate::memory::engine::lists::parse_lrem_count(count) {
                            crate::memory::engine::lists::LremDirection::Head(target) => {
                                list.retain(|v| {
                                    if (removed as usize) < target && v == &value {
                                        removed += 1;
                                        false
                                    } else {
                                        true
                                    }
                                });
                            }
                            crate::memory::engine::lists::LremDirection::Tail(target) => {
                                let indices: Vec<usize> = list
                                    .iter()
                                    .enumerate()
                                    .rev()
                                    .filter_map(|(i, v)| if v == &value { Some(i) } else { None })
                                    .take(target)
                                    .collect();
                                for i in indices {
                                    list.remove(i);
                                    removed += 1;
                                }
                            }
                            crate::memory::engine::lists::LremDirection::All => {
                                let before = list.len();
                                list.retain(|v| v != &value);
                                removed = (before - list.len()) as i64;
                            }
                        }
                        became_empty = list.is_empty();
                    }
                    _ => {
                        return Ok(RedisValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ));
                    }
                },
            }
            if became_empty {
                data.remove(key);
            }
            Ok(RedisValue::Integer(removed))
        }

        "LSET" => {
            if args.len() != 3 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'lset' command".to_string(),
                ));
            }
            let key = &args[0];
            let index: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                Ok(n) => n,
                Err(_) => {
                    return Ok(RedisValue::Error(
                        "ERR value is not an integer or out of range".to_string(),
                    ));
                }
            };
            let value = args[2].clone();

            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Error("ERR no such key".to_string()));
                }
            }

            match data.get_mut(key) {
                None => Ok(RedisValue::Error("ERR no such key".to_string())),
                Some(entry) => match &mut entry.data {
                    ValueData::List(list) => {
                        let n = list.len() as i64;
                        let actual = if index < 0 { index + n } else { index };
                        if actual < 0 || actual >= n {
                            return Ok(RedisValue::Error("ERR index out of range".to_string()));
                        }
                        list[actual as usize] = value;
                        Ok(RedisValue::Status("OK".to_string()))
                    }
                    _ => Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    )),
                },
            }
        }

        "LTRIM" => {
            if args.len() != 3 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'ltrim' command".to_string(),
                ));
            }
            let key = &args[0];
            let start: i64 = match String::from_utf8_lossy(&args[1]).parse() {
                Ok(n) => n,
                Err(_) => {
                    return Ok(RedisValue::Error(
                        "ERR value is not an integer or out of range".to_string(),
                    ));
                }
            };
            let stop: i64 = match String::from_utf8_lossy(&args[2]).parse() {
                Ok(n) => n,
                Err(_) => {
                    return Ok(RedisValue::Error(
                        "ERR value is not an integer or out of range".to_string(),
                    ));
                }
            };

            if let Some(entry) = data.get(key) {
                if entry.is_expired() {
                    data.remove(key);
                    return Ok(RedisValue::Status("OK".to_string()));
                }
            }

            let mut remove_key = false;
            match data.get_mut(key) {
                None => return Ok(RedisValue::Status("OK".to_string())),
                Some(entry) => match &mut entry.data {
                    ValueData::List(list) => {
                        let len = list.len();
                        match crate::memory::engine::lists::normalize_range_indices(start, stop, len) {
                            None => {
                                remove_key = true;
                            }
                            Some((s, e)) => {
                                let new_list: VecDeque<Bytes> =
                                    list.iter().skip(s).take(e - s + 1).cloned().collect();
                                *list = new_list;
                                if list.is_empty() {
                                    remove_key = true;
                                }
                            }
                        }
                    }
                    _ => {
                        return Ok(RedisValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ));
                    }
                },
            }
            if remove_key {
                data.remove(key);
            }
            Ok(RedisValue::Status("OK".to_string()))
        }

        "LMOVE" => {
            // LMOVE src dst LEFT|RIGHT LEFT|RIGHT
            if args.len() != 4 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'lmove' command".to_string(),
                ));
            }
            let src = args[0].clone();
            let dst = args[1].clone();
            let src_end_str = String::from_utf8_lossy(&args[2]);
            let dst_end_str = String::from_utf8_lossy(&args[3]);
            let src_from = match crate::memory::engine::lists::parse_list_end(&src_end_str) {
                Ok(e) => e,
                Err(e) => return Ok(RedisValue::Error(e.to_string())),
            };
            let dst_to = match crate::memory::engine::lists::parse_list_end(&dst_end_str) {
                Ok(e) => e,
                Err(e) => return Ok(RedisValue::Error(e.to_string())),
            };

            // Passive expiration
            if let Some(e) = data.get(&src) {
                if e.is_expired() {
                    data.remove(&src);
                }
            }
            if src != dst {
                if let Some(e) = data.get(&dst) {
                    if e.is_expired() {
                        data.remove(&dst);
                    }
                }
            }

            // P2-10: determine src state FIRST (read-only). Missing or empty
            // src is a no-op move and must return Nil without ever inspecting
            // dst's type — matches `Store::lmove_atomic` after round-1 P2-02
            // and real Redis. Wrongtype src is the only src-side error case.
            match data.get(&src) {
                None => return Ok(RedisValue::Nil),
                Some(entry) => match &entry.data {
                    ValueData::List(l) if l.is_empty() => return Ok(RedisValue::Nil),
                    ValueData::List(_) => {} // src has at least one element
                    _ => {
                        return Ok(RedisValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ));
                    }
                },
            }

            // Now that src has a poppable element, type-check dst BEFORE
            // mutating src (atomicity guarantee — the data write lock spans
            // the entire arm, so the dst type cannot change between this
            // check and the push below). Skipped when src == dst — src's
            // type was already validated above.
            if src != dst {
                if let Some(dst_entry) = data.get(&dst) {
                    if !matches!(dst_entry.data, ValueData::List(_)) {
                        return Ok(RedisValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ));
                    }
                }
            }

            // Pop from source (narrow scope so the borrow ends before data.remove).
            let (popped_opt, src_empty) = {
                let src_entry = match data.get_mut(&src) {
                    None => return Ok(RedisValue::Nil),
                    Some(e) => e,
                };
                let src_list = match &mut src_entry.data {
                    ValueData::List(l) => l,
                    _ => {
                        return Ok(RedisValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ));
                    }
                };
                let val = match src_from {
                    crate::memory::engine::lists::ListEnd::Left => src_list.pop_front(),
                    crate::memory::engine::lists::ListEnd::Right => src_list.pop_back(),
                };
                (val, src_list.is_empty())
            };

            let popped = match popped_opt {
                None => return Ok(RedisValue::Nil),
                Some(v) => v,
            };

            // P2-09: same-key rotations must preserve TTL. Removing the
            // entry here would clear `expires_at`; the entry is re-created
            // by the `or_insert_with` below with no expiry. For `LMOVE k k
            // ...` we leave the (briefly empty) entry in place — the push
            // arm immediately re-fills it. Cross-key moves keep the
            // existing D-03 behavior of deleting an empty src.
            if src_empty && src != dst {
                data.remove(&src);
            }

            let dst_entry = data.entry(dst).or_insert_with(ValueEntry::new_list);
            match &mut dst_entry.data {
                ValueData::List(l) => match dst_to {
                    crate::memory::engine::lists::ListEnd::Left => l.push_front(popped.clone()),
                    crate::memory::engine::lists::ListEnd::Right => l.push_back(popped.clone()),
                },
                _ => {
                    return Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ));
                }
            }
            Ok(RedisValue::BulkString(popped))
        }

        "RPOPLPUSH" => {
            // Equivalent to: LMOVE src dst RIGHT LEFT
            if args.len() != 2 {
                return Ok(RedisValue::Error(
                    "ERR wrong number of arguments for 'rpoplpush' command".to_string(),
                ));
            }
            let src = args[0].clone();
            let dst = args[1].clone();

            if let Some(e) = data.get(&src) {
                if e.is_expired() {
                    data.remove(&src);
                }
            }
            if src != dst {
                if let Some(e) = data.get(&dst) {
                    if e.is_expired() {
                        data.remove(&dst);
                    }
                }
            }

            // P2-10: see LMOVE arm above. Determine src state FIRST
            // (read-only). Missing/empty src returns Nil without inspecting
            // dst — matches Store::lmove_atomic after round-1 P2-02 and real
            // Redis. Only validate dst's type once we know there's a value
            // to move; the data write lock spans the whole arm, so this
            // ordering preserves atomicity.
            match data.get(&src) {
                None => return Ok(RedisValue::Nil),
                Some(entry) => match &entry.data {
                    ValueData::List(l) if l.is_empty() => return Ok(RedisValue::Nil),
                    ValueData::List(_) => {} // src has at least one element
                    _ => {
                        return Ok(RedisValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ));
                    }
                },
            }

            if src != dst {
                if let Some(dst_entry) = data.get(&dst) {
                    if !matches!(dst_entry.data, ValueData::List(_)) {
                        return Ok(RedisValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ));
                    }
                }
            }

            let (popped_opt, src_empty) = {
                let src_entry = match data.get_mut(&src) {
                    None => return Ok(RedisValue::Nil),
                    Some(e) => e,
                };
                let src_list = match &mut src_entry.data {
                    ValueData::List(l) => l,
                    _ => {
                        return Ok(RedisValue::Error(
                            "WRONGTYPE Operation against a key holding the wrong kind of value"
                                .to_string(),
                        ));
                    }
                };
                let val = src_list.pop_back();
                (val, src_list.is_empty())
            };

            let popped = match popped_opt {
                None => return Ok(RedisValue::Nil),
                Some(v) => v,
            };

            // P2-09: see LMOVE arm above. Same-key `RPOPLPUSH k k` must
            // preserve TTL — removing the entry here clears `expires_at`
            // and the `or_insert_with` below re-creates it without one.
            if src_empty && src != dst {
                data.remove(&src);
            }

            let dst_entry = data.entry(dst).or_insert_with(ValueEntry::new_list);
            match &mut dst_entry.data {
                ValueData::List(l) => l.push_front(popped.clone()),
                _ => {
                    return Ok(RedisValue::Error(
                        "WRONGTYPE Operation against a key holding the wrong kind of value"
                            .to_string(),
                    ));
                }
            }
            Ok(RedisValue::BulkString(popped))
        }

        // ── Blocking list commands (rejected — scripts are atomic) ───
        // Per D-13: real Redis returns this exact error wording when a
        // script tries to invoke a blocking command (singular "script", no
        // colon, no command name). M-01 fix: matches actual Redis output.
        "BLPOP" | "BRPOP" | "BLMOVE" => Ok(RedisValue::Error(
            "ERR This Redis command is not allowed from script".to_string(),
        )),

        _ => Ok(RedisValue::Error(format!("ERR unknown command '{}'", cmd))),
    }
}

fn stream_entry_to_lua_array(
    id: crate::memory::engine::streams::StreamId,
    fields: &HashMap<Bytes, Bytes>,
) -> RedisValue {
    let id_str = format!("{}-{}", id.0, id.1);
    let mut pairs: Vec<RedisValue> = Vec::with_capacity(fields.len() * 2);
    for (k, v) in fields.iter() {
        pairs.push(RedisValue::BulkString(k.clone()));
        pairs.push(RedisValue::BulkString(v.clone()));
    }
    RedisValue::Array(vec![
        RedisValue::BulkString(Bytes::from(id_str)),
        RedisValue::Array(pairs),
    ])
}

/// Format a f64 score to a string matching Redis's format.
/// Redis uses a minimal representation: "1" for 1.0, "1.5" for 1.5, etc.
fn format_redis_score(score: f64) -> String {
    if score.is_infinite() {
        if score.is_sign_positive() {
            "inf".to_string()
        } else {
            "-inf".to_string()
        }
    } else if score.fract() == 0.0 {
        format!("{}", score as i64)
    } else {
        format!("{}", score)
    }
}

/// Represents an inclusive or exclusive score bound for range queries.
enum ScoreBound {
    Inclusive(f64),
    Exclusive(f64),
}

impl ScoreBound {
    /// Return the underlying f64 value.
    fn value(&self) -> f64 {
        match self {
            ScoreBound::Inclusive(v) | ScoreBound::Exclusive(v) => *v,
        }
    }

    /// Whether this bound is inclusive.
    fn is_inclusive(&self) -> bool {
        matches!(self, ScoreBound::Inclusive(_))
    }

    /// Return the BTreeMap lower-bound. Since the BTreeMap key is
    /// `(OrderedFloat<f64>, Bytes)` and members can have arbitrary bytes,
    /// exclusive lower bounds cannot be represented by a single BTreeMap key.
    /// Instead, we always return `Included((v, Bytes::new()))` (the smallest
    /// possible key at score v) and rely on a `.skip_while` filter at the call
    /// site to drop members whose score equals v when the bound is exclusive.
    fn lower_btree_bound(&self) -> std::ops::Bound<(OrderedFloat<f64>, Bytes)> {
        std::ops::Bound::Included((OrderedFloat(self.value()), Bytes::new()))
    }
}

/// Parse a score argument string (supports "-inf", "+inf", "inf", "(" exclusive prefix).
fn parse_score_arg(arg: &Bytes) -> ScoreBound {
    let s = String::from_utf8_lossy(arg);
    let s = s.trim();
    match s {
        "-inf" => ScoreBound::Inclusive(f64::NEG_INFINITY),
        "+inf" | "inf" => ScoreBound::Inclusive(f64::INFINITY),
        _ => {
            if let Some(stripped) = s.strip_prefix('(') {
                ScoreBound::Exclusive(stripped.parse::<f64>().unwrap_or(0.0))
            } else {
                ScoreBound::Inclusive(s.parse::<f64>().unwrap_or(0.0))
            }
        }
    }
}

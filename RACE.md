# Perpetual rescheduling race condition

## The problem

When a `Perpetual` task is running and an external caller uses `docket.replace()`
to force immediate re-execution of the same key, two executions of that key can run
concurrently.  Both executions attempt to reschedule themselves on completion.  The
last one to complete wins, potentially overwriting a correctly-timed successor with
stale timing data.

This affects any task that combines:

1. Self-rescheduling (via `Perpetual` or manual `docket.replace()` in a finally block)
2. External triggering (via `docket.replace(when=now)` from an API handler, change
   listener, or similar)

## How it happens

### Redis state through the lifecycle of a single task

When a task is **scheduled** (via `add` or `replace`), the runs hash looks like:

```
state     = "scheduled" (or "queued" if immediate)
known     = <timestamp>
when      = <timestamp>
stream_id = <id>          (if queued to stream)
function  = <name>
args      = <data>
kwargs    = <data>
```

When a worker **claims** the task (`execution.claim()`), the Lua script:

- Sets `state = "running"`, `worker = <name>`, `started_at = <iso>`
- **Deletes `known` and `stream_id`**

This deletion of `known` is important: it's what allows a new task with the same key
to be scheduled while the current one runs.  Without this, `docket.add()` would see
the `known` field and return `EXISTS`.

When the task **completes** (`mark_as_completed()`):

- Sets `state = "completed"`, `completed_at = <iso>`
- Sets TTL or deletes the hash (based on `execution_ttl`)

### The race, step by step

```
Time   Event                              runs hash state
────   ─────                              ─────────────────
t0     Task A scheduled (gen 1)           state=scheduled, known=t0
t1     Worker claims A                    state=running, known=DELETED
       A begins executing...

t2     User changes config
       External replace(when=now)          state=queued, known=t2, stream_id=X
       creates task B

t3     Worker claims B                    state=running, known=DELETED
       B begins executing...
       A is still running concurrently!

t4     B completes
       Perpetual.on_complete fires
       docket.replace(when=correct_time)  state=scheduled, known=t4
       Task C is now scheduled with
       the correct next-run time

t5     A completes (stale data!)
       Perpetual.on_complete fires
       docket.replace(when=stale_time)    state=scheduled, known=t5
       OVERWRITES C with A's stale timing
```

At t5, the schedule is wrong.  Task A computed its `run_again_at` based on state
that existed before the user's config change at t2.  But A's `replace` at t5
unconditionally overwrites C (the correctly-scheduled successor from B).

### Why `replace` allows this

The scheduling Lua script for `replace=True` unconditionally clears any
queued/scheduled task for the key and creates a new one:

```lua
if replace then
    local existing_message_id = redis.call('HGET', runs_key, 'stream_id')
    if existing_message_id then
        redis.call('XDEL', stream_key, existing_message_id)
    end
    redis.call('ZREM', queue_key, task_key)
    redis.call('DEL', parked_key)
    -- falls through to create the new task
end
```

It does not check whether the key already has a scheduled successor.  It does not
know whether the caller is "the current execution rescheduling itself" or "an
external party forcing a new execution."  Both paths use the same `replace=True`
code.

### Why the per-key lock doesn't help

The Redis lock in `execution.schedule()` serializes scheduling operations for a
given key.  This prevents corrupted state from concurrent writes, but it doesn't
prevent the semantic race: both A's and B's rescheduling calls succeed in sequence,
and the last one wins.

### Perpetual.on_complete

`Perpetual.on_complete()` (in `dependencies/_perpetual.py`) always uses
`docket.replace()`:

```python
async def on_complete(self, execution: Execution, outcome: TaskOutcome) -> bool:
    if self.cancelled:
        ...
        return False

    ...
    await docket.replace(execution.function, when, execution.key)(
        *self.args,
        **self.kwargs,
    )
    return True
```

There is no check for whether this execution has been superseded.  If the task was
externally replaced while running, the stale execution's `on_complete` will still
call `replace` and overwrite whatever the newer execution scheduled.

## How Prefect Cloud avoided this with arq

Before docket, Prefect Cloud's scheduler used arq with an explicit **generation
counter** implemented as a Lua compare-and-swap script:

```python
async def check_and_increment_generation(
    pool: arq.ArqRedis,
    key: str,
    expected_generation: int,
) -> int | None:
    """
    Atomically check if the current generation matches the expected generation,
    and if so, increment it and return the next generation.

    If the current generation does not match the expected generation, return None.
    """
    # Lua script:
    #   local current = tonumber(redis.call('GET', key) or 0)
    #   if current == expected then
    #       local next = current + 1
    #       redis.call('SET', key, next)
    #       return next
    #   else
    #       return -1
    #   end
```

The scheduler task received `generation` as an explicit argument.  On entry, it
called `check_and_increment_generation` with its generation value.  If the counter
had moved forward (because an external caller had already enqueued a newer task with
a higher generation), the function returned `None` and the task exited early without
doing any work or rescheduling:

```python
async def schedule_runs_for_deployment_schedule(
    context: dict,
    deployment_id: UUID,
    deployment_schedule_id: UUID,
    workspace_id: UUID,
    generation: int,   # <-- passed as task argument
) -> None:
    pool: arq.ArqRedis = context["pool"]

    next_generation = await check_and_increment_generation(
        pool, f"schedule_runs_generation:{deployment_schedule_id}", generation
    )
    if not next_generation:
        logger.info("Ignoring duplicate run for schedule %s, generation %s",
                     deployment_schedule_id, generation)
        return

    # ... do work, then enqueue next run with next_generation ...
```

This worked because:

1. Each scheduling of a task incremented the generation counter
2. The task checked the counter atomically on entry
3. If the counter didn't match, the task knew it was stale and bailed out
4. The stale task never reached the self-rescheduling code

The generation counter approach was effective but required explicit plumbing in every
task: accepting a `generation` argument, calling the check function, passing
`next_generation` when rescheduling.  It was a per-task responsibility, not a
framework-level protection.

## Affected services in Prefect Cloud

Three services in Prefect Cloud (nebula) exhibit this pattern:

### 1. Scheduler (`orion/services/scheduler.py`)

- **Self-rescheduling task**: `schedule_runs_for_deployment_schedule` uses
  `docket.replace(when=run_again_at)` in a finally block, where `run_again_at`
  is computed from the results of scheduling (soonest future run + buffer + jitter,
  or a cooldown if no runs were generated).
- **External trigger**: `enqueue_schedule_runs_for_deployment_schedule` calls
  `docket.replace(when=now)` when a user creates/updates/toggles a deployment
  schedule, via the API's after-commit hook.
- **Impact**: A stale execution can overwrite the correct next-run time.  The sweep
  (`ensure_schedule_runs_scheduled`, every 10 minutes) bounds the damage.

### 2. Triggers-proactive (`events/triggers/proactive.py`)

- **Self-rescheduling task**: `evaluate_proactive_trigger` uses `Perpetual` with
  dynamic `perpetual.every = run_again_at - now` based on the trigger's `within`
  window and bucket state.
- **External trigger**: `listen_for_proactive_changes` calls
  `docket.replace(when=now)` when an automation is created/updated/deleted,
  via PostgreSQL NOTIFY.
- **Impact**: A stale execution can set the wrong evaluation interval.  The sweep
  (`find_and_schedule_proactive_triggers`, every 1 minute) bounds the damage.

### 3. Work-pools-background (`work_pools/services/background.py`)

- **Self-rescheduling task**: `poll_push_work_pool_for_new_runs_and_submit` uses
  `Perpetual` with dynamic `perpetual.every = next_poll - now` based on whether
  runs were found and proximity to future scheduled runs.
- **External trigger**: `enqueue_polling_tasks_for_work_pools(immediately=True)`
  calls `docket.replace(when=now)` when deployments are created or runs are
  scheduled, to force an immediate poll.
- **Impact**: A stale execution can set the wrong polling interval.  The sweep
  (`ensure_push_pools_polling_tasks_are_scheduled`, every 5 minutes) bounds the
  damage.

## The harder case: dynamic intervals from user data

The simplest Perpetual tasks use a fixed interval (`Perpetual(every=timedelta(minutes=5))`)
and are immune to this race — even if a stale execution "wins" the reschedule, it
schedules at the same fixed interval as the correct execution would have.

The problematic case is when the interval itself depends on user-controlled data:

- **Scheduler**: `run_again_at` depends on the scheduled run times from the
  deployment's cron/interval/rrule schedule.  If a user changes the schedule from
  "every hour" to "every minute", the stale execution might reschedule for an hour
  from now instead of a minute.

- **Triggers-proactive**: `perpetual.every` depends on the trigger's `within` window
  and bucket end times.  If a user changes the trigger window from 1 hour to 5
  minutes, the stale execution might reschedule for an hour out.

- **Work-pools-background**: `perpetual.every` depends on proximity to the next
  scheduled run.  Less affected because the intervals are bounded to seconds/minutes,
  so the staleness window is small.

In all these cases, the stale execution computed its next-run time from data that no
longer reflects reality.  The user made a change, the system responded by forcing
immediate re-evaluation (via `replace`), but the stale execution's `on_complete`
overwrites the result.

## Ideas for fixing this in docket

### Option A: Generation counter in the runs hash

Add a `generation` field to the runs hash that increments on every `schedule()` call.
Include the generation in the task message so the execution knows its own generation.
In `Perpetual.on_complete`, atomically check the generation before rescheduling.

**Scheduling Lua script change:**
```lua
-- Increment generation on every schedule (both add and replace)
local new_gen = redis.call('HINCRBY', runs_key, 'generation', 1)

-- Include generation in the task message
-- (already available as a field in the message data)
```

**New "perpetuate" Lua script (or modified schedule path):**
```lua
local current_gen = redis.call('HGET', runs_key, 'generation')
local my_gen = ARGV[n]  -- from the execution's message

if current_gen ~= my_gen then
    return 'SUPERSEDED'
end

-- proceed with normal replace logic
```

**Pros:**
- Fully closes the race with no false positives
- Automatic for all Perpetual tasks, no per-task plumbing
- Mirrors the proven arq pattern but as a framework-level feature

**Cons:**
- Requires a schema change to the runs hash (new `generation` field)
- The generation must be threaded from the message through to the Execution object
- Adds a small amount of complexity to the scheduling Lua script

### Option B: Check for a scheduled successor before rescheduling

In `Perpetual.on_complete`, use a modified Lua script that checks whether a successor
is already scheduled before replacing:

```lua
-- Check if a successor has been scheduled since we started running
local known = redis.call('HEXISTS', runs_key, 'known')
if known == 1 then
    return 'SUPERSEDED'
end

-- No successor — proceed with replace
```

This is different from `add` (which also checks `state == 'running'`) — since the
current task is still technically "running" when `on_complete` fires, `add` would
always return `EXISTS`.

**Pros:**
- Simpler than generation tracking
- No new fields in the runs hash
- Covers the main case: external `replace` sets `known`, the stale execution sees it

**Cons:**
- Doesn't cover the narrow window where the successor has been both scheduled AND
  claimed (clearing `known`) before the stale execution's `on_complete` fires.  In
  the step-by-step trace above, if B is claimed (clearing `known`) and then A
  completes, A would not see `known` and would proceed to overwrite.
- In practice this window is small (the successor would need to be scheduled,
  claimed, and started before the stale execution finishes), but it's not zero.

### Option C: Don't reschedule at all — use sweeps as the source of truth

Remove self-rescheduling from individual task executions entirely.  Instead, rely
on periodic sweep tasks (which already exist in all three services) to schedule the
next execution.

For the scheduler:  `ensure_schedule_runs_scheduled` already runs every 10 minutes
and uses `docket.add()` for each active schedule.  If the individual task doesn't
reschedule itself, the sweep is the only scheduler.

**Pros:**
- Eliminates the race entirely by removing the self-rescheduling path
- Simple to understand

**Cons:**
- Adds up to one full sweep interval of latency before a schedule is re-evaluated
- For long-interval schedules (hourly, daily), this might be acceptable
- For short-interval schedules (every minute), a 10-minute sweep is far too slow
- Requires reducing sweep intervals or adding per-task sweep intelligence
- Sweeps become a scaling bottleneck if they need to run frequently for many tasks

### Option D: Perpetual interval as a "floor", not an absolute

Rather than having `on_complete` unconditionally `replace`, have it use `add`.  But
`add` currently returns `EXISTS` when the key has `state == 'running'` — which is
the state during `on_complete` (before `mark_as_completed`).

To make this work, docket would need a variant of `add` that only checks for a
scheduled/queued successor (the `known` field) without checking the running state.
This is effectively Option B reframed as a modification to the scheduling primitive.

If a successor is already scheduled (by an external `replace`), the `add` is a
no-op.  If no successor exists, the `add` creates one.  The external `replace`
always wins because it unconditionally overwrites, while the self-reschedule
gracefully defers.

**The timing issue**: between B's claim (clearing `known`) and B's `on_complete`
(which would set a new successor), there's a window where A's `on_complete` would
see no successor and proceed.  But by this point B has already started executing,
so B's `on_complete` would then `add` and find A's rescheduled task — and since
it's `add` (not `replace`), it would be a no-op.  The result: A's timing wins
over B's, which is wrong, but it's the same issue as Option B's con.

### Recommendation

**Option A (generation counter)** is the only approach that fully closes the race
with zero false positives.  It's more work than the other options, but it's the same
proven mechanism that Prefect Cloud used with arq for years.  The key difference is
that it would be built into docket itself rather than requiring per-task plumbing.

The implementation touches three places:

1. **Scheduling Lua script**: `HINCRBY runs_key generation 1` on every schedule,
   include the generation in the task message.
2. **Execution object**: Read the generation from the message at claim time, expose
   it as `execution.generation`.
3. **Perpetual.on_complete**: Before calling `docket.replace()`, atomically check
   `HGET runs_key generation` against `execution.generation`.  If they differ,
   return `True` (marking as handled) without rescheduling — the execution was
   superseded.

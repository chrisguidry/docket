"""The Lua that moves one task key between lifecycle states.

Every transition in a task's life is one atomic script against the keys for
a single task: ``_schedule`` puts it on the stream or the queue, ``_claim``
takes it for a worker, ``_terminal`` records how it finished, and
``_cancel_task`` takes it back off.  The first three each check the
``generation`` counter in the runs hash, so a stale attempt can never
overwrite a newer one.

``Execution`` and ``Docket`` call these; the scripts hold no Python logic of
their own.
"""

from typing import Any

from ._lua import Arg, Args, Key, redis_script
from ._redis import RedisClient


@redis_script
async def _schedule(
    redis: RedisClient,
    *,
    stream_key: Key[str],
    known_key: Key[str],
    parked_key: Key[str],
    queue_key: Key[str],
    stream_id_key: Key[str],
    runs_key: Key[str],
    state_channel: Key[str],
    task_key: Arg[str],
    when_timestamp: Arg[float],
    is_immediate: Arg[bool],
    replace: Arg[bool],
    reschedule_message_id: Arg[bytes],
    expected_generation: Arg[int],
    worker_group_name: Arg[str],
    state_payload: Arg[str],
    message: Args[dict[bytes, bytes]],
) -> bytes | str: ...


@redis_script
async def _claim(
    redis: RedisClient,
    *,
    runs_key: Key[str],
    progress_key: Key[str],
    known_key: Key[str],
    stream_id_key: Key[str],
    state_channel: Key[str],
    stream_key: Key[str],
    worker: Arg[str],
    started_at: Arg[str],
    generation: Arg[int],
    state_payload: Arg[str],
    key_json: Arg[str],
    worker_group_name: Arg[str],
    message_id: Arg[bytes],
) -> list[Any]: ...


@redis_script
async def _terminal(
    redis: RedisClient,
    *,
    runs_key: Key[str],
    state_channel: Key[str],
    progress_key: Key[str],
    stream_key: Key[str],
    generation: Arg[int],
    state: Arg[str],
    completed_at: Arg[str],
    ttl_seconds: Arg[int],
    state_payload: Arg[str],
    worker_group_name: Arg[str],
    message_id: Arg[bytes],
    extra_fields: Args[list[str]],
) -> bytes: ...


@redis_script
async def _cancel_task(
    redis: RedisClient,
    *,
    stream_key: Key[str],
    known_key: Key[str],
    parked_key: Key[str],
    queue_key: Key[str],
    stream_id_key: Key[str],
    runs_key: Key[str],
    progress_key: Key[str],
    state_channel: Key[str],
    task_key: Arg[str],
    completed_at: Arg[str],
    state_payload: Arg[str],
) -> bytes: ...

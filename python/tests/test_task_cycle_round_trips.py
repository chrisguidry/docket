"""How many Redis round-trips one task cycle costs.

A worker pays for every round-trip twice: about half a millisecond of Python
in redis-py and asyncio, and another Redis call the server has to serialize
against everything else on the key.  These tests pin the count for the two
shapes that matter, a plain task and a Perpetual that reschedules itself, so
a round-trip cannot creep back in unnoticed.
"""

# pyright: reportPrivateUsage=false

import asyncio
import contextlib
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Any, AsyncGenerator

from docket import Docket, Perpetual, Worker
from docket._execution_scripts import _claim, _schedule, _terminal
from tests.conftest import wait_until

SCRIPT_NAMES = {
    _claim.sha: "claim",
    _schedule.sha: "schedule",
    _terminal.sha: "terminal",
}


class RecordingClient:
    """A Redis client that appends ``(command, arguments)`` for every call.

    Wraps rather than replaces the real client, so the commands still run.
    Pipelines come back wrapped too, because a pipelined read is a read.
    """

    def __init__(self, client: Any, calls: list[tuple[str, tuple[Any, ...]]]) -> None:
        self._client = client
        self._calls = calls

    def __getattr__(self, name: str) -> Any:
        # Docket only ever calls methods on a client, so every attribute that
        # reaches here is one.
        attribute = getattr(self._client, name)

        def recording(*args: Any, **kwargs: Any) -> Any:
            self._calls.append((name, args))
            result = attribute(*args, **kwargs)
            return (
                RecordingClient(result, self._calls) if name == "pipeline" else result
            )

        return recording

    async def __aenter__(self) -> "RecordingClient":
        await self._client.__aenter__()
        return self

    async def __aexit__(self, *exception: Any) -> Any:
        return await self._client.__aexit__(*exception)


@asynccontextmanager
async def recording_commands(
    docket: Docket, key: str
) -> AsyncGenerator[list[str], None]:
    """Name every command that touches ``key``'s runs or progress hash.

    Script calls are named for the script they run, so the list reads as the
    sequence of round-trips one task key cost.  It is filled when the block
    closes, so assert on it after the block, not inside.
    """
    # Prime the server's script cache.  A script the server has never seen
    # costs a second EVALSHA after the NOSCRIPT reload, which is a one-time
    # cost of the fresh backend, not of the task cycle under test.
    async with docket.redis() as redis:
        for script in (_claim, _schedule, _terminal):
            await redis.script_load(script.lua)

    task_keys = {docket.key(f"runs:{key}"), docket.key(f"progress:{key}")}
    calls: list[tuple[str, tuple[Any, ...]]] = []
    commands: list[str] = []
    real_redis = docket.redis

    @asynccontextmanager
    async def recording_redis() -> AsyncGenerator[RecordingClient, None]:
        async with real_redis() as client:
            yield RecordingClient(client, calls)

    docket.redis = recording_redis  # pyright: ignore[reportAttributeAccessIssue]
    try:
        yield commands
    finally:
        del docket.redis
        commands.extend(
            SCRIPT_NAMES.get(arguments[0], command)
            for command, arguments in calls
            if task_keys & {a for a in arguments if isinstance(a, str)}
        )


async def test_a_plain_task_costs_two_round_trips(docket: Docket, worker: Worker):
    """Claiming and finishing a task is the whole cost of running it."""

    async def plain_task() -> None:
        pass

    await docket.add(plain_task, key="plain")()

    async with recording_commands(docket, "plain") as commands:
        await worker.run_until_finished()

    assert commands == ["claim", "terminal"]


async def test_a_perpetual_cycle_costs_three_round_trips(
    docket: Docket, worker: Worker
):
    """A Perpetual adds its own reschedule, and nothing else, to the cycle.

    Two cycles, six round-trips.  A cycle's terminal can land after the next
    cycle's claim, so the assertion counts the commands rather than ordering
    them.
    """
    runs: list[int] = []

    async def perpetual_task(
        perpetual: Perpetual = Perpetual(every=timedelta(0)),
    ) -> None:
        runs.append(len(runs))
        if len(runs) == 2:
            perpetual.after(timedelta(hours=1))

    await docket.add(perpetual_task, key="perpetual")()

    async def both_cycles_have_finished() -> bool:
        # An empty stream means the second cycle's terminal has ACKed and
        # deleted its message; the third is an hour away in the queue.
        async with docket.redis() as redis:
            return len(runs) == 2 and await redis.xlen(docket.stream_key) == 0

    async with recording_commands(docket, "perpetual") as commands:
        worker_task = asyncio.create_task(worker.run_until_finished())
        await wait_until(both_cycles_have_finished, description="two cycles")
        worker_task.cancel()
        with contextlib.suppress(asyncio.CancelledError):
            await worker_task

    assert sorted(commands) == [
        "claim",
        "claim",
        "schedule",
        "schedule",
        "terminal",
        "terminal",
    ]

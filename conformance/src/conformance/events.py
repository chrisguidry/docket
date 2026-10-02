"""The events that a scenario's tasks write, in the shape every agent writes."""

import asyncio
from collections.abc import Callable
from dataclasses import dataclass
from typing import cast

import redis.exceptions
from redis.asyncio import Redis


# What XREAD returns for one stream, with decode_responses.
Entries = list[tuple[str, list[tuple[str, dict[str, str]]]]]


@dataclass(frozen=True)
class Event:
    event: str
    task: str
    key: str
    attempt: int
    worker: str
    time: float


class Events:
    """Reads a scenario's event stream and keeps every event it has read."""

    def __init__(self, redis: Redis, scenario: str) -> None:
        self.redis = redis
        self.stream = f"conformance:{scenario}:events"
        self.seen: list[Event] = []
        self._last_id = "0-0"

    def matching(self, event: str, task: str | None = None) -> list[Event]:
        return [
            seen
            for seen in self.seen
            if seen.event == event and (task is None or seen.task == task)
        ]

    async def poll(self, seconds: float) -> None:
        """Read what arrives in the next ``seconds``.

        The chaos scenario restarts Redis, so a lost connection only means
        that nothing arrived.
        """
        try:
            response = cast(
                Entries,
                await self.redis.xread(
                    {self.stream: self._last_id},
                    block=int(seconds * 1000),
                    count=1000,
                ),
            )
        except redis.exceptions.ConnectionError:
            await asyncio.sleep(seconds)
            return

        for _, entries in response:
            for entry_id, fields in entries:
                self._last_id = entry_id
                self.seen.append(
                    Event(
                        event=fields["event"],
                        task=fields["task"],
                        key=fields["key"],
                        attempt=int(fields["attempt"]),
                        worker=fields["worker"],
                        time=float(fields["time"]),
                    )
                )

    async def wait_for(
        self, done: Callable[[], bool], timeout: float, waiting_for: str
    ) -> None:
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        while not done():
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise AssertionError(
                    f"Waited {timeout} s for {waiting_for}, "
                    f"and saw {len(self.seen)} events on {self.stream}"
                )
            await self.poll(min(remaining, 1.0))

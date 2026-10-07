"""What a scenario works with: Redis, the agents it starts, and their events."""

import asyncio
import json
import os
import random
import signal
from asyncio.subprocess import Process
from collections.abc import AsyncGenerator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Literal, cast
from urllib.parse import urlparse
from uuid import uuid4

from docker.models.containers import Container
from redis.asyncio import Redis
from redis.asyncio.client import PubSub

from .events import Events
from .implementations import Implementation

Role = Literal["produce", "worker"]

TERMINAL_STATES = {"completed", "failed", "cancelled"}


@dataclass
class Agent:
    implementation: Implementation
    role: Role
    process: Process
    log: Path

    @property
    def running(self) -> bool:
        return self.process.returncode is None


class Harness:
    def __init__(
        self,
        scenario: str,
        implementations: list[Implementation],
        url: str,
        container: Container,
        workdir: Path,
    ) -> None:
        self.scenario = scenario
        self.implementations = implementations
        self.url = url
        self.container = container
        self.workdir = workdir
        self.docket = f"conformance-{uuid4()}"
        address = urlparse(url)
        # redis-py 8 times out reads after 5 s, which would cut short the
        # blocking reads in Events.poll.
        self.redis = Redis(
            host=address.hostname or "localhost",
            port=address.port or 6379,
            decode_responses=True,
            socket_timeout=None,
            socket_connect_timeout=10,
        )
        self.events = Events(self.redis, scenario)
        self.agents: list[Agent] = []

    async def start(
        self,
        role: Role,
        *,
        implementation: Implementation | None = None,
        env: dict[str, str] | None = None,
    ) -> Agent:
        """Start an agent, with extra environment variables when given.

        Without an implementation, it chooses one of them at random.
        """
        implementation = implementation or random.choice(self.implementations)
        log = self.workdir / f"{len(self.agents):03d}-{role}-{implementation.name}.log"
        with log.open("wb") as output:
            process = await asyncio.create_subprocess_exec(
                *implementation.agent,
                role,
                "--scenario",
                self.scenario,
                "--url",
                self.url,
                "--docket",
                self.docket,
                stdout=output,
                stderr=output,
                env={**os.environ, "PYTHONUNBUFFERED": "1", **(env or {})},
            )
        agent = Agent(implementation, role, process, log)
        self.agents.append(agent)
        return agent

    async def produce(
        self,
        count: int = 1,
        timeout: float = 30,
        *,
        implementation: Implementation | None = None,
        env: dict[str, str] | None = None,
    ) -> None:
        """Run producers at the same time, each one to the end."""
        producers = [
            await self.start("produce", implementation=implementation, env=env)
            for _ in range(count)
        ]
        await self.exited(producers, timeout)
        codes = [producer.process.returncode for producer in producers]
        if any(codes):
            raise AssertionError(f"Producers exited {codes}")

    async def exited(self, agents: list[Agent], timeout: float) -> None:
        try:
            await asyncio.wait_for(
                asyncio.gather(*(agent.process.wait() for agent in agents)),
                timeout,
            )
        except asyncio.TimeoutError:
            raise AssertionError(f"Agents still running after {timeout} s") from None

    def signal(self, agent: Agent, signum: signal.Signals) -> None:
        if agent.running:
            agent.process.send_signal(signum)

    async def restart_redis(self) -> None:
        await asyncio.to_thread(self.container.restart, timeout=2)

    async def run_state(self, key: str) -> str | None:
        """The state that docket's Lua scripts keep for each task."""
        state = await self.redis.hget(f"{self.docket}:runs:{key}", "state")
        return cast(str | None, state)

    async def settled_state(self, key: str, timeout: float = 5) -> str | None:
        """The run state once it is terminal, or the last state seen.

        A task records its last event before its worker marks the run done,
        so the state can lag the events by a moment.
        """
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        state = await self.run_state(key)
        while state not in TERMINAL_STATES and loop.time() < deadline:
            await asyncio.sleep(0.1)
            state = await self.run_state(key)
        return state

    @asynccontextmanager
    async def published_states(self, key: str) -> AsyncGenerator[list[str]]:
        """Collects each state that docket publishes for ``key`` during the block.

        Redis delivers a message only to current subscribers, so this waits for
        the subscription to be confirmed before the block starts.  The list
        fills when the block ends.
        """
        pubsub: PubSub = self.redis.pubsub()  # pyright: ignore[reportUnknownMemberType]
        states: list[str] = []
        async with pubsub:
            await pubsub.subscribe(f"{self.docket}:state:{key}")
            await pubsub.get_message(timeout=5)
            yield states
            while message := cast(
                dict[str, Any] | None,
                await pubsub.get_message(ignore_subscribe_messages=True, timeout=1),
            ):
                states.append(json.loads(message["data"])["state"])

    async def stop(self) -> None:
        for agent in self.agents:
            self.signal(agent, signal.SIGKILL)
        await asyncio.gather(*(agent.process.wait() for agent in self.agents))
        await self.redis.aclose()

    def report(self, lines: int = 20) -> str:
        """The end of each agent's log, for a scenario that failed."""
        sections: list[str] = []
        for agent in self.agents:
            tail = agent.log.read_text(errors="replace").splitlines()[-lines:]
            sections.append(
                f"--- {agent.log.name} (exit {agent.process.returncode})\n"
                + "\n".join(tail)
            )
        return "\n".join(sections)

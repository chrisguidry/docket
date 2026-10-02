"""What a scenario works with: Redis, the agents it starts, and their events."""

import asyncio
import os
import random
import signal
from asyncio.subprocess import Process
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, cast
from urllib.parse import urlparse
from uuid import uuid4

from docker.models.containers import Container
from redis.asyncio import Redis

from .events import Events
from .implementations import Implementation

Role = Literal["produce", "worker"]


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

    async def start(self, role: Role) -> Agent:
        """Start an agent from one of the implementations, chosen at random."""
        implementation = random.choice(self.implementations)
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
                env={**os.environ, "PYTHONUNBUFFERED": "1"},
            )
        agent = Agent(implementation, role, process, log)
        self.agents.append(agent)
        return agent

    async def produce(self, timeout: float = 30) -> None:
        """Run one producer to the end."""
        agent = await self.start("produce")
        await self.exited([agent], timeout)
        if agent.process.returncode != 0:
            raise AssertionError(f"The producer exited with {agent.process.returncode}")

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

"""Tests for worker liveness cleanup across overlapping worker sessions."""

from __future__ import annotations

import asyncio
from datetime import timedelta

from docket import Docket, Worker
from tests.conftest import wait_until


async def test_same_named_workers_remove_all_advertised_tasks_on_final_cleanup(
    docket: Docket,
    redis_url: str,
):
    async def local_task() -> None: ...

    async def peer_task() -> None: ...

    docket.register(local_task)

    async with Docket(name=docket.name, url=redis_url) as peer_docket:
        peer_docket.tasks.clear()
        peer_docket.register(peer_task)

        async with (
            Worker(
                docket,
                name="shared-capability-worker",
                minimum_check_interval=timedelta(milliseconds=5),
                scheduling_resolution=timedelta(milliseconds=5),
            ) as local_worker,
            Worker(
                peer_docket,
                name="shared-capability-worker",
                minimum_check_interval=timedelta(milliseconds=5),
                scheduling_resolution=timedelta(milliseconds=5),
            ) as peer_worker,
        ):
            local_run = asyncio.create_task(local_worker.run_forever())
            peer_run = asyncio.create_task(peer_worker.run_forever())

            async def both_task_capabilities_are_advertised() -> bool:
                local_workers = await docket.task_workers("local_task")
                peer_workers = await docket.task_workers("peer_task")
                return {worker.name for worker in local_workers} == {
                    "shared-capability-worker"
                } and {worker.name for worker in peer_workers} == {
                    "shared-capability-worker"
                }

            try:
                await wait_until(
                    both_task_capabilities_are_advertised,
                    timeout=2.0,
                    description="same-name task capability advertisements",
                )

                local_run.cancel()
                await asyncio.gather(local_run, return_exceptions=True)

                async def only_peer_capability_remains() -> bool:
                    local_workers = await docket.task_workers("local_task")
                    peer_workers = await docket.task_workers("peer_task")
                    workers = await docket.workers()
                    return (
                        local_workers == []
                        and {worker.name for worker in peer_workers}
                        == {"shared-capability-worker"}
                        and {
                            task
                            for worker in workers
                            if worker.name == "shared-capability-worker"
                            for task in worker.tasks
                        }
                        == {"peer_task"}
                    )

                await wait_until(
                    only_peer_capability_remains,
                    timeout=2.0,
                    description="exiting-only capability cleanup",
                )

                peer_run.cancel()
                await asyncio.gather(peer_run, return_exceptions=True)
            finally:
                local_run.cancel()
                peer_run.cancel()
                await asyncio.gather(local_run, peer_run, return_exceptions=True)

    assert await docket.task_workers("local_task") == []
    assert await docket.task_workers("peer_task") == []

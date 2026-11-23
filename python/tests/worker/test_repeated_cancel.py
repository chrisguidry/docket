"""A cancellation that reaches the worker twice.

A subscription that moves to a new connection after a credential rotation can
deliver a cancel message twice.
"""

# pyright: reportPrivateUsage=false

import asyncio

from docket import Docket, Worker
from docket.worker import PubSubMessage


def cancel_message(docket: Docket, key: str) -> PubSubMessage:
    return {
        "type": "pmessage",
        "pattern": docket.key("cancel:*").encode(),
        "channel": docket.key(f"cancel:{key}").encode(),
        "data": key.encode(),
    }


async def test_a_repeated_cancel_leaves_the_tasks_cleanup_alone(
    docket: Docket, worker: Worker
):
    started = asyncio.Event()
    cleaning_up = asyncio.Event()
    release = asyncio.Event()
    cleaned_up = asyncio.Event()

    async def task_with_slow_cleanup():
        started.set()
        try:
            await asyncio.sleep(60)
        except asyncio.CancelledError:
            cleaning_up.set()
            await release.wait()
            cleaned_up.set()
            raise

    docket.register(task_with_slow_cleanup)
    execution = await docket.add(task_with_slow_cleanup)()
    running = asyncio.create_task(worker.run_until_finished())
    await asyncio.wait_for(started.wait(), timeout=5)

    await worker._handle_cancellation(cancel_message(docket, execution.key))
    await asyncio.wait_for(cleaning_up.wait(), timeout=5)
    await worker._handle_cancellation(cancel_message(docket, execution.key))

    release.set()
    await asyncio.wait_for(running, timeout=5)
    assert cleaned_up.is_set()

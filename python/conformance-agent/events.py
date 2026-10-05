import time

from docket import Docket


async def record(
    docket: Docket,
    scenario: str,
    event: str,
    *,
    task: str,
    key: str,
    attempt: int = 0,
    worker: str = "",
) -> None:
    """Add an event to the scenario's stream, in the shape every agent writes.

    The producer has no attempt or worker, so it writes 0 and "".
    """
    async with docket.redis() as redis:
        await redis.xadd(
            f"conformance:{scenario}:events",
            {
                "event": event,
                "task": task,
                "key": key,
                "attempt": attempt,
                "worker": worker,
                "time": time.time(),
            },
        )

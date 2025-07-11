import asyncio
from docket import ConcurrencyLimit, Docket, Worker


async def test_basic_concurrency_limit(docket: Docket, worker: Worker):
    """Test basic concurrency limiting functionality."""
    results: list[str] = []

    async def test_task(
        customer_id: int,
        concurrency: ConcurrencyLimit = ConcurrencyLimit(
            "customer_id", max_concurrent=1
        ),
    ):
        results.append(f"start_{customer_id}")
        await asyncio.sleep(0.01)  # Short delay
        results.append(f"end_{customer_id}")

    # Schedule 2 tasks for the same customer
    await docket.add(test_task)(customer_id=1)
    await docket.add(test_task)(customer_id=1)

    # Run worker
    await worker.run_until_finished()

    # Should have 4 results: start_1, end_1, start_1, end_1
    assert len(results) == 4
    assert results[0] == "start_1"
    assert results[1] == "end_1"
    assert results[2] == "start_1"
    assert results[3] == "end_1"

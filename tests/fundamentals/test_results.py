"""Tests for task result storage and retrieval."""

from docket import Docket, Worker


async def test_task_results_can_be_stored_and_retrieved(docket: Docket, worker: Worker):
    """Test that string results are stored and retrievable."""
    result_value = "hello world"

    async def returns_str() -> str:
        return result_value

    docket.register(returns_str)
    execution = await docket.add(returns_str)()
    await worker.run_until_finished()

    # Retrieve result
    result = await execution.get_result()
    assert result == result_value

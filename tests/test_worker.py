import pytest
from redis import RedisError

from docket.docket import Docket
from docket.worker import Worker


async def test_worker_aenter_propagates_connection_errors():
    """The worker should propagate Redis connection errors"""

    docket = Docket(name="test-docket", host="nonexistent-host", port=12345)
    worker = Worker(docket)
    with pytest.raises(RedisError):
        await worker.__aenter__()

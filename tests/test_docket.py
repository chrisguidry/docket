import pytest
import redis.exceptions

from docket.docket import Docket


async def test_docket_aenter_propagates_connection_errors():
    """The docket should propagate Redis connection errors"""

    docket = Docket(name="test-docket", url="redis://nonexistent-host:12345/0")
    with pytest.raises(redis.exceptions.RedisError):
        await docket.__aenter__()

    await docket.__aexit__(None, None, None)

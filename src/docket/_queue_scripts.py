"""Atomic Redis operations used by reliable message queues."""

from ._lua import Arg, Key, redis_script
from ._redis import RedisClient


@redis_script
async def put_message(
    redis: RedisClient,
    *,
    stream_key: Key[str],
    deduplication_key: Key[str],
    message_key: Arg[str],
    data: Arg[bytes],
    max_size: Arg[int],
    now_timestamp: Arg[float],
) -> bytes:
    """
    redis.call('ZREMRANGEBYSCORE', deduplication_key, 1, now_timestamp)
    if redis.call('ZSCORE', deduplication_key, message_key) then
        return 'DUPLICATE'
    end
    if max_size > 0 and redis.call('XLEN', stream_key) >= max_size then
        return 'FULL'
    end

    local message_id = redis.call(
        'XADD', stream_key, '*', 'key', message_key, 'data', data
    )
    redis.call('EXPIRE', stream_key, 2147483647)
    redis.call('ZADD', deduplication_key, 0, message_key)
    redis.call('EXPIRE', deduplication_key, 2147483647)
    return message_id
    """
    ...


@redis_script
async def acknowledge_message(
    redis: RedisClient,
    *,
    stream_key: Key[str],
    deduplication_key: Key[str],
    group_name: Arg[str],
    message_id: Arg[bytes],
    message_key: Arg[str],
    idle_ttl_seconds: Arg[int],
    acknowledged_until: Arg[float],
) -> int:
    """
    redis.call('XACK', stream_key, group_name, message_id)
    redis.call('XDEL', stream_key, message_id)
    if acknowledged_until > 0 then
        redis.call('ZADD', deduplication_key, acknowledged_until, message_key)
        redis.call('EXPIRE', deduplication_key, 2147483647)
    else
        redis.call('ZREM', deduplication_key, message_key)
    end
    if redis.call('XLEN', stream_key) == 0 then
        redis.call('EXPIRE', stream_key, idle_ttl_seconds)
    end
    return 1
    """
    ...


@redis_script
async def release_message(
    redis: RedisClient,
    *,
    source_stream_key: Key[str],
    destination_stream_key: Key[str],
    group_name: Arg[str],
    message_id: Arg[bytes],
    message_key: Arg[str],
    data: Arg[bytes],
    max_size: Arg[int],
    idle_ttl_seconds: Arg[int],
) -> bytes:
    """
    if source_stream_key ~= destination_stream_key
        and max_size > 0
        and redis.call('XLEN', destination_stream_key) >= max_size
    then
        return 'FULL'
    end

    redis.call('XACK', source_stream_key, group_name, message_id)
    redis.call('XDEL', source_stream_key, message_id)
    local new_message_id = redis.call(
        'XADD', destination_stream_key, '*', 'key', message_key, 'data', data
    )
    redis.call('EXPIRE', destination_stream_key, 2147483647)
    if redis.call('XLEN', source_stream_key) == 0 then
        redis.call('EXPIRE', source_stream_key, idle_ttl_seconds)
    end
    return new_message_id
    """
    ...

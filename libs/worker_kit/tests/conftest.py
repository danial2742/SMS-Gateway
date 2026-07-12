import pytest
from gateway_common.redis.client import make_redis


@pytest.fixture
async def redis_client(redis_url):
    redis = make_redis(redis_url)
    try:
        yield redis
    finally:
        await redis.aclose()

from redis.asyncio import Redis, from_url


def make_redis(redis_url: str) -> Redis:
    # socket_timeout must exceed any BLPOP block timeout callers pass —
    # otherwise the client's own read timeout fires before Redis's BLPOP
    # timeout does, surfacing as a spurious redis.exceptions.TimeoutError
    # instead of the normal "no message" nil response.
    return from_url(redis_url, decode_responses=True, socket_timeout=60)

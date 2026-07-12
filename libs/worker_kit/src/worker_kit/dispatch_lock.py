from redis.asyncio import Redis

_LOCK_VALUE = "IN_FLIGHT"
DISPATCH_LOCK_TTL_SECONDS = 60


def _lock_key(sms_id: str) -> str:
    return f"dispatch:lock:{sms_id}"


async def try_acquire_dispatch_lock(
    redis: Redis, sms_id: str, *, ttl_seconds: int = DISPATCH_LOCK_TTL_SECONDS
) -> bool:
    """Guards against concurrent redelivery of the same sms_id (e.g. a Kafka
    consumer-group rebalance) dispatching to the operator twice — SET NX PX,
    same pattern as gateway_common.redis.idempotency's fast lock.
    """
    return bool(await redis.set(_lock_key(sms_id), _LOCK_VALUE, nx=True, px=ttl_seconds * 1000))


async def release_dispatch_lock(redis: Redis, sms_id: str) -> None:
    await redis.delete(_lock_key(sms_id))

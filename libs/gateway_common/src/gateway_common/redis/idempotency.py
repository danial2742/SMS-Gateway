from redis.asyncio import Redis

_LOCK_VALUE = "IN_PROGRESS"


def _lock_key(tenant_id: str, idempotency_key: str) -> str:
    return f"idem:{tenant_id}:{idempotency_key}"


async def try_acquire_lock(
    redis: Redis, tenant_id: str, idempotency_key: str, *, ttl_seconds: int
) -> bool:
    """Fast in-flight lock (ADR-009): SET NX PX. True if this caller won the
    lock (first request for this key); False if another request with the
    same key is already in flight — durable guarantee still comes from the
    Postgres unique constraint on (tenant_id, idempotency_key), this is only
    the fast path.
    """
    return bool(
        await redis.set(_lock_key(tenant_id, idempotency_key), _LOCK_VALUE, nx=True, px=ttl_seconds * 1000)
    )


async def release_lock(redis: Redis, tenant_id: str, idempotency_key: str) -> None:
    await redis.delete(_lock_key(tenant_id, idempotency_key))

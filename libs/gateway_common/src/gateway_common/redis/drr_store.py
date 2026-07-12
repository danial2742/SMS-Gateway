import json
from typing import Protocol, cast

from redis.asyncio import Redis
from redis.exceptions import TimeoutError as RedisTimeoutError

ACTIVE_TENANTS_KEY = "active_tenants"

# Combines RPUSH + ZADD NX into one atomic call — done as two separate Redis
# calls, a consumer's dequeue-empty-check/remove_active race could run
# between them and orphan a message that was just enqueued (see the
# RedisDrrStore.enqueue docstring below).
_ENQUEUE_LUA = """
local queue_key = KEYS[1]
local active_key = KEYS[2]
local tenant_id = ARGV[1]
local message = ARGV[2]

redis.call("RPUSH", queue_key, message)
redis.call("ZADD", active_key, "NX", 0, tenant_id)
return 1
"""


def _tenant_queue_key(tenant_id: str) -> str:
    return f"queue:tenant:{tenant_id}"


def dispatch_ready_key(tier: str) -> str:
    return f"dispatch:ready:{tier}"


class TenantQueuePort(Protocol):
    """Port the pure DRR algorithm (fair_scheduler/drr.py) is coded against.
    Structural typing — RedisDrrStore below and any in-memory test fake both
    satisfy it without inheritance.
    """

    async def enqueue(self, tenant_id: str, message: dict) -> None: ...
    async def dequeue(self, tenant_id: str) -> dict | None: ...
    async def requeue_front(self, tenant_id: str, message: dict) -> None: ...
    async def is_empty(self, tenant_id: str) -> bool: ...
    async def active_tenants(self) -> list[str]: ...
    async def add_active(self, tenant_id: str) -> None: ...
    async def remove_active(self, tenant_id: str) -> None: ...


class RedisDrrStore:
    """Redis-backed DRR state: `queue:tenant:{id}` Lists hold pending
    messages per tenant, `active_tenants` ZSET (score always 0 — used purely
    for O(1) membership/NX-dedup via ZADD NX, not ordering; iteration order
    for equal scores is not a documented arrival guarantee) tracks which
    tenants currently have a non-empty queue.
    """

    def __init__(self, redis: Redis) -> None:
        self._redis = redis
        self._enqueue_script = redis.register_script(_ENQUEUE_LUA)

    async def enqueue(self, tenant_id: str, message: dict) -> None:
        # RPUSH + ZADD NX as one atomic Lua call — as two separate round
        # trips, a consumer racing is_empty()/remove_active() between them
        # could remove the tenant from active_tenants right after this
        # RPUSH lands, orphaning the message until some later unrelated
        # enqueue for the same tenant re-adds it.
        await self._enqueue_script(
            keys=[_tenant_queue_key(tenant_id), ACTIVE_TENANTS_KEY],
            args=[tenant_id, json.dumps(message)],
        )

    async def dequeue(self, tenant_id: str) -> dict | None:
        raw = await self._redis.lpop(_tenant_queue_key(tenant_id))
        return json.loads(cast(str, raw)) if raw is not None else None

    async def requeue_front(self, tenant_id: str, message: dict) -> None:
        # LPUSH puts it back at the head, where LPOP (dequeue) reads next —
        # undoes a dequeue that a DRR round couldn't yet afford.
        await self._redis.lpush(_tenant_queue_key(tenant_id), json.dumps(message))

    async def is_empty(self, tenant_id: str) -> bool:
        return await self._redis.llen(_tenant_queue_key(tenant_id)) == 0

    async def active_tenants(self) -> list[str]:
        return cast(list[str], await self._redis.zrange(ACTIVE_TENANTS_KEY, 0, -1))

    async def add_active(self, tenant_id: str) -> None:
        await self._redis.zadd(ACTIVE_TENANTS_KEY, {tenant_id: 0}, nx=True)

    async def remove_active(self, tenant_id: str) -> None:
        await self._redis.zrem(ACTIVE_TENANTS_KEY, tenant_id)

    async def push_ready(self, tier: str, message: dict) -> None:
        await self._redis.rpush(dispatch_ready_key(tier), json.dumps(message))

    async def blpop_ready(self, tier: str, timeout: int = 5) -> dict | None:
        try:
            result = await self._redis.blpop([dispatch_ready_key(tier)], timeout=timeout)
        except RedisTimeoutError:
            # Client-side socket read timeout racing the server-side BLPOP
            # timeout — equivalent to the normal nil/no-message outcome.
            return None
        if result is None:
            return None
        _key, raw = result
        return json.loads(raw)

import hashlib
import json
import uuid

from gateway_common.db.models import IdempotencyKey
from gateway_common.domain.enums import IdempotencyStatus
from gateway_common.domain.errors import IdempotencyKeyInFlightError, IdempotencyKeyReusedError
from gateway_common.redis.idempotency import release_lock, try_acquire_lock
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession


def request_hash(body: dict) -> str:
    canonical = json.dumps(body, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode()).hexdigest()


async def check_idempotency(
    session: AsyncSession,
    redis: Redis,
    *,
    tenant_id: uuid.UUID,
    idempotency_key: str,
    body_hash: str,
    ttl_seconds: int,
) -> dict | None:
    """ADR-009: Redis SET NX PX fast lock + Postgres unique-constraint
    durable backstop.

    Returns a cached response snapshot if this is a safe replay of an
    already-completed request (same key, same body) — the caller should
    return that snapshot as-is. Returns None if the caller should proceed to
    execute the request fresh (and is responsible for releasing the lock via
    `release_idempotency_lock`, and for persisting the IdempotencyKey row as
    part of its own atomic transaction).

    Raises IdempotencyKeyInFlightError (409) or IdempotencyKeyReusedError (422).
    """
    got_lock = await try_acquire_lock(redis, str(tenant_id), idempotency_key, ttl_seconds=ttl_seconds)

    existing = await session.get(IdempotencyKey, (tenant_id, idempotency_key))
    if existing is not None:
        if got_lock:
            await release_lock(redis, str(tenant_id), idempotency_key)
        if existing.request_hash != body_hash:
            raise IdempotencyKeyReusedError(
                "idempotency key reused with a different request body"
            )
        # Currently unreachable: every write path (submission.py,
        # wallet_service.py) inserts its IdempotencyKey row already as
        # COMPLETED, in the same transaction as the write, never as an
        # intermediate IN_PROGRESS row. Kept as forward-compatible defensive
        # code — a future write path that needs pre-commit in-flight
        # visibility can rely on this branch already being correct, rather
        # than that guarantee being re-added later without this check.
        if existing.status == IdempotencyStatus.IN_PROGRESS:
            raise IdempotencyKeyInFlightError("a request with this idempotency key is in flight")
        return existing.response_snapshot

    if not got_lock:
        raise IdempotencyKeyInFlightError("a request with this idempotency key is in flight")

    return None


async def release_idempotency_lock(redis: Redis, tenant_id: uuid.UUID, idempotency_key: str) -> None:
    await release_lock(redis, str(tenant_id), idempotency_key)

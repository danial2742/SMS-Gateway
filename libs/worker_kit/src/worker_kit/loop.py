import asyncio
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime

from gateway_common.db.engine import DatabaseSessions
from gateway_common.db.models import Sms
from gateway_common.domain.enums import SmsStatus, Tier
from gateway_common.kafka.producer import GatewayProducer
from gateway_common.logging import get_logger
from gateway_common.metrics import (
    operator_dispatch_duration_seconds,
    operator_dispatch_result_total,
)
from gateway_common.operator.protocol import OperatorClient
from gateway_common.request_context import request_id_var, tenant_id_var
from redis.asyncio import Redis
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from worker_kit.dispatch_lock import release_dispatch_lock, try_acquire_dispatch_lock
from worker_kit.dlq import publish_to_dlq
from worker_kit.refund import refund_and_mark_dead_letter
from worker_kit.retry import backoff_seconds, is_retryable, should_retry

logger = get_logger()

_TERMINAL_STATUSES = frozenset(
    {SmsStatus.SENT_TO_OPERATOR, SmsStatus.DELIVERED, SmsStatus.FAILED, SmsStatus.FAILED_DEAD_LETTER}
)


@dataclass(frozen=True)
class WorkerConfig:
    tier: Tier
    retry_topic: str  # own tier's topic — retries republish here, per docs/queue.md
    dlq_topic: str


async def process_dispatch(
    db_sessions: DatabaseSessions,
    producer: GatewayProducer,
    operator: OperatorClient,
    config: WorkerConfig,
    payload: dict,
    *,
    redis: Redis,
    attempt_count: int,
    request_id: str,
) -> None:
    """docs/queue.md Worker Pool — the shared loop shape for both tiers:
    receive, status-check (internal-redelivery idempotency guard), dispatch,
    on-failure classify+retry-or-DLQ. Stateless between calls — a crash
    mid-call loses at most this one message, which Kafka redelivers.
    """
    # Rebinds the request_id that arrived via Kafka headers into log context
    # for this call, so every log line this dispatch emits is queryable by
    # the same request_id the API assigned — the end-to-end correlation
    # observability.md promises (single worker process, one message at a
    # time, so a plain set() here can't leak across concurrent dispatches).
    request_id_var.set(request_id)
    tenant_id_var.set(payload["tenant_id"])

    sms_id = uuid.UUID(payload["sms_id"])

    async with db_sessions.primary() as session:
        sms = await _fetch_sms(session, sms_id)
        if sms is None:
            logger.warning("worker_sms_not_found", sms_id=str(sms_id))
            return
        if sms.status in _TERMINAL_STATUSES:
            logger.info("worker_skip_terminal", sms_id=str(sms_id), status=sms.status)
            return
        created_at = sms.created_at
        cost = sms.cost

    # Concurrent redelivery of the same sms_id (e.g. a Kafka consumer-group
    # rebalance racing itself) can otherwise pass the terminal-status check
    # above twice and dispatch to the operator twice — this lock is the
    # single-flight guard for the network call + DB write that follows.
    if not await try_acquire_dispatch_lock(redis, str(sms_id)):
        logger.warning("worker_dispatch_lock_contended", sms_id=str(sms_id), tier=config.tier)
        return

    try:
        start = time.monotonic()
        try:
            operator_message_id = await operator.send(
                recipient=payload["recipient"], message=payload["message_body"], sms_id=str(sms_id)
            )
        except Exception as exc:  # noqa: BLE001 — classified below, not swallowed
            operator_dispatch_duration_seconds.labels(tier=config.tier).observe(
                time.monotonic() - start
            )
            await _handle_failure(
                db_sessions,
                producer,
                config,
                payload,
                exc,
                attempt_count=attempt_count,
                request_id=request_id,
                created_at=created_at,
                refund_amount=cost,
            )
            return

        operator_dispatch_duration_seconds.labels(tier=config.tier).observe(time.monotonic() - start)
        operator_dispatch_result_total.labels(tier=config.tier, outcome="success").inc()

        async with db_sessions.primary() as session:
            await session.execute(
                update(Sms)
                .where(Sms.id == sms_id, Sms.created_at == created_at)
                .values(
                    status=SmsStatus.SENT_TO_OPERATOR,
                    sent_at=_now(),
                    operator_message_id=operator_message_id,
                    attempt_count=attempt_count + 1,
                )
            )
            await session.commit()
        logger.info("worker_dispatched", sms_id=str(sms_id), tier=config.tier)
    finally:
        await release_dispatch_lock(redis, str(sms_id))


async def process_dispatch_safely(
    db_sessions: DatabaseSessions,
    producer: GatewayProducer,
    operator: OperatorClient,
    config: WorkerConfig,
    payload: dict,
    *,
    redis: Redis,
    attempt_count: int,
    request_id: str,
) -> None:
    """Last-resort guard around process_dispatch for callers whose message
    source has limited or no redelivery on crash: normal_worker's Redis
    BLPOP is destructive (a lost message there is gone for good), and
    express_worker fans one Kafka BatchAccepted message out into N
    process_dispatch calls, where one bad recipient must not abort the rest
    of the batch. Anything process_dispatch itself doesn't already classify
    and handle (a DB error, a producer.send failure, a bug) lands here:
    logged and routed straight to DLQ rather than silently lost or left to
    crash the caller's loop.
    """
    try:
        await process_dispatch(
            db_sessions,
            producer,
            operator,
            config,
            payload,
            redis=redis,
            attempt_count=attempt_count,
            request_id=request_id,
        )
    except Exception as exc:  # noqa: BLE001 — deliberately catch-all, see docstring
        sms_id = payload.get("sms_id")
        logger.error(
            "worker_dispatch_failed_unexpectedly", tier=config.tier, sms_id=sms_id, error=str(exc)[:500]
        )
        try:
            tenant_id = uuid.UUID(payload["tenant_id"])
        except (KeyError, ValueError):
            logger.critical("worker_dispatch_guard_missing_tenant_id", sms_id=sms_id)
            return
        await publish_to_dlq(
            producer,
            config.dlq_topic,
            payload,
            tenant_id=tenant_id,
            request_id=request_id,
            error=str(exc)[:500],
            attempt_count=attempt_count,
            first_attempted_at=payload.get("first_attempted_at", _now_iso()),
            last_attempted_at=_now_iso(),
            reason="unexpected_worker_error",
            tier=config.tier,
        )


async def _handle_failure(
    db_sessions: DatabaseSessions,
    producer: GatewayProducer,
    config: WorkerConfig,
    payload: dict,
    exc: Exception,
    *,
    attempt_count: int,
    request_id: str,
    created_at: datetime,
    refund_amount: int,
) -> None:
    sms_id = uuid.UUID(payload["sms_id"])
    tenant_id = uuid.UUID(payload["tenant_id"])
    retryable = is_retryable(exc)
    outcome = "retryable_failure" if retryable else "non_retryable"
    operator_dispatch_result_total.labels(tier=config.tier, outcome=outcome).inc()

    if retryable and should_retry(config.tier, attempt_count):
        delay = backoff_seconds(config.tier, attempt_count)
        await asyncio.sleep(delay)

        next_payload = {**payload, "first_attempted_at": payload.get("first_attempted_at", _now_iso())}
        await producer.send(
            config.retry_topic,
            next_payload,
            tenant_id=tenant_id,
            request_id=request_id,
            attempt_count=attempt_count + 1,
        )
        async with db_sessions.primary() as session:
            await session.execute(
                update(Sms)
                .where(Sms.id == sms_id, Sms.created_at == created_at)
                .values(attempt_count=attempt_count + 1, last_error=str(exc)[:500])
            )
            await session.commit()
        logger.warning("worker_retry_scheduled", sms_id=str(sms_id), attempt_count=attempt_count + 1)
        return

    async with db_sessions.primary() as session:
        refunded = await refund_and_mark_dead_letter(
            session,
            sms_id=sms_id,
            created_at=created_at,
            tenant_id=tenant_id,
            refund_amount=refund_amount,
        )

    if refunded:
        await publish_to_dlq(
            producer,
            config.dlq_topic,
            payload,
            tenant_id=tenant_id,
            request_id=request_id,
            error=str(exc)[:500],
            attempt_count=attempt_count,
            first_attempted_at=payload.get("first_attempted_at", _now_iso()),
            last_attempted_at=_now_iso(),
            reason="non_retryable" if not retryable else "retries_exhausted",
            tier=config.tier,
        )
        logger.error("worker_dead_lettered", sms_id=str(sms_id), tier=config.tier)


async def _fetch_sms(session: AsyncSession, sms_id: uuid.UUID) -> Sms | None:
    result = await session.execute(select(Sms).where(Sms.id == sms_id))
    return result.scalar_one_or_none()


def _now() -> datetime:
    return datetime.now(UTC)


def _now_iso() -> str:
    return _now().isoformat()

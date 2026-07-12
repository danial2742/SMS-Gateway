import asyncio

from gateway_common.db.engine import DatabaseSessions
from gateway_common.db.models import OutboxEvent
from gateway_common.kafka.producer import GatewayProducer
from gateway_common.logging import get_logger
from gateway_common.metrics import outbox_oldest_unpublished_age_seconds, outbox_unpublished_rows
from gateway_common.shutdown import GracefulShutdown
from sqlalchemy import func, select, update
from sqlalchemy.ext.asyncio import AsyncSession

logger = get_logger()


async def poll_and_publish(
    session: AsyncSession, producer: GatewayProducer, *, batch_size: int
) -> int:
    """One poll cycle: SELECT ... FOR UPDATE SKIP LOCKED so multiple relay
    instances compete for disjoint row sets without blocking each other
    (docs/queue.md pipeline diagram, docs/database.md idx_outbox_unpublished).
    """
    rows = (
        (
            await session.execute(
                select(OutboxEvent)
                .where(OutboxEvent.published_at.is_(None))
                .order_by(OutboxEvent.created_at)
                .limit(batch_size)
                .with_for_update(skip_locked=True)
            )
        )
        .scalars()
        .all()
    )
    if not rows:
        await session.commit()
        return 0

    for row in rows:
        # Stamp event_type onto the wire payload — consumers (fair-scheduler
        # ingest loop, express-worker) branch on it to tell SmsAccepted from
        # BatchAccepted without guessing from field presence.
        wire_payload = {**row.payload, "event_type": row.event_type}
        await producer.send(
            row.topic,
            wire_payload,
            tenant_id=row.partition_key,
            request_id=row.payload.get("request_id", ""),
            attempt_count=0,
        )

    ids = [row.id for row in rows]
    await session.execute(
        update(OutboxEvent)
        .where(OutboxEvent.id.in_(ids))
        .values(published_at=func.now(), attempts=OutboxEvent.attempts + 1)
    )
    await session.commit()
    return len(rows)


async def emit_backlog_metrics(session: AsyncSession) -> None:
    result = await session.execute(
        select(
            func.count(),
            func.extract("epoch", func.now() - func.min(OutboxEvent.created_at)),
        ).where(OutboxEvent.published_at.is_(None))
    )
    count, age = result.one()
    outbox_unpublished_rows.set(count or 0)
    outbox_oldest_unpublished_age_seconds.set(age or 0)


async def run(
    db_sessions: DatabaseSessions,
    producer: GatewayProducer,
    shutdown: GracefulShutdown,
    *,
    poll_interval_seconds: float,
    batch_size: int,
) -> None:
    while not shutdown.should_stop:
        async with db_sessions.primary() as session:
            published = await poll_and_publish(session, producer, batch_size=batch_size)
            await emit_backlog_metrics(session)

        if published == 0:
            logger.debug("relay_idle")
            await asyncio.sleep(poll_interval_seconds)
        else:
            logger.info("relay_published", count=published)

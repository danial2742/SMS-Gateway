import uuid
from collections.abc import AsyncIterator

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from gateway_common.db.models import Batch, Sms

# docs/database.md "Why batch fan-out doesn't insert to outbox_events per
# recipient": the write path emits ONE BatchAccepted outbox row regardless of
# batch size. Whichever consumer (fair-scheduler ingest loop / express-worker
# consume loop) first sees that event calls this routine to expand it into
# per-recipient dispatch units — see docs/queue.md Ordering guarantees
# "Batch children" and the plan's resolution for the missing 6th "fan-out
# service" the docs describe generically.
DEFAULT_PAGE_SIZE = 1000


async def expand_batch_accepted(
    session: AsyncSession, batch_id: uuid.UUID, *, page_size: int = DEFAULT_PAGE_SIZE
) -> AsyncIterator[dict]:
    """Paginates `sms WHERE batch_id = $1` in scan order and yields one
    per-recipient dispatch payload per row, body populated from the parent
    batch (storage optimization: a 1M-recipient batch stores the body once).
    """
    batch = (await session.execute(select(Batch).where(Batch.id == batch_id))).scalar_one()

    last_id: uuid.UUID | None = None
    while True:
        query = select(Sms).where(Sms.batch_id == batch_id)
        if last_id is not None:
            query = query.where(Sms.id > last_id)
        query = query.order_by(Sms.id).limit(page_size)

        rows = (await session.execute(query)).scalars().all()
        if not rows:
            return

        for row in rows:
            last_id = row.id
            yield {
                "sms_id": str(row.id),
                "tenant_id": str(row.tenant_id),
                "batch_id": str(batch_id),
                "recipient": row.recipient,
                "message_body": row.message_body or batch.message_body,
                "priority": row.priority,
                "cost": row.cost,
            }

        if len(rows) < page_size:
            return

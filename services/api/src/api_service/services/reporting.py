import uuid
from datetime import datetime

from gateway_common.db.models import Batch, Sms
from gateway_common.domain.enums import Priority, SmsStatus
from gateway_common.domain.errors import BatchNotFoundError, SmsNotFoundError
from gateway_common.pagination import Cursor
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession


async def get_sms_detail(session: AsyncSession, tenant_id: uuid.UUID, sms_id: uuid.UUID) -> Sms:
    # Scoped by tenant_id, never by id alone (docs/security.md Authorization
    # boundaries) — prevents one tenant enumerating another's message history.
    result = await session.execute(
        select(Sms).where(Sms.id == sms_id, Sms.tenant_id == tenant_id)
    )
    sms = result.scalar_one_or_none()
    if sms is None:
        raise SmsNotFoundError(f"no sms {sms_id} for this tenant")
    return sms


async def get_batch_detail(session: AsyncSession, tenant_id: uuid.UUID, batch_id: uuid.UUID) -> dict:
    result = await session.execute(
        select(Batch).where(Batch.id == batch_id, Batch.tenant_id == tenant_id)
    )
    batch = result.scalar_one_or_none()
    if batch is None:
        raise BatchNotFoundError(f"no batch {batch_id} for this tenant")

    # docs/api.md: sent_count/failed_count are derived from a periodic
    # aggregate over child sms rows, eventually consistent with dispatch, not
    # computed synchronously per request. This build computes it live from
    # the read replica as a v1 simplification of that aggregation job — same
    # eventual-consistency property (replica lag), simpler moving parts.
    counts = await session.execute(
        select(Sms.status, func.count()).where(Sms.batch_id == batch_id).group_by(Sms.status)
    )
    by_status: dict[SmsStatus, int] = {status: count for status, count in counts.all()}
    sent_count = by_status.get(SmsStatus.SENT_TO_OPERATOR, 0) + by_status.get(SmsStatus.DELIVERED, 0)
    failed_count = by_status.get(SmsStatus.FAILED, 0) + by_status.get(SmsStatus.FAILED_DEAD_LETTER, 0)

    return {
        "batch_id": batch.id,
        "status": batch.status,
        "recipient_count": batch.recipient_count,
        "sent_count": sent_count,
        "failed_count": failed_count,
        "created_at": batch.created_at,
    }


async def list_sms_report(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    *,
    from_: datetime,
    to: datetime,
    status: SmsStatus | None,
    priority: Priority | None,
    batch_id: uuid.UUID | None,
    cursor: Cursor | None,
    limit: int,
) -> tuple[list[Sms], Cursor | None]:
    query = select(Sms).where(
        Sms.tenant_id == tenant_id, Sms.created_at >= from_, Sms.created_at < to
    )
    if status is not None:
        query = query.where(Sms.status == status)
    if priority is not None:
        query = query.where(Sms.priority == priority)
    if batch_id is not None:
        query = query.where(Sms.batch_id == batch_id)
    if cursor is not None:
        query = query.where(
            (Sms.created_at > cursor.created_at)
            | ((Sms.created_at == cursor.created_at) & (Sms.id > cursor.id))
        )

    query = query.order_by(Sms.created_at, Sms.id).limit(limit + 1)
    rows = (await session.execute(query)).scalars().all()

    next_cursor = None
    if len(rows) > limit:
        rows = rows[:limit]
        last = rows[-1]
        next_cursor = Cursor(created_at=last.created_at, id=last.id)

    return list(rows), next_cursor

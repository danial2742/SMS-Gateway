import uuid
from datetime import datetime

from fastapi import APIRouter, Depends, Query
from gateway_common.domain.enums import Priority, SmsStatus
from gateway_common.domain.errors import MissingDateRangeError
from gateway_common.pagination import Cursor, clamp_limit
from gateway_common.validation import validate_date_range
from sqlalchemy.ext.asyncio import AsyncSession

from api_service.config import settings
from api_service.deps import get_read_session, get_tenant_id
from api_service.schemas.reports import ReportItem, ReportResponse
from api_service.services import reporting

router = APIRouter(prefix="/api/v1", tags=["reports"])


@router.get("/reports/sms", response_model=ReportResponse)
async def report_sms(
    from_: datetime | None = Query(default=None, alias="from"),
    to: datetime | None = Query(default=None),
    status: SmsStatus | None = Query(default=None),
    priority: Priority | None = Query(default=None),
    batch_id: uuid.UUID | None = Query(default=None),
    cursor: str | None = Query(default=None),
    limit: int | None = Query(default=None),
    tenant_id: uuid.UUID = Depends(get_tenant_id),
    session: AsyncSession = Depends(get_read_session),
) -> ReportResponse:
    if from_ is None or to is None:
        raise MissingDateRangeError("'from' and 'to' query parameters are required")
    validate_date_range(from_, to, max_range_days=settings.report_max_range_days)

    decoded_cursor = Cursor.decode(cursor) if cursor else None
    rows, next_cursor = await reporting.list_sms_report(
        session,
        tenant_id,
        from_=from_,
        to=to,
        status=status,
        priority=priority,
        batch_id=batch_id,
        cursor=decoded_cursor,
        limit=clamp_limit(limit),
    )

    return ReportResponse(
        items=[
            ReportItem(
                sms_id=row.id,
                recipient=row.recipient,
                status=row.status,
                priority=row.priority,
                cost=row.cost,
                created_at=row.created_at,
            )
            for row in rows
        ],
        next_cursor=next_cursor.encode() if next_cursor else None,
    )

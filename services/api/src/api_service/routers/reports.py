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
from api_service.schemas.errors import ErrorEnvelope
from api_service.schemas.reports import ReportItem, ReportResponse
from api_service.services import reporting

router = APIRouter(prefix="/api/v1", tags=["reports"])


@router.get(
    "/reports/sms",
    response_model=ReportResponse,
    summary="Query historical SMS records",
    description=(
        "Paginated, filterable query over historical SMS records for a tenant. Served from a "
        "read replica, never the OLTP primary. Cursor-based pagination — an empty/absent "
        "`next_cursor` signals the last page."
    ),
    responses={
        400: {"model": ErrorEnvelope, "description": "MISSING_DATE_RANGE"},
        422: {"model": ErrorEnvelope, "description": "INVALID_DATE_RANGE | INVALID_CURSOR"},
    },
)
async def report_sms(
    from_: datetime | None = Query(default=None, alias="from", description="Range start (inclusive), ISO 8601. Required."),
    to: datetime | None = Query(default=None, description="Range end (exclusive), ISO 8601. Required; must be after `from`."),
    status: SmsStatus | None = Query(default=None, description="Filter by SMS status."),
    priority: Priority | None = Query(default=None, description="Filter by priority."),
    batch_id: uuid.UUID | None = Query(default=None, description="Restrict to one batch's children."),
    cursor: str | None = Query(default=None, description="Opaque cursor from a prior response's `next_cursor`; omit for the first page."),
    limit: int | None = Query(default=None, description="Max items per page. Clamped to [1, 200], default 50 — values above 200 are clamped, not rejected."),
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

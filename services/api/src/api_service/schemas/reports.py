import uuid
from datetime import datetime

from gateway_common.domain.enums import Priority, SmsStatus
from pydantic import BaseModel


class ReportItem(BaseModel):
    sms_id: uuid.UUID
    recipient: str
    status: SmsStatus
    priority: Priority
    cost: int
    created_at: datetime


class ReportResponse(BaseModel):
    items: list[ReportItem]
    next_cursor: str | None = None

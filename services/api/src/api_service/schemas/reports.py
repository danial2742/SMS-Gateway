import uuid
from datetime import datetime

from gateway_common.domain.enums import Priority, SmsStatus
from pydantic import BaseModel, Field


class ReportItem(BaseModel):
    sms_id: uuid.UUID
    recipient: str
    status: SmsStatus
    priority: Priority
    cost: int = Field(..., description="Credits deducted for this message.")
    created_at: datetime


class ReportResponse(BaseModel):
    items: list[ReportItem]
    next_cursor: str | None = Field(
        default=None, description="Opaque cursor for the next page. Absent or empty signals the last page."
    )

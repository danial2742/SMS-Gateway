import uuid
from datetime import datetime

from gateway_common.domain.enums import Priority, SmsStatus
from pydantic import BaseModel


class SmsCreateRequest(BaseModel):
    recipient: str
    message: str
    priority: Priority = Priority.NORMAL


class SmsCreateResponse(BaseModel):
    sms_id: uuid.UUID
    status: SmsStatus
    cost: int
    balance_after: int


class SmsDetailResponse(BaseModel):
    sms_id: uuid.UUID
    batch_id: uuid.UUID | None
    recipient: str
    status: SmsStatus
    priority: Priority
    attempt_count: int
    created_at: datetime
    sent_at: datetime | None

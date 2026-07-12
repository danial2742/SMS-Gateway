import uuid
from datetime import datetime

from gateway_common.domain.enums import BatchStatus, Priority
from pydantic import BaseModel


class BatchCreateRequest(BaseModel):
    recipients: list[str]
    message: str
    priority: Priority = Priority.NORMAL


class BatchCreateResponse(BaseModel):
    batch_id: uuid.UUID
    recipient_count: int
    total_cost: int
    status: BatchStatus
    balance_after: int


class BatchDetailResponse(BaseModel):
    batch_id: uuid.UUID
    status: BatchStatus
    recipient_count: int
    sent_count: int
    failed_count: int
    created_at: datetime

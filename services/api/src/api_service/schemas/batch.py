import uuid
from datetime import datetime

from gateway_common.domain.enums import BatchStatus, Priority
from pydantic import BaseModel, Field


class BatchCreateRequest(BaseModel):
    recipients: list[str] = Field(
        ...,
        description="Non-empty, E.164 per entry, bounded by a configured max recipients per request.",
        examples=[["+15551234567", "+15557654321"]],
    )
    message: str = Field(..., description="Stored once, not per recipient.", examples=["50% off today only"])
    priority: Priority = Field(default=Priority.NORMAL, description="Delivery priority.")


class BatchCreateResponse(BaseModel):
    batch_id: uuid.UUID
    recipient_count: int = Field(..., description="Number of recipients in this batch.")
    total_cost: int = Field(..., description="recipient_count * unit_cost, in credits.")
    status: BatchStatus = Field(..., description="Always ACCEPTED on acceptance.")
    balance_after: int = Field(..., description="Tenant wallet balance after this deduction, in credits.")


class BatchDetailResponse(BaseModel):
    batch_id: uuid.UUID
    status: BatchStatus
    recipient_count: int
    sent_count: int = Field(
        ...,
        description="Derived from a periodic aggregate over child SMS rows — eventually consistent, not real-time.",
    )
    failed_count: int = Field(
        ...,
        description="Derived from a periodic aggregate over child SMS rows — eventually consistent, not real-time.",
    )
    created_at: datetime

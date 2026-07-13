import uuid
from datetime import datetime

from gateway_common.domain.enums import Priority, SmsStatus
from pydantic import BaseModel, Field


class SmsCreateRequest(BaseModel):
    recipient: str = Field(..., description="E.164 format.", examples=["+15551234567"])
    message: str = Field(
        ...,
        description="Single-page content; must fit the character limit for the resolved GSM-7/UCS-2 encoding.",
        examples=["Your code is 4821"],
    )
    priority: Priority = Field(default=Priority.NORMAL, description="Delivery priority.")


class SmsCreateResponse(BaseModel):
    sms_id: uuid.UUID
    status: SmsStatus = Field(..., description="Always QUEUED on acceptance.")
    cost: int = Field(..., description="Credits deducted for this message.")
    balance_after: int = Field(..., description="Tenant wallet balance after this deduction, in credits.")


class SmsDetailResponse(BaseModel):
    sms_id: uuid.UUID
    batch_id: uuid.UUID | None = Field(..., description="Set if this message was submitted as part of a batch.")
    recipient: str
    status: SmsStatus = Field(
        ...,
        description="QUEUED, SENT_TO_OPERATOR, DELIVERED, FAILED, or FAILED_DEAD_LETTER. "
        "DELIVERED is not populated absent a delivery-receipt integration.",
    )
    priority: Priority
    attempt_count: int
    created_at: datetime
    sent_at: datetime | None = Field(..., description="Set once the message reaches SENT_TO_OPERATOR or later.")

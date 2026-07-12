import uuid
from datetime import datetime

from gateway_common.domain.errors import InvalidAmountError
from pydantic import BaseModel, field_validator


class WalletResponse(BaseModel):
    tenant_id: uuid.UUID
    balance: int
    currency: str
    updated_at: datetime


class ChargeRequest(BaseModel):
    amount: int | float
    method_ref: str | None = None

    @field_validator("amount")
    @classmethod
    def _amount_must_be_positive_int(cls, value: int | float) -> int | float:
        # Raises the domain error directly (not ValueError) so it propagates
        # past pydantic as INVALID_AMOUNT/422 (docs/api.md), not a generic
        # FastAPI validation error mapped to 400 INVALID_JSON.
        if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
            raise InvalidAmountError("amount must be a positive integer")
        return value


class ChargeResponse(BaseModel):
    topup_id: uuid.UUID
    balance_after: int

import uuid

from fastapi import APIRouter, Depends, Header
from gateway_common.domain.errors import IdempotencyKeyMissingError
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from api_service.config import settings
from api_service.deps import get_primary_session, get_redis, get_tenant_id
from api_service.schemas.errors import ErrorEnvelope
from api_service.schemas.wallet import ChargeRequest, ChargeResponse, WalletResponse
from api_service.services import idempotency_service, wallet_service

router = APIRouter(prefix="/api/v1", tags=["wallet"])


@router.get(
    "/wallet",
    response_model=WalletResponse,
    summary="Get wallet balance",
    description=(
        "Retrieve the current balance for the authenticated tenant. Used to check spendable "
        "balance before submitting large batches, and by internal dashboards."
    ),
    responses={404: {"model": ErrorEnvelope, "description": "WALLET_NOT_FOUND"}},
)
async def get_wallet(
    tenant_id: uuid.UUID = Depends(get_tenant_id),
    session: AsyncSession = Depends(get_primary_session),
) -> WalletResponse:
    wallet = await wallet_service.get_wallet(session, tenant_id)
    return WalletResponse(
        tenant_id=wallet.tenant_id,
        balance=wallet.balance,
        currency=wallet.currency,
        updated_at=wallet.updated_at,
    )


@router.post(
    "/wallet/charge",
    response_model=ChargeResponse,
    status_code=201,
    summary="Top up wallet balance",
    description=(
        "Top up a tenant's SMS balance following a successful external payment. Called by an "
        "internal billing/payment integration, not directly by an end customer's SMS-sending "
        "client. Requires an `Idempotency-Key` header."
    ),
    responses={
        400: {"model": ErrorEnvelope, "description": "INVALID_JSON | MISSING_IDEMPOTENCY_KEY"},
        422: {"model": ErrorEnvelope, "description": "INVALID_AMOUNT"},
    },
)
async def charge_wallet(
    body: ChargeRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    tenant_id: uuid.UUID = Depends(get_tenant_id),
    session: AsyncSession = Depends(get_primary_session),
    redis: Redis = Depends(get_redis),
) -> ChargeResponse:
    if not idempotency_key:
        raise IdempotencyKeyMissingError("Idempotency-Key header is required")

    body_hash = idempotency_service.request_hash(body.model_dump(mode="json"))
    cached = await idempotency_service.check_idempotency(
        session,
        redis,
        tenant_id=tenant_id,
        idempotency_key=idempotency_key,
        body_hash=body_hash,
        ttl_seconds=settings.idempotency_key_ttl_seconds,
    )
    if cached is not None:
        return ChargeResponse(**cached)

    try:
        response_snapshot = await wallet_service.charge_wallet(
            session,
            tenant_id,
            body.amount,
            body.method_ref,
            idempotency_key=idempotency_key,
            body_hash=body_hash,
            idempotency_ttl_seconds=settings.idempotency_key_ttl_seconds,
        )
    finally:
        await idempotency_service.release_idempotency_lock(redis, tenant_id, idempotency_key)

    return ChargeResponse(**response_snapshot)

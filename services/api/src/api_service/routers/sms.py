import uuid

from fastapi import APIRouter, Depends, Header
from gateway_common.domain.errors import IdempotencyKeyMissingError
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from api_service.config import settings
from api_service.deps import get_primary_session, get_read_session, get_redis, get_tenant_id
from api_service.schemas.sms import SmsCreateRequest, SmsCreateResponse, SmsDetailResponse
from api_service.services import idempotency_service, reporting, submission

router = APIRouter(prefix="/api/v1", tags=["sms"])


@router.post("/sms", response_model=SmsCreateResponse, status_code=202)
async def create_sms(
    body: SmsCreateRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    tenant_id: uuid.UUID = Depends(get_tenant_id),
    session: AsyncSession = Depends(get_primary_session),
    redis: Redis = Depends(get_redis),
) -> SmsCreateResponse:
    # Idempotency-Key presence checked before any other validation (docs/api.md).
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
        return SmsCreateResponse(**cached)

    try:
        response_snapshot = await submission.submit_single_sms(
            session,
            tenant_id=tenant_id,
            recipient=body.recipient,
            message=body.message,
            priority=body.priority,
            idempotency_key=idempotency_key,
            body_hash=body_hash,
            idempotency_ttl_seconds=settings.idempotency_key_ttl_seconds,
            express_topic=settings.kafka_topic_express,
            normal_topic=settings.kafka_topic_normal,
        )
    finally:
        await idempotency_service.release_idempotency_lock(redis, tenant_id, idempotency_key)

    return SmsCreateResponse(**response_snapshot)


@router.get("/sms/{sms_id}", response_model=SmsDetailResponse)
async def get_sms(
    sms_id: uuid.UUID,
    tenant_id: uuid.UUID = Depends(get_tenant_id),
    session: AsyncSession = Depends(get_read_session),
) -> SmsDetailResponse:
    sms = await reporting.get_sms_detail(session, tenant_id, sms_id)
    return SmsDetailResponse(
        sms_id=sms.id,
        batch_id=sms.batch_id,
        recipient=sms.recipient,
        status=sms.status,
        priority=sms.priority,
        attempt_count=sms.attempt_count,
        created_at=sms.created_at,
        sent_at=sms.sent_at,
    )

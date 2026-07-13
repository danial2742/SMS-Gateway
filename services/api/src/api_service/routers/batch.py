import uuid

from fastapi import APIRouter, Depends, Header
from gateway_common.domain.errors import IdempotencyKeyMissingError
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession

from api_service.config import settings
from api_service.deps import get_primary_session, get_read_session, get_redis, get_tenant_id
from api_service.schemas.batch import BatchCreateRequest, BatchCreateResponse, BatchDetailResponse
from api_service.schemas.errors import ErrorEnvelope
from api_service.services import idempotency_service, reporting, submission

router = APIRouter(prefix="/api/v1", tags=["batch"])


@router.post(
    "/sms/batch",
    response_model=BatchCreateResponse,
    status_code=202,
    summary="Submit a batch of SMS",
    description=(
        "Send identical content to many recipients as one atomic operation. Requires an "
        "`Idempotency-Key` header. Any invalid recipient rejects the entire batch — no partial "
        "acceptance."
    ),
    responses={
        402: {"model": ErrorEnvelope, "description": "INSUFFICIENT_BALANCE (for the entire batch)"},
        422: {
            "model": ErrorEnvelope,
            "description": "EMPTY_RECIPIENT_LIST | BATCH_TOO_LARGE | INVALID_RECIPIENT",
        },
    },
)
async def create_batch(
    body: BatchCreateRequest,
    idempotency_key: str | None = Header(default=None, alias="Idempotency-Key"),
    tenant_id: uuid.UUID = Depends(get_tenant_id),
    session: AsyncSession = Depends(get_primary_session),
    redis: Redis = Depends(get_redis),
) -> BatchCreateResponse:
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
        return BatchCreateResponse(**cached)

    try:
        response_snapshot = await submission.submit_batch_sms(
            session,
            tenant_id=tenant_id,
            recipients=body.recipients,
            message=body.message,
            priority=body.priority,
            idempotency_key=idempotency_key,
            body_hash=body_hash,
            idempotency_ttl_seconds=settings.idempotency_key_ttl_seconds,
            express_topic=settings.kafka_topic_express,
            normal_topic=settings.kafka_topic_normal,
            max_recipients=settings.batch_max_recipients,
        )
    finally:
        await idempotency_service.release_idempotency_lock(redis, tenant_id, idempotency_key)

    return BatchCreateResponse(**response_snapshot)


@router.get(
    "/batches/{batch_id}",
    response_model=BatchDetailResponse,
    summary="Get batch progress",
    description=(
        "Poll aggregate progress of a batch send. `sent_count`/`failed_count` are derived from a "
        "periodic aggregate over child SMS rows — eventually consistent with in-flight sends."
    ),
    responses={404: {"model": ErrorEnvelope, "description": "BATCH_NOT_FOUND"}},
)
async def get_batch(
    batch_id: uuid.UUID,
    tenant_id: uuid.UUID = Depends(get_tenant_id),
    session: AsyncSession = Depends(get_read_session),
) -> BatchDetailResponse:
    detail = await reporting.get_batch_detail(session, tenant_id, batch_id)
    return BatchDetailResponse(**detail)

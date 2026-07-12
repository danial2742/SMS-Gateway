import asyncio
import uuid

from gateway_common.domain.enums import Tier
from gateway_common.kafka.producer import GatewayProducer
from gateway_common.logging import get_logger
from gateway_common.metrics import dlq_messages_total, dlq_publish_failed_total

logger = get_logger()

_MAX_PUBLISH_ATTEMPTS = 3
_RETRY_BACKOFF_SECONDS = 0.5


async def publish_to_dlq(
    producer: GatewayProducer,
    dlq_topic: str,
    message: dict,
    *,
    tenant_id: uuid.UUID,
    request_id: str,
    error: str,
    attempt_count: int,
    first_attempted_at: str,
    last_attempted_at: str,
    reason: str,
    tier: Tier,
) -> None:
    """docs/queue.md Dead Letter Queue: original message + failure metadata,
    retained 30 days for investigation/manual replay (replay tooling itself
    is out of scope for v1 — docs/assumptions.md).

    The refund + FAILED_DEAD_LETTER DB state is already committed by the time
    this is called, so a Kafka outage here can't be allowed to raise past
    this function — that would leave the caller with a committed refund and
    no way to compensate. Retries a few times, then swallows and logs at
    `critical` so the loss is at least observable/alertable instead of silent.
    """
    payload = {
        **message,
        "error": error,
        "attempt_count": attempt_count,
        "first_attempted_at": first_attempted_at,
        "last_attempted_at": last_attempted_at,
    }
    for attempt in range(1, _MAX_PUBLISH_ATTEMPTS + 1):
        try:
            await producer.send(
                dlq_topic,
                payload,
                tenant_id=tenant_id,
                request_id=request_id,
                attempt_count=attempt_count,
            )
            dlq_messages_total.labels(tier=tier, reason=reason).inc()
            return
        except Exception as exc:  # noqa: BLE001 — last-resort mitigation, see docstring
            if attempt == _MAX_PUBLISH_ATTEMPTS:
                logger.critical(
                    "dlq_publish_failed",
                    sms_id=message.get("sms_id"),
                    tenant_id=str(tenant_id),
                    reason=reason,
                    tier=tier,
                    error=str(exc)[:500],
                )
                dlq_publish_failed_total.labels(tier=tier).inc()
                return
            await asyncio.sleep(_RETRY_BACKOFF_SECONDS * attempt)

from aiokafka import AIOKafkaConsumer
from gateway_common.db.engine import DatabaseSessions
from gateway_common.domain.enums import OutboxEventType
from gateway_common.fanout import expand_batch_accepted
from gateway_common.kafka.consumer import decode_message
from gateway_common.logging import get_logger
from gateway_common.redis.drr_store import RedisDrrStore
from gateway_common.request_context import request_id_var, tenant_id_var
from gateway_common.shutdown import GracefulShutdown

logger = get_logger()


async def ingest_loop(
    consumer: AIOKafkaConsumer,
    store: RedisDrrStore,
    db_sessions: DatabaseSessions,
    shutdown: GracefulShutdown,
) -> None:
    """Consumes sms.normal: SmsAccepted events enqueue directly;
    BatchAccepted events are expanded inline (docs/database.md "Why batch
    fan-out doesn't insert to outbox_events per recipient" — the fan-out read
    happens here, in whichever consumer sees the event first, per the plan's
    resolution for the fan-out step the docs describe generically).
    """
    async for raw in consumer:
        if shutdown.should_stop:
            break

        message = decode_message(raw)
        event_type = message.payload.get("event_type")
        request_id_var.set(message.request_id)
        tenant_id_var.set(message.payload.get("tenant_id", ""))

        if event_type == OutboxEventType.BATCH_ACCEPTED.value:
            async with db_sessions.read() as session:
                batch_id = message.payload["batch_id"]
                async for dispatch in expand_batch_accepted(session, batch_id):
                    await store.enqueue(dispatch["tenant_id"], dispatch)
                    logger.info("scheduler_enqueued", sms_id=dispatch["sms_id"], batch_id=batch_id)
        else:
            payload = message.payload
            await store.enqueue(payload["tenant_id"], payload)
            logger.info("scheduler_enqueued", sms_id=payload.get("sms_id"))

        await consumer.commit()

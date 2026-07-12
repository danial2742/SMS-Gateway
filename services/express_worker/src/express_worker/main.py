import asyncio
from contextlib import AsyncExitStack

from gateway_common.db.engine import DatabaseSessions
from gateway_common.domain.enums import OutboxEventType, Tier
from gateway_common.fanout import expand_batch_accepted
from gateway_common.health import ping_kafka, ping_postgres, ping_redis
from gateway_common.health_server import serve_health
from gateway_common.kafka.consumer import decode_message, make_consumer
from gateway_common.kafka.producer import GatewayProducer
from gateway_common.logging import configure_logging, get_logger
from gateway_common.operator.factory import make_operator_client
from gateway_common.redis.client import make_redis
from gateway_common.shutdown import GracefulShutdown
from prometheus_client import start_http_server
from worker_kit.loop import WorkerConfig, process_dispatch_safely

from express_worker.settings import settings

logger = get_logger()


async def main() -> None:
    configure_logging("express-worker", settings.log_level)
    start_http_server(settings.metrics_port)

    async with AsyncExitStack() as stack:
        db_sessions = DatabaseSessions(settings.database_url)
        stack.push_async_callback(db_sessions.dispose)

        redis = make_redis(settings.redis_url)
        stack.push_async_callback(redis.aclose)

        producer = GatewayProducer(settings.brokers_list)
        await producer.start()
        stack.push_async_callback(producer.stop)

        operator = make_operator_client(settings)

        consumer = make_consumer(
            settings.kafka_topic_express, brokers=settings.brokers_list, group_id=settings.consumer_group
        )
        await consumer.start()
        stack.push_async_callback(consumer.stop)

        config = WorkerConfig(
            tier=Tier.EXPRESS,
            retry_topic=settings.kafka_topic_express,
            dlq_topic=settings.kafka_topic_dlq_express,
        )

        async def readiness_checks() -> dict[str, str]:
            return {
                "postgres": await ping_postgres(db_sessions.primary_engine),
                "redis": await ping_redis(redis),
                "kafka": await ping_kafka(producer.client),
            }

        health_server = await serve_health(settings.health_port, readiness_checks)
        stack.callback(health_server.close)

        shutdown = GracefulShutdown(settings.shutdown_grace_period_seconds)
        shutdown.install()

        logger.info("express_worker_started")
        async for raw in consumer:
            if shutdown.should_stop:
                break

            message = decode_message(raw)
            event_type = message.payload.get("event_type")

            if event_type == OutboxEventType.BATCH_ACCEPTED.value:
                async with db_sessions.read() as session:
                    dispatches = [
                        d async for d in expand_batch_accepted(session, message.payload["batch_id"])
                    ]
                for dispatch in dispatches:
                    await process_dispatch_safely(
                        db_sessions,
                        producer,
                        operator,
                        config,
                        dispatch,
                        redis=redis,
                        attempt_count=message.attempt_count,
                        request_id=message.request_id,
                    )
            else:
                await process_dispatch_safely(
                    db_sessions,
                    producer,
                    operator,
                    config,
                    message.payload,
                    redis=redis,
                    attempt_count=message.attempt_count,
                    request_id=message.request_id,
                )

            await consumer.commit()
        logger.info("express_worker_stopped")


if __name__ == "__main__":
    asyncio.run(main())

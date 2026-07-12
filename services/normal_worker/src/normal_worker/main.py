import asyncio
from contextlib import AsyncExitStack

from gateway_common.db.engine import DatabaseSessions
from gateway_common.domain.enums import Tier
from gateway_common.health import ping_kafka, ping_postgres, ping_redis
from gateway_common.health_server import serve_health
from gateway_common.kafka.producer import GatewayProducer
from gateway_common.logging import configure_logging, get_logger
from gateway_common.operator.factory import make_operator_client
from gateway_common.redis.client import make_redis
from gateway_common.redis.drr_store import RedisDrrStore
from gateway_common.shutdown import GracefulShutdown
from prometheus_client import start_http_server
from worker_kit.loop import WorkerConfig, process_dispatch_safely

from normal_worker.settings import settings

logger = get_logger()


async def main() -> None:
    configure_logging("normal-worker", settings.log_level)
    start_http_server(settings.metrics_port)

    async with AsyncExitStack() as stack:
        db_sessions = DatabaseSessions(settings.database_url)
        stack.push_async_callback(db_sessions.dispose)

        redis = make_redis(settings.redis_url)
        stack.push_async_callback(redis.aclose)
        store = RedisDrrStore(redis)

        producer = GatewayProducer(settings.brokers_list)
        await producer.start()
        stack.push_async_callback(producer.stop)

        operator = make_operator_client(settings)

        config = WorkerConfig(
            tier=Tier.NORMAL,
            retry_topic=settings.kafka_topic_normal,
            dlq_topic=settings.kafka_topic_dlq_normal,
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

        logger.info("normal_worker_started")
        while not shutdown.should_stop:
            # Dispatch handoff from the Fair Scheduler is a Redis List, not a
            # direct Kafka consume — see the plan's DRR->Normal-Worker
            # handoff resolution. Retries and DLQ still go through Kafka
            # (docs/queue.md), same as Express.
            message = await store.blpop_ready("normal", timeout=settings.blpop_timeout_seconds)
            if message is None:
                continue

            await process_dispatch_safely(
                db_sessions,
                producer,
                operator,
                config,
                message,
                redis=redis,
                attempt_count=int(message.get("attempt_count", 0)),
                request_id=message.get("request_id", ""),
            )
        logger.info("normal_worker_stopped")


if __name__ == "__main__":
    asyncio.run(main())

import asyncio

from aiokafka import AIOKafkaProducer
from gateway_common.db.engine import DatabaseSessions
from gateway_common.health import ping_kafka, ping_postgres, ping_redis
from gateway_common.health_server import serve_health
from gateway_common.kafka.consumer import make_consumer
from gateway_common.logging import configure_logging, get_logger
from gateway_common.redis.client import make_redis
from gateway_common.redis.drr_store import RedisDrrStore
from gateway_common.shutdown import GracefulShutdown
from prometheus_client import start_http_server

from fair_scheduler.drr import DeficitRoundRobin
from fair_scheduler.ingest import ingest_loop
from fair_scheduler.round_loop import round_loop
from fair_scheduler.settings import settings

logger = get_logger()


async def main() -> None:
    configure_logging("fair-scheduler", settings.log_level)
    start_http_server(settings.metrics_port)

    db_sessions = DatabaseSessions(settings.database_url)
    redis = make_redis(settings.redis_url)
    store = RedisDrrStore(redis)
    drr = DeficitRoundRobin(store, quantum=settings.quantum)

    consumer = make_consumer(
        settings.kafka_topic_normal, brokers=settings.brokers_list, group_id=settings.consumer_group
    )
    await consumer.start()

    # Health-check-only producer: fair-scheduler doesn't publish to Kafka
    # (dispatch handoff is the Redis List documented in the plan), but
    # /readyz still verifies broker reachability per deployment.md.
    health_producer = AIOKafkaProducer(bootstrap_servers=settings.brokers_list)
    await health_producer.start()

    async def readiness_checks() -> dict[str, str]:
        return {
            "postgres": await ping_postgres(db_sessions.primary_engine),
            "redis": await ping_redis(redis),
            "kafka": await ping_kafka(health_producer),
        }

    health_server = await serve_health(settings.health_port, readiness_checks)

    shutdown = GracefulShutdown(settings.shutdown_grace_period_seconds)
    shutdown.install()

    logger.info("fair_scheduler_started")
    ingest_task = asyncio.create_task(ingest_loop(consumer, store, db_sessions, shutdown))
    round_task = asyncio.create_task(
        round_loop(drr, store, shutdown, round_interval_seconds=settings.round_interval_seconds)
    )
    try:
        await shutdown.wait()
    finally:
        # Stopping the consumer first unblocks ingest_loop's `async for`,
        # which otherwise only checks shutdown.should_stop between messages.
        health_server.close()
        await consumer.stop()
        ingest_task.cancel()
        round_task.cancel()
        await asyncio.gather(ingest_task, round_task, return_exceptions=True)
        await health_producer.stop()
        await redis.aclose()
        await db_sessions.dispose()
        logger.info("fair_scheduler_stopped")


if __name__ == "__main__":
    asyncio.run(main())

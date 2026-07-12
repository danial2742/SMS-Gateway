import asyncio

from gateway_common.db.engine import DatabaseSessions
from gateway_common.health import ping_kafka, ping_postgres
from gateway_common.health_server import serve_health
from gateway_common.kafka.producer import GatewayProducer
from gateway_common.logging import configure_logging, get_logger
from gateway_common.shutdown import GracefulShutdown
from prometheus_client import start_http_server

from outbox_relay.relay import run
from outbox_relay.settings import settings

logger = get_logger()


async def main() -> None:
    configure_logging("outbox-relay", settings.log_level)
    start_http_server(settings.metrics_port)

    db_sessions = DatabaseSessions(settings.database_url)
    producer = GatewayProducer(settings.brokers_list)
    await producer.start()

    async def readiness_checks() -> dict[str, str]:
        return {
            "postgres": await ping_postgres(db_sessions.primary_engine),
            "kafka": await ping_kafka(producer.client),
        }

    health_server = await serve_health(settings.health_port, readiness_checks)

    shutdown = GracefulShutdown(settings.shutdown_grace_period_seconds)
    shutdown.install()

    logger.info("outbox_relay_started")
    try:
        await run(
            db_sessions,
            producer,
            shutdown,
            poll_interval_seconds=settings.poll_interval_seconds,
            batch_size=settings.batch_size,
        )
    finally:
        health_server.close()
        await producer.stop()
        await db_sessions.dispose()
        logger.info("outbox_relay_stopped")


if __name__ == "__main__":
    asyncio.run(main())

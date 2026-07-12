from aiokafka import AIOKafkaProducer
from redis.asyncio import Redis
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine

from gateway_common.logging import get_logger

logger = get_logger()


async def ping_postgres(engine: AsyncEngine) -> str:
    try:
        async with engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return "ok"
    except Exception as exc:
        logger.warning("health_check_failed", check="postgres", error=str(exc))
        return "timeout"


async def ping_redis(redis: Redis) -> str:
    try:
        await redis.ping()
        return "ok"
    except Exception as exc:
        logger.warning("health_check_failed", check="redis", error=str(exc))
        return "timeout"


async def ping_kafka(producer: AIOKafkaProducer) -> str:
    try:
        await producer.client.fetch_all_metadata()
        return "ok"
    except Exception as exc:
        logger.warning("health_check_failed", check="kafka", error=str(exc))
        return "timeout"


async def readiness_payload(checks: dict[str, str]) -> tuple[dict, int]:
    """Builds the /readyz body per docs/api.md — 200 if every check is 'ok',
    503 (with the failing check identified) otherwise.
    """
    all_ok = all(v == "ok" for v in checks.values())
    body = {"status": "ready" if all_ok else "not_ready", "checks": checks}
    return body, (200 if all_ok else 503)

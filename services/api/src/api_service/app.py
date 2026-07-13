from contextlib import AsyncExitStack, asynccontextmanager

from aiokafka import AIOKafkaProducer
from fastapi import FastAPI
from gateway_common.db.engine import DatabaseSessions
from gateway_common.logging import configure_logging
from gateway_common.redis.client import make_redis
from prometheus_client import make_asgi_app

from api_service.config import settings
from api_service.error_handlers import register_error_handlers
from api_service.middleware.body_size_limit import BodySizeLimitMiddleware
from api_service.middleware.rate_limit import RateLimitMiddleware
from api_service.middleware.request_id import RequestIdMiddleware
from api_service.routers import batch, health, reports, sms, wallet


@asynccontextmanager
async def lifespan(app: FastAPI):
    configure_logging("api", settings.log_level)

    async with AsyncExitStack() as stack:
        app.state.db_sessions = DatabaseSessions(settings.database_url, settings.database_read_url)
        stack.push_async_callback(app.state.db_sessions.dispose)

        # redis client itself is created eagerly in create_app() (see the
        # comment there) — only its cleanup belongs to the lifespan.
        stack.push_async_callback(app.state.redis.aclose)

        app.state.kafka_health_producer = AIOKafkaProducer(bootstrap_servers=settings.brokers_list)
        await app.state.kafka_health_producer.start()
        stack.push_async_callback(app.state.kafka_health_producer.stop)

        yield


API_DESCRIPTION = """
REST API for submitting and tracking SMS messages, batches, and tenant wallet balance.

All endpoints below `/api/v1` require an `X-Tenant-ID` header. Mutating `POST` endpoints
(`/sms`, `/sms/batch`, `/wallet/charge`) additionally require an `Idempotency-Key` header
(UUID recommended); retrying the same key with the same body replays the original result,
retrying with a different body returns `422 IDEMPOTENCY_KEY_REUSED`.

Errors use a consistent JSON envelope:

```json
{"error": {"code": "INSUFFICIENT_BALANCE", "message": "...", "request_id": "9c3f2b7e-..."}}
```

`request_id` matches the `request_id` emitted in structured logs for that request.
"""

OPENAPI_TAGS = [
    {"name": "health", "description": "Unauthenticated liveness/readiness probes."},
    {"name": "wallet", "description": "Tenant balance and top-ups."},
    {"name": "sms", "description": "Single-message submission and status."},
    {"name": "batch", "description": "Multi-recipient batch submission and progress."},
    {"name": "reports", "description": "Paginated historical SMS queries."},
]


def create_app() -> FastAPI:
    app = FastAPI(
        title="SMS Gateway API",
        version="0.1.0",
        description=API_DESCRIPTION,
        openapi_tags=OPENAPI_TAGS,
        lifespan=lifespan,
    )

    # Created eagerly (redis.asyncio connects lazily on first command) so the
    # rate-limit middleware — instantiated at app-build time, before lifespan
    # startup runs — and the lifespan-managed lifecycle share one client.
    redis = make_redis(settings.redis_url)
    app.state.redis = redis

    # add_middleware prepends, so the middleware added last runs outermost.
    # BodySizeLimitMiddleware rejects oversized requests before anything
    # else runs; RequestIdMiddleware still needs to be outermost of the
    # remaining two so request_id is set before rate limiting can
    # short-circuit the response.
    app.add_middleware(RateLimitMiddleware, redis=redis, rps=settings.rate_limit_rps)
    app.add_middleware(RequestIdMiddleware)
    app.add_middleware(BodySizeLimitMiddleware, max_bytes=settings.max_request_body_bytes)

    register_error_handlers(app)

    app.include_router(health.router)
    app.include_router(wallet.router)
    app.include_router(sms.router)
    app.include_router(batch.router)
    app.include_router(reports.router)

    app.mount("/metrics", make_asgi_app())

    return app

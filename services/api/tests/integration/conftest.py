import os
from collections.abc import AsyncIterator

import httpx
import pytest


@pytest.fixture
async def client(postgres_url, redis_url, kafka_brokers) -> AsyncIterator[httpx.AsyncClient]:
    # Function-scoped (unlike the session-scoped containers it depends on):
    # a module/session-scoped async client whose lifespan spans multiple
    # test functions runs into pytest-asyncio event-loop-per-test teardown
    # ordering issues (each test function gets its own loop by default; a
    # long-lived asyncpg/redis connection created in test A's loop errors
    # closing under test B's loop). Rebuilding the (cheap) app + lifespan
    # per test avoids that entirely; the expensive part — the containers —
    # stays shared.
    #
    # api_service.config.settings is a module-level singleton validated
    # eagerly at import time (fail-fast startup, deployment.md) — env vars
    # must land before that module is first imported.
    os.environ["DATABASE_URL"] = postgres_url
    os.environ["DATABASE_READ_URL"] = postgres_url
    os.environ["REDIS_URL"] = redis_url
    os.environ["KAFKA_BROKERS"] = kafka_brokers

    from api_service.app import create_app

    app = create_app()
    async with app.router.lifespan_context(app):
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://test") as ac:
            yield ac


@pytest.fixture
def tenant_headers() -> dict:
    import uuid

    return {"X-Tenant-ID": "11111111-1111-1111-1111-111111111111", "Idempotency-Key": str(uuid.uuid4())}

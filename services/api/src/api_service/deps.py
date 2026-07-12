import uuid
from collections.abc import AsyncIterator

from fastapi import Header, Request
from gateway_common.db.engine import DatabaseSessions
from redis.asyncio import Redis
from sqlalchemy.ext.asyncio import AsyncSession


# Auth/tenant resolution is out of scope (docs/assumptions.md #1) — every
# service trusts an already-resolved tenant_id in request context. Locally
# and in this build, that's the X-Tenant-ID header (README.md Quick start);
# a real deployment would resolve this upstream (gateway/auth layer) before
# the request reaches this service.
async def get_tenant_id(x_tenant_id: uuid.UUID = Header(..., alias="X-Tenant-ID")) -> uuid.UUID:
    return x_tenant_id


async def get_db_sessions(request: Request) -> DatabaseSessions:
    return request.app.state.db_sessions


async def get_primary_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.db_sessions.primary() as session:
        yield session


async def get_read_session(request: Request) -> AsyncIterator[AsyncSession]:
    async with request.app.state.db_sessions.read() as session:
        yield session


async def get_redis(request: Request) -> Redis:
    return request.app.state.redis

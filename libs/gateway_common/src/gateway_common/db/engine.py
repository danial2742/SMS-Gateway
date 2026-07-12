from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)


def make_engine(database_url: str, *, pool_size: int = 10) -> AsyncEngine:
    return create_async_engine(database_url, pool_size=pool_size, pool_pre_ping=True)


def make_session_factory(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return async_sessionmaker(engine, expire_on_commit=False)


class DatabaseSessions:
    """Holds the primary (read-write) and read-replica (read-only) session
    factories a service needs. Reporting/status reads use `read`; every
    write path (submission, refund, top-up) uses `primary`.
    """

    def __init__(self, primary_url: str, read_url: str | None = None) -> None:
        self.primary_engine = make_engine(primary_url)
        self.primary_sessions = make_session_factory(self.primary_engine)

        self.read_engine = make_engine(read_url) if read_url else self.primary_engine
        self.read_sessions = (
            make_session_factory(self.read_engine) if read_url else self.primary_sessions
        )

    @asynccontextmanager
    async def primary(self) -> AsyncIterator[AsyncSession]:
        async with self.primary_sessions() as session:
            yield session

    @asynccontextmanager
    async def read(self) -> AsyncIterator[AsyncSession]:
        async with self.read_sessions() as session:
            yield session

    async def dispose(self) -> None:
        await self.primary_engine.dispose()
        if self.read_engine is not self.primary_engine:
            await self.read_engine.dispose()

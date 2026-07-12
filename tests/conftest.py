import os
import subprocess
import sys
from pathlib import Path

import pytest
from testcontainers.kafka import KafkaContainer
from testcontainers.postgres import PostgresContainer
from testcontainers.redis import RedisContainer

REPO_ROOT = Path(__file__).resolve().parent.parent


@pytest.fixture(scope="session")
def postgres_url() -> str:
    """Session-scoped Postgres container, migrated once via Alembic — every
    integration test in the suite shares it (docs/testing.md's integration
    layer against a real Postgres, not mocked).
    """
    with PostgresContainer("postgres:16-alpine", driver="asyncpg") as pg:
        url = pg.get_connection_url()
        env = {**os.environ, "DATABASE_URL": url}
        subprocess.run(
            [sys.executable, "-m", "alembic", "-c", "migrations/alembic.ini", "upgrade", "head"],
            cwd=REPO_ROOT,
            env=env,
            check=True,
        )
        yield url


@pytest.fixture(scope="session")
def redis_url() -> str:
    with RedisContainer("redis:7-alpine") as redis:
        yield f"redis://{redis.get_container_host_ip()}:{redis.get_exposed_port(6379)}/0"


@pytest.fixture(scope="session")
def kafka_brokers() -> str:
    with KafkaContainer("confluentinc/cp-kafka:7.7.0") as kafka:
        yield kafka.get_bootstrap_server()

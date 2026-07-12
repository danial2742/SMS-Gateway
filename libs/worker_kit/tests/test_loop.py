import uuid

import asyncpg
import pytest
from gateway_common.db.engine import DatabaseSessions
from gateway_common.db.models import Sms, Wallet
from gateway_common.domain.enums import Tier
from gateway_common.kafka.producer import GatewayProducer
from gateway_common.operator.protocol import OperatorClientError, OperatorTimeoutError
from sqlalchemy import select
from worker_kit.loop import WorkerConfig, process_dispatch

pytestmark = pytest.mark.integration


class FakeProducer(GatewayProducer):
    """Records send() calls instead of hitting Kafka — no broker needed."""

    def __init__(self) -> None:
        super().__init__(brokers=["localhost:9092"])
        self.sent: list[dict] = []

    async def send(
        self, topic: str, payload: dict, *, tenant_id: uuid.UUID, request_id: str, attempt_count: int = 0
    ) -> None:
        self.sent.append({"topic": topic, "payload": payload, "attempt_count": attempt_count})


class FakeOperatorClient:
    """Structurally satisfies OperatorClient (a Protocol) — raises the given
    exception on send(), or returns a fixed operator_message_id on success.
    """

    def __init__(self, exc: Exception | None = None) -> None:
        self._exc = exc

    async def send(self, *, recipient: str, message: str, sms_id: str) -> str:
        if self._exc is not None:
            raise self._exc
        return "op-msg-1"


async def _provision_tenant_wallet_and_sms(postgres_url: str, *, balance: int, cost: int) -> tuple[
    uuid.UUID, uuid.UUID
]:
    tenant_id = uuid.uuid4()
    sms_id = uuid.uuid4()
    dsn = postgres_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("INSERT INTO tenants (id, name) VALUES ($1, $2)", tenant_id, "loop-test")
        await conn.execute("INSERT INTO wallets (tenant_id, balance) VALUES ($1, $2)", tenant_id, balance)
        await conn.execute(
            """
            INSERT INTO sms (id, tenant_id, recipient, priority, cost, status)
            VALUES ($1, $2, $3, 'EXPRESS', $4, 'QUEUED')
            """,
            sms_id,
            tenant_id,
            "+15551234567",
            cost,
        )
    finally:
        await conn.close()
    return tenant_id, sms_id


def _payload(tenant_id: uuid.UUID, sms_id: uuid.UUID, cost: int) -> dict:
    return {
        "tenant_id": str(tenant_id),
        "sms_id": str(sms_id),
        "recipient": "+15551234567",
        "message_body": "hello",
        "cost": cost,
    }


@pytest.mark.asyncio
async def test_process_dispatch_schedules_retry_when_retryable_and_below_max_attempts(
    postgres_url, redis_client
):
    tenant_id, sms_id = await _provision_tenant_wallet_and_sms(postgres_url, balance=10, cost=3)
    db_sessions = DatabaseSessions(postgres_url)
    producer = FakeProducer()
    config = WorkerConfig(tier=Tier.EXPRESS, retry_topic="sms.express.retry", dlq_topic="sms.express.dlq")
    try:
        await process_dispatch(
            db_sessions,
            producer,
            FakeOperatorClient(exc=OperatorTimeoutError("timed out")),
            config,
            _payload(tenant_id, sms_id, cost=3),
            redis=redis_client,
            attempt_count=0,
            request_id="req-1",
        )

        assert len(producer.sent) == 1
        assert producer.sent[0]["topic"] == "sms.express.retry"
        assert producer.sent[0]["attempt_count"] == 1

        async with db_sessions.read() as session:
            sms = (await session.execute(select(Sms).where(Sms.id == sms_id))).scalar_one()
            wallet = await session.get(Wallet, tenant_id)
        assert sms.attempt_count == 1
        assert sms.last_error is not None
        assert sms.status == "QUEUED"
        assert wallet.balance == 10  # no refund on a retry, only on dead-letter
    finally:
        await db_sessions.dispose()


@pytest.mark.asyncio
async def test_process_dispatch_dead_letters_and_refunds_when_retries_exhausted(postgres_url, redis_client):
    tenant_id, sms_id = await _provision_tenant_wallet_and_sms(postgres_url, balance=10, cost=3)
    db_sessions = DatabaseSessions(postgres_url)
    producer = FakeProducer()
    # Tier.EXPRESS max_attempts is 2 — attempt_count=2 means retries are exhausted.
    config = WorkerConfig(tier=Tier.EXPRESS, retry_topic="sms.express.retry", dlq_topic="sms.express.dlq")
    try:
        await process_dispatch(
            db_sessions,
            producer,
            FakeOperatorClient(exc=OperatorTimeoutError("timed out")),
            config,
            _payload(tenant_id, sms_id, cost=3),
            redis=redis_client,
            attempt_count=2,
            request_id="req-2",
        )

        assert len(producer.sent) == 1
        assert producer.sent[0]["topic"] == "sms.express.dlq"

        async with db_sessions.read() as session:
            sms = (await session.execute(select(Sms).where(Sms.id == sms_id))).scalar_one()
            wallet = await session.get(Wallet, tenant_id)
        assert sms.status == "FAILED_DEAD_LETTER"
        assert wallet.balance == 13
    finally:
        await db_sessions.dispose()


@pytest.mark.asyncio
async def test_process_dispatch_dead_letters_immediately_on_non_retryable_error(postgres_url, redis_client):
    tenant_id, sms_id = await _provision_tenant_wallet_and_sms(postgres_url, balance=10, cost=3)
    db_sessions = DatabaseSessions(postgres_url)
    producer = FakeProducer()
    config = WorkerConfig(tier=Tier.EXPRESS, retry_topic="sms.express.retry", dlq_topic="sms.express.dlq")
    try:
        await process_dispatch(
            db_sessions,
            producer,
            FakeOperatorClient(exc=OperatorClientError(422)),
            config,
            _payload(tenant_id, sms_id, cost=3),
            redis=redis_client,
            attempt_count=0,
            request_id="req-3",
        )

        assert len(producer.sent) == 1
        assert producer.sent[0]["topic"] == "sms.express.dlq"

        async with db_sessions.read() as session:
            sms = (await session.execute(select(Sms).where(Sms.id == sms_id))).scalar_one()
            wallet = await session.get(Wallet, tenant_id)
        assert sms.status == "FAILED_DEAD_LETTER"
        assert wallet.balance == 13
    finally:
        await db_sessions.dispose()


@pytest.mark.asyncio
async def test_process_dispatch_marks_sent_to_operator_on_success(postgres_url, redis_client):
    tenant_id, sms_id = await _provision_tenant_wallet_and_sms(postgres_url, balance=10, cost=3)
    db_sessions = DatabaseSessions(postgres_url)
    producer = FakeProducer()
    config = WorkerConfig(tier=Tier.EXPRESS, retry_topic="sms.express.retry", dlq_topic="sms.express.dlq")
    try:
        await process_dispatch(
            db_sessions,
            producer,
            FakeOperatorClient(),
            config,
            _payload(tenant_id, sms_id, cost=3),
            redis=redis_client,
            attempt_count=0,
            request_id="req-success",
        )

        assert len(producer.sent) == 0  # no retry/DLQ publish on success

        async with db_sessions.read() as session:
            sms = (await session.execute(select(Sms).where(Sms.id == sms_id))).scalar_one()
            wallet = await session.get(Wallet, tenant_id)
        assert sms.status == "SENT_TO_OPERATOR"
        assert sms.sent_at is not None
        assert sms.operator_message_id == "op-msg-1"
        assert sms.attempt_count == 1
        assert wallet.balance == 10  # no refund on success
    finally:
        await db_sessions.dispose()

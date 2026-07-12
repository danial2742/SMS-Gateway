import uuid

import asyncpg
import pytest
from gateway_common.db.engine import DatabaseSessions
from gateway_common.db.models import Sms, Wallet
from sqlalchemy import select
from worker_kit.refund import refund_and_mark_dead_letter

pytestmark = pytest.mark.integration


async def _provision_tenant_wallet_and_sms(postgres_url: str, *, balance: int, cost: int) -> tuple[
    uuid.UUID, uuid.UUID, object
]:
    tenant_id = uuid.uuid4()
    sms_id = uuid.uuid4()
    dsn = postgres_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("INSERT INTO tenants (id, name) VALUES ($1, $2)", tenant_id, "refund-test")
        await conn.execute("INSERT INTO wallets (tenant_id, balance) VALUES ($1, $2)", tenant_id, balance)
        created_at = await conn.fetchval(
            """
            INSERT INTO sms (id, tenant_id, recipient, priority, cost, status)
            VALUES ($1, $2, $3, 'NORMAL', $4, 'FAILED')
            RETURNING created_at
            """,
            sms_id,
            tenant_id,
            "+15551234567",
            cost,
        )
    finally:
        await conn.close()
    return tenant_id, sms_id, created_at


@pytest.mark.asyncio
async def test_refund_and_mark_dead_letter_refunds_wallet_and_marks_sms(postgres_url):
    tenant_id, sms_id, created_at = await _provision_tenant_wallet_and_sms(postgres_url, balance=10, cost=3)
    db_sessions = DatabaseSessions(postgres_url)
    try:
        async with db_sessions.primary() as session:
            refunded = await refund_and_mark_dead_letter(
                session, sms_id=sms_id, created_at=created_at, tenant_id=tenant_id, refund_amount=3
            )
        assert refunded is True

        async with db_sessions.read() as session:
            wallet = await session.get(Wallet, tenant_id)
            sms = (await session.execute(select(Sms).where(Sms.id == sms_id))).scalar_one()
        assert wallet.balance == 13
        assert sms.status == "FAILED_DEAD_LETTER"
    finally:
        await db_sessions.dispose()


@pytest.mark.asyncio
async def test_refund_and_mark_dead_letter_is_idempotent_against_redelivery(postgres_url):
    """Kafka/DLQ redelivery can call this twice for the same sms_id — the
    second call must be a no-op, not a double refund.
    """
    tenant_id, sms_id, created_at = await _provision_tenant_wallet_and_sms(postgres_url, balance=10, cost=3)
    db_sessions = DatabaseSessions(postgres_url)
    try:
        async with db_sessions.primary() as session:
            await refund_and_mark_dead_letter(
                session, sms_id=sms_id, created_at=created_at, tenant_id=tenant_id, refund_amount=3
            )

        async with db_sessions.primary() as session:
            refunded_again = await refund_and_mark_dead_letter(
                session, sms_id=sms_id, created_at=created_at, tenant_id=tenant_id, refund_amount=3
            )
        assert refunded_again is False

        async with db_sessions.read() as session:
            wallet = await session.get(Wallet, tenant_id)
        assert wallet.balance == 13
    finally:
        await db_sessions.dispose()

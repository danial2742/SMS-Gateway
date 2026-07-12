import uuid
from datetime import datetime

from gateway_common.db.models import Sms, Wallet, WalletLedger
from gateway_common.domain.enums import LedgerReason, SmsStatus
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession


async def refund_and_mark_dead_letter(
    session: AsyncSession,
    *,
    sms_id: uuid.UUID,
    created_at: datetime,
    tenant_id: uuid.UUID,
    refund_amount: int,
) -> bool:
    """docs/database.md Transactions — Refund (DLQ landing), inline per the
    plan's DLQ resolution: the worker performs this at DLQ-publish time,
    guarded so a redelivered DLQ publish (at-least-once) can never
    double-refund.

    Returns False (no-op) if the sms row was already FAILED_DEAD_LETTER —
    idempotency guard against duplicate DLQ processing on Kafka redelivery.
    """
    result = await session.execute(
        update(Sms)
        .where(Sms.id == sms_id, Sms.created_at == created_at, Sms.status != SmsStatus.FAILED_DEAD_LETTER)
        .values(status=SmsStatus.FAILED_DEAD_LETTER, updated_at=func.now())
        .returning(Sms.id)
    )
    if result.first() is None:
        await session.commit()
        return False

    wallet_result = await session.execute(
        update(Wallet)
        .where(Wallet.tenant_id == tenant_id)
        .values(balance=Wallet.balance + refund_amount, updated_at=func.now())
        .returning(Wallet.balance)
    )
    balance_after = wallet_result.scalar_one()

    session.add(
        WalletLedger(
            tenant_id=tenant_id,
            delta=refund_amount,
            reason=LedgerReason.REFUND,
            reference_id=sms_id,
            balance_after=balance_after,
        )
    )

    await session.commit()
    return True

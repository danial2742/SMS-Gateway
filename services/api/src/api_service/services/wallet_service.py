import uuid
from datetime import UTC, datetime, timedelta

from gateway_common.db.models import IdempotencyKey, Topup, Wallet, WalletLedger
from gateway_common.domain.enums import (
    IdempotencyResourceType,
    IdempotencyStatus,
    LedgerReason,
    TopupStatus,
)
from gateway_common.domain.errors import WalletNotFoundError
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession


async def get_wallet(session: AsyncSession, tenant_id: uuid.UUID) -> Wallet:
    wallet = await session.get(Wallet, tenant_id)
    if wallet is None:
        raise WalletNotFoundError(f"no wallet for tenant {tenant_id}")
    return wallet


async def charge_wallet(
    session: AsyncSession,
    tenant_id: uuid.UUID,
    amount: int | float,
    method_ref: str | None,
    *,
    idempotency_key: str,
    body_hash: str,
    idempotency_ttl_seconds: int,
) -> dict:
    """POST /wallet/charge: single UPDATE + ledger insert + topups insert +
    idempotency_keys insert, one atomic transaction — mirrors
    submission.py's shape so "charged" and "safe to replay" can never
    disagree. No outbox event — topups don't need async fan-out.
    """
    topup_id = uuid.uuid4()
    session.add(
        Topup(
            id=topup_id,
            tenant_id=tenant_id,
            amount=amount,
            method_ref=method_ref,
            status=TopupStatus.COMPLETED,
        )
    )

    result = await session.execute(
        update(Wallet)
        .where(Wallet.tenant_id == tenant_id)
        .values(balance=Wallet.balance + amount, updated_at=func.now())
        .returning(Wallet.balance)
    )
    row = result.first()
    if row is None:
        raise WalletNotFoundError(f"no wallet for tenant {tenant_id}")
    balance_after = row[0]

    session.add(
        WalletLedger(
            tenant_id=tenant_id,
            delta=amount,
            reason=LedgerReason.TOPUP,
            reference_id=topup_id,
            balance_after=balance_after,
        )
    )

    response_snapshot = {"topup_id": str(topup_id), "balance_after": balance_after}
    session.add(
        IdempotencyKey(
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
            request_hash=body_hash,
            resource_type=IdempotencyResourceType.WALLET_CHARGE,
            resource_id=topup_id,
            status=IdempotencyStatus.COMPLETED,
            response_snapshot=response_snapshot,
            expires_at=datetime.now(UTC) + timedelta(seconds=idempotency_ttl_seconds),
        )
    )

    await session.commit()
    return response_snapshot

import uuid
from datetime import UTC, datetime, timedelta

from gateway_common.db.models import Batch, IdempotencyKey, OutboxEvent, Sms, Wallet, WalletLedger
from gateway_common.domain.cost import sms_cost
from gateway_common.domain.enums import (
    BatchStatus,
    IdempotencyResourceType,
    IdempotencyStatus,
    LedgerReason,
    OutboxAggregateType,
    OutboxEventType,
    Priority,
    SmsStatus,
)
from gateway_common.domain.errors import InsufficientBalanceError
from gateway_common.kafka.topics import topic_for_priority
from gateway_common.request_context import get_request_id
from gateway_common.validation import validate_message, validate_recipient, validate_recipients
from sqlalchemy import func, update
from sqlalchemy.ext.asyncio import AsyncSession

# docs/assumptions.md #4: cost is resolved to an integer credit amount
# *before* the atomic deduction; pricing configuration (rate cards, per-tenant
# pricing) is out of scope for v1 — a fixed unit cost stands in for it here.
UNIT_COST_CREDITS = 1


async def submit_single_sms(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    recipient: str,
    message: str,
    priority: Priority,
    idempotency_key: str,
    body_hash: str,
    idempotency_ttl_seconds: int,
    express_topic: str,
    normal_topic: str,
) -> dict:
    """docs/database.md Transactions — Submission (single SMS): one atomic
    UPDATE...RETURNING deduction, sms insert, ledger insert, exactly one
    outbox row. The idempotency_keys row lands in the SAME transaction so
    "charged" and "safe to replay" can never disagree.
    """
    validate_recipient(recipient)
    validate_message(message)

    cost = sms_cost(UNIT_COST_CREDITS)
    sms_id = uuid.uuid4()

    result = await session.execute(
        update(Wallet)
        .where(Wallet.tenant_id == tenant_id, Wallet.balance >= cost)
        .values(balance=Wallet.balance - cost, updated_at=func.now())
        .returning(Wallet.balance)
    )
    row = result.first()
    if row is None:
        raise InsufficientBalanceError("wallet balance is insufficient to accept this request")
    balance_after = row[0]

    session.add(
        Sms(
            id=sms_id,
            tenant_id=tenant_id,
            batch_id=None,
            recipient=recipient,
            message_body=message,
            priority=priority,
            cost=cost,
            status=SmsStatus.QUEUED,
        )
    )
    session.add(
        WalletLedger(
            tenant_id=tenant_id,
            delta=-cost,
            reason=LedgerReason.SMS_DEDUCT,
            reference_id=sms_id,
            balance_after=balance_after,
        )
    )
    session.add(
        OutboxEvent(
            aggregate_type=OutboxAggregateType.SMS,
            aggregate_id=sms_id,
            event_type=OutboxEventType.SMS_ACCEPTED,
            payload={
                "sms_id": str(sms_id),
                "tenant_id": str(tenant_id),
                "recipient": recipient,
                "message_body": message,
                "priority": priority.value,
                "cost": cost,
                "request_id": get_request_id(),
            },
            partition_key=tenant_id,
            topic=topic_for_priority(priority, express_topic=express_topic, normal_topic=normal_topic),
        )
    )

    response_snapshot = {
        "sms_id": str(sms_id),
        "status": SmsStatus.QUEUED.value,
        "cost": cost,
        "balance_after": balance_after,
    }
    session.add(
        IdempotencyKey(
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
            request_hash=body_hash,
            resource_type=IdempotencyResourceType.SMS,
            resource_id=sms_id,
            status=IdempotencyStatus.COMPLETED,
            response_snapshot=response_snapshot,
            expires_at=datetime.now(UTC) + timedelta(seconds=idempotency_ttl_seconds),
        )
    )

    await session.commit()
    return response_snapshot


async def submit_batch_sms(
    session: AsyncSession,
    *,
    tenant_id: uuid.UUID,
    recipients: list[str],
    message: str,
    priority: Priority,
    idempotency_key: str,
    body_hash: str,
    idempotency_ttl_seconds: int,
    express_topic: str,
    normal_topic: str,
    max_recipients: int,
) -> dict:
    """docs/database.md Transactions — Submission (batch): total_cost computed
    once, one atomic UPDATE covers the whole batch (no partial acceptance),
    and exactly ONE outbox row is written regardless of recipient count — see
    docs/database.md "Why batch fan-out doesn't insert to outbox_events per
    recipient".
    """
    validate_recipients(recipients, max_recipients=max_recipients)
    validate_message(message)

    recipient_count = len(recipients)
    total_cost = sms_cost(UNIT_COST_CREDITS, recipient_count)
    batch_id = uuid.uuid4()

    result = await session.execute(
        update(Wallet)
        .where(Wallet.tenant_id == tenant_id, Wallet.balance >= total_cost)
        .values(balance=Wallet.balance - total_cost, updated_at=func.now())
        .returning(Wallet.balance)
    )
    row = result.first()
    if row is None:
        raise InsufficientBalanceError(
            "wallet balance is insufficient to accept this batch in full"
        )
    balance_after = row[0]

    session.add(
        Batch(
            id=batch_id,
            tenant_id=tenant_id,
            message_body=message,
            recipient_count=recipient_count,
            priority=priority,
            unit_cost=UNIT_COST_CREDITS,
            total_cost=total_cost,
            status=BatchStatus.ACCEPTED,
        )
    )
    session.add_all(
        Sms(
            id=uuid.uuid4(),
            tenant_id=tenant_id,
            batch_id=batch_id,
            recipient=recipient,
            message_body=None,  # inherits from batches.message_body — storage optimization
            priority=priority,
            cost=UNIT_COST_CREDITS,
            status=SmsStatus.QUEUED,
        )
        for recipient in recipients
    )
    session.add(
        WalletLedger(
            tenant_id=tenant_id,
            delta=-total_cost,
            reason=LedgerReason.SMS_DEDUCT,
            reference_id=batch_id,
            balance_after=balance_after,
        )
    )
    session.add(
        OutboxEvent(
            aggregate_type=OutboxAggregateType.BATCH,
            aggregate_id=batch_id,
            event_type=OutboxEventType.BATCH_ACCEPTED,
            payload={
                "batch_id": str(batch_id),
                "tenant_id": str(tenant_id),
                "priority": priority.value,
                "recipient_count": recipient_count,
                "request_id": get_request_id(),
            },
            partition_key=tenant_id,
            topic=topic_for_priority(priority, express_topic=express_topic, normal_topic=normal_topic),
        )
    )

    response_snapshot = {
        "batch_id": str(batch_id),
        "recipient_count": recipient_count,
        "total_cost": total_cost,
        "status": BatchStatus.ACCEPTED.value,
        "balance_after": balance_after,
    }
    session.add(
        IdempotencyKey(
            tenant_id=tenant_id,
            idempotency_key=idempotency_key,
            request_hash=body_hash,
            resource_type=IdempotencyResourceType.BATCH,
            resource_id=batch_id,
            status=IdempotencyStatus.COMPLETED,
            response_snapshot=response_snapshot,
            expires_at=datetime.now(UTC) + timedelta(seconds=idempotency_ttl_seconds),
        )
    )

    await session.commit()
    return response_snapshot

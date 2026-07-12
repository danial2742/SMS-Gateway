import uuid
from collections.abc import Iterable
from datetime import datetime
from enum import StrEnum

from sqlalchemy import (
    BigInteger,
    CheckConstraint,
    ForeignKey,
    Index,
    Integer,
    Text,
    func,
    text,
)
from sqlalchemy.dialects.postgresql import JSONB, TIMESTAMP, UUID
from sqlalchemy.orm import Mapped, mapped_column

from gateway_common.db.base import Base
from gateway_common.domain.enums import (
    BatchStatus,
    IdempotencyStatus,
    LedgerReason,
    Priority,
    SmsStatus,
    TopupStatus,
)


def _in_predicate(column: str, members: Iterable[StrEnum]) -> str:
    values = ", ".join(f"'{member.value}'" for member in members)
    return f"{column} IN ({values})"


def _check_in(column: str, enum_cls: type[StrEnum]) -> str:
    # Keeps models.py in sync with domain/enums.py by construction. New enum
    # members still need a new Alembic migration to ALTER the DB constraint —
    # this only prevents drift within models.py itself.
    return _in_predicate(column, enum_cls)


class Tenant(Base):
    __tablename__ = "tenants"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    name: Mapped[str] = mapped_column(Text, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )


class Wallet(Base):
    __tablename__ = "wallets"

    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id"), primary_key=True
    )
    balance: Mapped[int] = mapped_column(BigInteger, nullable=False, server_default="0")
    currency: Mapped[str] = mapped_column(Text, nullable=False, server_default="credits")
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (CheckConstraint("balance >= 0", name="ck_wallets_balance_non_negative"),)


class WalletLedger(Base):
    __tablename__ = "wallet_ledger"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False
    )
    delta: Mapped[int] = mapped_column(BigInteger, nullable=False)
    reason: Mapped[LedgerReason] = mapped_column(Text, nullable=False)
    reference_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    balance_after: Mapped[int] = mapped_column(BigInteger, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint(_check_in("reason", LedgerReason), name="ck_wallet_ledger_reason"),
        Index("idx_ledger_tenant_time", "tenant_id", "created_at"),
        Index("idx_ledger_reference", "reference_id"),
    )


class Topup(Base):
    __tablename__ = "topups"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False
    )
    amount: Mapped[int] = mapped_column(BigInteger, nullable=False)
    method_ref: Mapped[str | None] = mapped_column(Text, nullable=True)
    status: Mapped[TopupStatus] = mapped_column(
        Text, nullable=False, server_default=TopupStatus.COMPLETED.value
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("amount > 0", name="ck_topups_amount_positive"),
        CheckConstraint(_check_in("status", TopupStatus), name="ck_topups_status"),
        Index("idx_topups_tenant_time", "tenant_id", "created_at"),
    )


class Batch(Base):
    __tablename__ = "batches"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False
    )
    message_body: Mapped[str] = mapped_column(Text, nullable=False)
    recipient_count: Mapped[int] = mapped_column(Integer, nullable=False)
    priority: Mapped[Priority] = mapped_column(Text, nullable=False)
    unit_cost: Mapped[int] = mapped_column(BigInteger, nullable=False)
    total_cost: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[BatchStatus] = mapped_column(
        Text, nullable=False, server_default=BatchStatus.ACCEPTED.value
    )
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        CheckConstraint("recipient_count > 0", name="ck_batches_recipient_count_positive"),
        CheckConstraint(_check_in("priority", Priority), name="ck_batches_priority"),
        CheckConstraint(_check_in("status", BatchStatus), name="ck_batches_status"),
        Index("idx_batches_tenant_time", "tenant_id", "created_at"),
    )


class Sms(Base):
    """Maps the native RANGE-partitioned `sms` table (partitioned by created_at).

    Partitioning DDL lives in the Alembic migration (raw SQL) — SQLAlchemy's ORM
    has no first-class declarative partitioning support. The composite primary
    key (id, created_at) is required by Postgres for any partitioned table.
    """

    __tablename__ = "sms"

    id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    tenant_id: Mapped[uuid.UUID] = mapped_column(
        UUID(as_uuid=True), ForeignKey("tenants.id"), nullable=False
    )
    batch_id: Mapped[uuid.UUID | None] = mapped_column(
        UUID(as_uuid=True), ForeignKey("batches.id"), nullable=True
    )
    recipient: Mapped[str] = mapped_column(Text, nullable=False)
    message_body: Mapped[str | None] = mapped_column(Text, nullable=True)
    priority: Mapped[Priority] = mapped_column(Text, nullable=False)
    cost: Mapped[int] = mapped_column(BigInteger, nullable=False)
    status: Mapped[SmsStatus] = mapped_column(
        Text, nullable=False, server_default=SmsStatus.QUEUED.value
    )
    attempt_count: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    last_error: Mapped[str | None] = mapped_column(Text, nullable=True)
    operator_message_id: Mapped[str | None] = mapped_column(Text, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now(), primary_key=True
    )
    updated_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    sent_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)

    __table_args__ = (
        CheckConstraint(_check_in("priority", Priority), name="ck_sms_priority"),
        CheckConstraint(_check_in("status", SmsStatus), name="ck_sms_status"),
        Index("idx_sms_tenant_time", "tenant_id", "created_at"),
        Index("idx_sms_batch", "batch_id"),
        Index(
            "idx_sms_status_pending",
            "status",
            postgresql_where=text(
                _in_predicate("status", [SmsStatus.QUEUED, SmsStatus.SENT_TO_OPERATOR])
            ),
        ),
    )


class OutboxEvent(Base):
    __tablename__ = "outbox_events"

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True, autoincrement=True)
    aggregate_type: Mapped[str] = mapped_column(Text, nullable=False)
    aggregate_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    event_type: Mapped[str] = mapped_column(Text, nullable=False)
    payload: Mapped[dict] = mapped_column(JSONB, nullable=False)
    partition_key: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), nullable=False)
    topic: Mapped[str] = mapped_column(Text, nullable=False)
    published_at: Mapped[datetime | None] = mapped_column(TIMESTAMP(timezone=True), nullable=True)
    attempts: Mapped[int] = mapped_column(Integer, nullable=False, server_default="0")
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )

    __table_args__ = (
        Index(
            "idx_outbox_unpublished",
            "created_at",
            postgresql_where=text("published_at IS NULL"),
        ),
        Index("idx_outbox_aggregate", "aggregate_type", "aggregate_id"),
    )


class IdempotencyKey(Base):
    __tablename__ = "idempotency_keys"

    tenant_id: Mapped[uuid.UUID] = mapped_column(UUID(as_uuid=True), primary_key=True)
    idempotency_key: Mapped[str] = mapped_column(Text, primary_key=True)
    request_hash: Mapped[str] = mapped_column(Text, nullable=False)
    resource_type: Mapped[str] = mapped_column(Text, nullable=False)
    resource_id: Mapped[uuid.UUID | None] = mapped_column(UUID(as_uuid=True), nullable=True)
    status: Mapped[IdempotencyStatus] = mapped_column(
        Text, nullable=False, server_default=IdempotencyStatus.IN_PROGRESS.value
    )
    response_snapshot: Mapped[dict | None] = mapped_column(JSONB, nullable=True)
    created_at: Mapped[datetime] = mapped_column(
        TIMESTAMP(timezone=True), nullable=False, server_default=func.now()
    )
    expires_at: Mapped[datetime] = mapped_column(TIMESTAMP(timezone=True), nullable=False)

    __table_args__ = (
        CheckConstraint(_check_in("status", IdempotencyStatus), name="ck_idempotency_keys_status"),
        Index("idx_idempotency_expires", "expires_at"),
    )

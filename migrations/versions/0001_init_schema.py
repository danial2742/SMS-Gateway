"""init schema

Translates docs/database.md's DDL exactly. Raw SQL (not op.create_table) is
used throughout: the `sms` table needs native PARTITION BY RANGE, which
SQLAlchemy/Alembic have no first-class declarative support for, and keeping
every table's DDL in one raw-SQL style keeps this migration a direct,
line-for-line mirror of the docs — the source of truth to diff against.

Revision ID: 0001
Revises:
Create Date: 2026-07-11
"""
from collections.abc import Sequence

from alembic import op

revision: str = "0001"
down_revision: str | None = None
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.execute("""
        CREATE TABLE tenants (
            id          UUID PRIMARY KEY,
            name        TEXT NOT NULL,
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)

    op.execute("""
        CREATE TABLE wallets (
            tenant_id   UUID PRIMARY KEY REFERENCES tenants(id),
            balance     BIGINT NOT NULL DEFAULT 0 CHECK (balance >= 0),
            currency    TEXT NOT NULL DEFAULT 'credits',
            updated_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)

    op.execute("""
        CREATE TABLE wallet_ledger (
            id            BIGSERIAL PRIMARY KEY,
            tenant_id     UUID NOT NULL REFERENCES tenants(id),
            delta         BIGINT NOT NULL,
            reason        TEXT NOT NULL CHECK (reason IN ('TOPUP','SMS_DEDUCT','REFUND')),
            reference_id  UUID NOT NULL,
            balance_after BIGINT NOT NULL,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX idx_ledger_tenant_time ON wallet_ledger (tenant_id, created_at)")
    op.execute("CREATE INDEX idx_ledger_reference ON wallet_ledger (reference_id)")

    op.execute("""
        CREATE TABLE topups (
            id          UUID PRIMARY KEY,
            tenant_id   UUID NOT NULL REFERENCES tenants(id),
            amount      BIGINT NOT NULL CHECK (amount > 0),
            method_ref  TEXT,
            status      TEXT NOT NULL DEFAULT 'COMPLETED' CHECK (status IN ('COMPLETED')),
            created_at  TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX idx_topups_tenant_time ON topups (tenant_id, created_at)")

    op.execute("""
        CREATE TABLE batches (
            id              UUID PRIMARY KEY,
            tenant_id       UUID NOT NULL REFERENCES tenants(id),
            message_body    TEXT NOT NULL,
            recipient_count INT NOT NULL CHECK (recipient_count > 0),
            priority        TEXT NOT NULL CHECK (priority IN ('NORMAL','EXPRESS')),
            unit_cost       BIGINT NOT NULL,
            total_cost      BIGINT NOT NULL,
            status          TEXT NOT NULL DEFAULT 'ACCEPTED'
                            CHECK (status IN ('ACCEPTED','IN_PROGRESS','COMPLETED','PARTIALLY_FAILED','FAILED')),
            created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("CREATE INDEX idx_batches_tenant_time ON batches (tenant_id, created_at)")

    op.execute("""
        CREATE TABLE sms (
            id                  UUID NOT NULL,
            tenant_id           UUID NOT NULL REFERENCES tenants(id),
            batch_id            UUID REFERENCES batches(id),
            recipient           TEXT NOT NULL,
            message_body        TEXT,
            priority            TEXT NOT NULL CHECK (priority IN ('NORMAL','EXPRESS')),
            cost                BIGINT NOT NULL,
            status              TEXT NOT NULL DEFAULT 'QUEUED'
                                CHECK (status IN ('QUEUED','SENT_TO_OPERATOR','DELIVERED','FAILED','FAILED_DEAD_LETTER')),
            attempt_count       INT NOT NULL DEFAULT 0,
            last_error          TEXT,
            operator_message_id TEXT,
            created_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
            updated_at          TIMESTAMPTZ NOT NULL DEFAULT now(),
            sent_at             TIMESTAMPTZ,
            PRIMARY KEY (id, created_at)
        ) PARTITION BY RANGE (created_at)
    """)

    # Initial partitions covering the migration's rollout window. Creating
    # additional monthly partitions ahead of each boundary is not yet
    # automated (out of scope for this pass) — see docs/database.md. A
    # DEFAULT partition is added as a safety net so an insert outside these
    # explicit ranges degrades to landing in the catch-all rather than
    # hard-failing with "no partition of relation sms found for row".
    op.execute("""
        CREATE TABLE sms_2026_07 PARTITION OF sms
            FOR VALUES FROM ('2026-07-01') TO ('2026-08-01')
    """)
    op.execute("""
        CREATE TABLE sms_2026_08 PARTITION OF sms
            FOR VALUES FROM ('2026-08-01') TO ('2026-09-01')
    """)
    op.execute("""
        CREATE TABLE sms_2026_09 PARTITION OF sms
            FOR VALUES FROM ('2026-09-01') TO ('2026-10-01')
    """)
    op.execute("""
        CREATE TABLE sms_2026_10 PARTITION OF sms
            FOR VALUES FROM ('2026-10-01') TO ('2026-11-01')
    """)
    op.execute("""
        CREATE TABLE sms_2026_11 PARTITION OF sms
            FOR VALUES FROM ('2026-11-01') TO ('2026-12-01')
    """)
    op.execute("""
        CREATE TABLE sms_2026_12 PARTITION OF sms
            FOR VALUES FROM ('2026-12-01') TO ('2027-01-01')
    """)
    op.execute("CREATE TABLE sms_default PARTITION OF sms DEFAULT")

    op.execute("CREATE INDEX idx_sms_tenant_time ON sms (tenant_id, created_at)")
    op.execute("CREATE INDEX idx_sms_batch ON sms (batch_id)")
    # Mirrors gateway_common.db.models.Sms's idx_sms_status_pending
    # predicate — keep both in sync if SmsStatus's QUEUED/SENT_TO_OPERATOR
    # pending-set ever changes.
    op.execute("""
        CREATE INDEX idx_sms_status_pending ON sms (status)
            WHERE status IN ('QUEUED','SENT_TO_OPERATOR')
    """)

    op.execute("""
        CREATE TABLE outbox_events (
            id            BIGSERIAL PRIMARY KEY,
            aggregate_type TEXT NOT NULL,
            aggregate_id  UUID NOT NULL,
            event_type    TEXT NOT NULL,
            payload       JSONB NOT NULL,
            partition_key UUID NOT NULL,
            topic         TEXT NOT NULL,
            published_at  TIMESTAMPTZ,
            attempts      INT NOT NULL DEFAULT 0,
            created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
        )
    """)
    op.execute("""
        CREATE INDEX idx_outbox_unpublished ON outbox_events (created_at)
            WHERE published_at IS NULL
    """)
    op.execute("CREATE INDEX idx_outbox_aggregate ON outbox_events (aggregate_type, aggregate_id)")

    op.execute("""
        CREATE TABLE idempotency_keys (
            tenant_id        UUID NOT NULL,
            idempotency_key  TEXT NOT NULL,
            request_hash     TEXT NOT NULL,
            resource_type    TEXT NOT NULL,
            resource_id      UUID,
            status           TEXT NOT NULL DEFAULT 'IN_PROGRESS' CHECK (status IN ('IN_PROGRESS','COMPLETED')),
            response_snapshot JSONB,
            created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
            expires_at       TIMESTAMPTZ NOT NULL,
            PRIMARY KEY (tenant_id, idempotency_key)
        )
    """)
    op.execute("CREATE INDEX idx_idempotency_expires ON idempotency_keys (expires_at)")


def downgrade() -> None:
    op.execute("DROP TABLE IF EXISTS idempotency_keys")
    op.execute("DROP TABLE IF EXISTS outbox_events")
    op.execute("DROP TABLE IF EXISTS sms_default")
    op.execute("DROP TABLE IF EXISTS sms_2026_12")
    op.execute("DROP TABLE IF EXISTS sms_2026_11")
    op.execute("DROP TABLE IF EXISTS sms_2026_10")
    op.execute("DROP TABLE IF EXISTS sms_2026_09")
    op.execute("DROP TABLE IF EXISTS sms_2026_08")
    op.execute("DROP TABLE IF EXISTS sms_2026_07")
    op.execute("DROP TABLE IF EXISTS sms")
    op.execute("DROP TABLE IF EXISTS batches")
    op.execute("DROP TABLE IF EXISTS topups")
    op.execute("DROP TABLE IF EXISTS wallet_ledger")
    op.execute("DROP TABLE IF EXISTS wallets")
    op.execute("DROP TABLE IF EXISTS tenants")

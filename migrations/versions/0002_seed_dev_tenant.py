"""seed dev tenant

Matches README.md Quick start: X-Tenant-ID 11111111-1111-1111-1111-111111111111
with 100,000 credits.

Revision ID: 0002
Revises: 0001
Create Date: 2026-07-11
"""
import os
import uuid
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import UUID as PG_UUID

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

DEV_TENANT_ID = uuid.UUID("11111111-1111-1111-1111-111111111111")


def upgrade() -> None:
    # This is fixture data, not schema — the standard `alembic upgrade head`
    # one-shot migrate path (docker/Dockerfile.migrate) has no environment
    # gate of its own, so without this the dev tenant would be seeded into
    # production too. Opt-in only: unset/anything-but-"true" is a no-op.
    if os.environ.get("SEED_DEV_DATA") != "true":
        return

    op.execute(
        sa.text("INSERT INTO tenants (id, name) VALUES (:tenant_id, 'Dev Tenant')").bindparams(
            sa.bindparam("tenant_id", value=DEV_TENANT_ID, type_=PG_UUID(as_uuid=True))
        )
    )
    op.execute(
        sa.text("INSERT INTO wallets (tenant_id, balance) VALUES (:tenant_id, 100000)").bindparams(
            sa.bindparam("tenant_id", value=DEV_TENANT_ID, type_=PG_UUID(as_uuid=True))
        )
    )
    op.execute(
        sa.text(
            """
            INSERT INTO wallet_ledger (tenant_id, delta, reason, reference_id, balance_after)
            VALUES (:tenant_id, 100000, 'TOPUP', :tenant_id, 100000)
            """
        ).bindparams(sa.bindparam("tenant_id", value=DEV_TENANT_ID, type_=PG_UUID(as_uuid=True)))
    )


def downgrade() -> None:
    tenant_id_param = sa.bindparam("tenant_id", value=DEV_TENANT_ID, type_=PG_UUID(as_uuid=True))
    op.execute(sa.text("DELETE FROM wallet_ledger WHERE tenant_id = :tenant_id").bindparams(tenant_id_param))
    op.execute(sa.text("DELETE FROM wallets WHERE tenant_id = :tenant_id").bindparams(tenant_id_param))
    op.execute(sa.text("DELETE FROM tenants WHERE id = :tenant_id").bindparams(tenant_id_param))

import asyncio
import uuid

import asyncpg
import httpx
import pytest

pytestmark = pytest.mark.integration


async def _provision_tenant(postgres_url: str, balance: int) -> uuid.UUID:
    tenant_id = uuid.uuid4()
    # asyncpg wants a plain postgresql:// DSN, not the +asyncpg SQLAlchemy form.
    dsn = postgres_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("INSERT INTO tenants (id, name) VALUES ($1, $2)", tenant_id, "concurrency-test")
        await conn.execute("INSERT INTO wallets (tenant_id, balance) VALUES ($1, $2)", tenant_id, balance)
    finally:
        await conn.close()
    return tenant_id


@pytest.mark.asyncio
async def test_exactly_n_minus_one_concurrent_sends_succeed(client, postgres_url):
    """docs/testing.md Concurrency tests — the direct proof of the atomic
    UPDATE...RETURNING invariant (docs/database.md Concurrency): a tenant
    funded for exactly N-1 sends must see exactly N-1 succeed and exactly
    one 402, with no lost updates and no double-spend, regardless of
    concurrent arrival order.
    """
    n = 10
    tenant_id = await _provision_tenant(postgres_url, balance=n - 1)
    headers_base = {"X-Tenant-ID": str(tenant_id), "Content-Type": "application/json"}

    async def submit() -> httpx.Response:
        headers = {**headers_base, "Idempotency-Key": str(uuid.uuid4())}
        return await client.post(
            "/api/v1/sms",
            headers=headers,
            json={"recipient": "+15551234567", "message": "race", "priority": "NORMAL"},
        )

    responses = await asyncio.gather(*[submit() for _ in range(n)])
    statuses = [r.status_code for r in responses]

    assert statuses.count(202) == n - 1
    assert statuses.count(402) == 1
    assert len(statuses) == n

    wallet = await client.get("/api/v1/wallet", headers={"X-Tenant-ID": str(tenant_id)})
    assert wallet.json()["balance"] == 0


@pytest.mark.asyncio
async def test_concurrent_identical_idempotency_key_only_charges_once(client, postgres_url):
    """A different idempotency mechanism than the wallet race above (ADR-009)
    — N concurrent requests sharing one Idempotency-Key must result in
    exactly one accepted charge, not N.
    """
    n = 10
    tenant_id = await _provision_tenant(postgres_url, balance=100)
    shared_key = str(uuid.uuid4())
    headers = {
        "X-Tenant-ID": str(tenant_id),
        "Idempotency-Key": shared_key,
        "Content-Type": "application/json",
    }
    body = {"recipient": "+15551234567", "message": "same key", "priority": "NORMAL"}

    async def submit() -> httpx.Response:
        return await client.post("/api/v1/sms", headers=headers, json=body)

    responses = await asyncio.gather(*[submit() for _ in range(n)])
    statuses = [r.status_code for r in responses]

    # Every response is either the one true acceptance (202) or a safe
    # in-flight/replay signal (409) — never a second distinct charge.
    assert statuses.count(202) >= 1
    assert all(s in (202, 409) for s in statuses)

    wallet = await client.get("/api/v1/wallet", headers={"X-Tenant-ID": str(tenant_id)})
    assert wallet.json()["balance"] == 99  # exactly one deduction of 1 credit

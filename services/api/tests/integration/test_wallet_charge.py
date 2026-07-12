import asyncio
import uuid

import asyncpg
import httpx
import pytest

pytestmark = pytest.mark.integration


async def _provision_tenant(postgres_url: str, balance: int) -> uuid.UUID:
    tenant_id = uuid.uuid4()
    dsn = postgres_url.replace("postgresql+asyncpg://", "postgresql://")
    conn = await asyncpg.connect(dsn)
    try:
        await conn.execute("INSERT INTO tenants (id, name) VALUES ($1, $2)", tenant_id, "charge-test")
        await conn.execute("INSERT INTO wallets (tenant_id, balance) VALUES ($1, $2)", tenant_id, balance)
    finally:
        await conn.close()
    return tenant_id


@pytest.mark.asyncio
async def test_charge_wallet_persists_across_a_separate_request(client, postgres_url):
    """Regression test for the fixed commit bug: charge_wallet() used to only
    flush(), never commit(), so the balance increase was rolled back the
    moment the request-scoped session closed. Reading the balance back via a
    brand-new request (a fresh session) is the only way to actually observe
    that bug — asserting within the same request/session would pass even
    with the old broken code.
    """
    tenant_id = await _provision_tenant(postgres_url, balance=0)
    headers = {
        "X-Tenant-ID": str(tenant_id),
        "Idempotency-Key": str(uuid.uuid4()),
        "Content-Type": "application/json",
    }

    charge_response = await client.post("/api/v1/wallet/charge", headers=headers, json={"amount": 500})
    assert charge_response.status_code == 201
    assert charge_response.json()["balance_after"] == 500

    wallet_response = await client.get("/api/v1/wallet", headers={"X-Tenant-ID": str(tenant_id)})
    assert wallet_response.json()["balance"] == 500


@pytest.mark.asyncio
async def test_charge_wallet_missing_idempotency_key_is_rejected(client, postgres_url):
    tenant_id = await _provision_tenant(postgres_url, balance=0)

    response = await client.post(
        "/api/v1/wallet/charge",
        headers={"X-Tenant-ID": str(tenant_id), "Content-Type": "application/json"},
        json={"amount": 500},
    )

    assert response.status_code == 400
    assert response.json()["error"]["code"] == "MISSING_IDEMPOTENCY_KEY"


@pytest.mark.asyncio
async def test_concurrent_identical_idempotency_key_only_charges_once(client, postgres_url):
    n = 10
    tenant_id = await _provision_tenant(postgres_url, balance=0)
    shared_key = str(uuid.uuid4())
    headers = {
        "X-Tenant-ID": str(tenant_id),
        "Idempotency-Key": shared_key,
        "Content-Type": "application/json",
    }
    body = {"amount": 500}

    async def submit() -> httpx.Response:
        return await client.post("/api/v1/wallet/charge", headers=headers, json=body)

    responses = await asyncio.gather(*[submit() for _ in range(n)])
    statuses = [r.status_code for r in responses]

    assert statuses.count(201) >= 1
    assert all(s in (201, 409) for s in statuses)

    wallet_response = await client.get("/api/v1/wallet", headers={"X-Tenant-ID": str(tenant_id)})
    assert wallet_response.json()["balance"] == 500  # exactly one charge, not n

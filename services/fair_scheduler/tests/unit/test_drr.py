from collections import deque

import pytest
from fair_scheduler.drr import DeficitRoundRobin


class FakeQueuePort:
    """In-memory TenantQueuePort fake — no Redis needed. Mirrors
    RedisDrrStore's FIFO-list-per-tenant + active-set semantics exactly, so
    DeficitRoundRobin is fully unit-testable in isolation. Direct
    replacement for the deleted Go repo's "DRR (6 tests passing)".
    """

    def __init__(self) -> None:
        self.queues: dict[str, deque] = {}
        self.active: list[str] = []

    async def enqueue(self, tenant_id: str, message: dict) -> None:
        self.queues.setdefault(tenant_id, deque()).append(message)
        if tenant_id not in self.active:
            self.active.append(tenant_id)

    async def dequeue(self, tenant_id: str) -> dict | None:
        queue = self.queues.get(tenant_id)
        if not queue:
            return None
        return queue.popleft()

    async def requeue_front(self, tenant_id: str, message: dict) -> None:
        self.queues.setdefault(tenant_id, deque()).appendleft(message)

    async def is_empty(self, tenant_id: str) -> bool:
        return not self.queues.get(tenant_id)

    async def active_tenants(self) -> list[str]:
        return list(self.active)

    async def add_active(self, tenant_id: str) -> None:
        if tenant_id not in self.active:
            self.active.append(tenant_id)

    async def remove_active(self, tenant_id: str) -> None:
        if tenant_id in self.active:
            self.active.remove(tenant_id)


def msg(cost: int = 1) -> dict:
    return {"cost": cost}


@pytest.mark.asyncio
async def test_deficit_arithmetic_admits_floor_quantum_over_cost():
    store = FakeQueuePort()
    for _ in range(10):
        await store.enqueue("tenant-a", msg(cost=3))

    drr = DeficitRoundRobin(store, quantum=10)
    admitted = await drr.run_round()

    # deficit=10, cost=3 per message -> 3 dispatched (9 spent), 1 left over
    assert len(admitted) == 3
    assert await store.is_empty("tenant-a") is False


@pytest.mark.asyncio
async def test_no_starvation_every_active_tenant_served_in_one_round():
    store = FakeQueuePort()
    for _ in range(1000):
        await store.enqueue("heavy-tenant", msg(cost=1))
    await store.enqueue("light-tenant", msg(cost=1))

    drr = DeficitRoundRobin(store, quantum=10)
    admitted = await drr.run_round()

    # A 1M-backlog and a 1-message backlog get equal per-round share: the
    # light tenant's single message is fully drained, the heavy tenant gets
    # exactly one quantum's worth (10), not starved by the huge backlog.
    assert await store.is_empty("light-tenant") is True
    assert len(store.queues["heavy-tenant"]) == 990
    assert len(admitted) == 11  # heavy tenant's 10 (quantum) + light tenant's 1


@pytest.mark.asyncio
async def test_single_active_tenant_gets_full_capacity():
    store = FakeQueuePort()
    for _ in range(5):
        await store.enqueue("solo-tenant", msg(cost=1))

    drr = DeficitRoundRobin(store, quantum=10)
    admitted = await drr.run_round()

    # Only one active tenant -> round-robin degenerates to "always this
    # tenant's turn": all 5 ready messages drain in one round (bounded only
    # by quantum=10, not artificially throttled).
    assert len(admitted) == 5
    assert await store.is_empty("solo-tenant") is True


@pytest.mark.asyncio
async def test_deficit_resets_to_zero_on_empty_queue():
    store = FakeQueuePort()
    await store.enqueue("tenant-a", msg(cost=1))

    drr = DeficitRoundRobin(store, quantum=10)
    await drr.run_round()  # drains the single message, deficit -> 9 leftover before reset

    assert drr._deficit["tenant-a"] == 0
    assert "tenant-a" not in await store.active_tenants()


@pytest.mark.asyncio
async def test_newly_active_tenant_enters_at_zero_deficit_no_banked_credit():
    store = FakeQueuePort()
    drr = DeficitRoundRobin(store, quantum=10)

    # Round 1: a single cheap message leaves 9 units of quantum unspent
    # right before the queue empties and the tenant goes inactive. If that
    # leftover were wrongly carried forward (the abuse case docs/queue.md
    # warns about — banking credit while idle), it would inflate every
    # future round's effective quantum.
    await store.enqueue("tenant-a", msg(cost=1))
    await drr.run_round()
    assert await store.active_tenants() == []  # went inactive, deficit reset

    # Round 2: tenant reactivates with abundant cost-3 supply (never runs
    # dry mid-round, so the queue stays non-empty and no reset masks the
    # result). A correctly-reset deficit (0 + quantum 10) admits floor(10/3)
    # = 3; a "no reset" bug (9 leftover + 10 = 19) would admit floor(19/3) = 6.
    for _ in range(100):
        await store.enqueue("tenant-a", msg(cost=3))
    admitted = await drr.run_round()

    assert len(admitted) == 3
    assert await store.is_empty("tenant-a") is False


@pytest.mark.asyncio
async def test_tenant_removed_from_active_set_when_drained():
    store = FakeQueuePort()
    await store.enqueue("tenant-a", msg(cost=1))
    await store.enqueue("tenant-b", msg(cost=1))

    drr = DeficitRoundRobin(store, quantum=10)
    await drr.run_round()

    assert await store.active_tenants() == []

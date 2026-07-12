from gateway_common.redis.drr_store import TenantQueuePort

# docs/queue.md Fair Scheduler — Deficit Round Robin (Shreedhar & Varghese).
# Pure, I/O-free: unit-testable against a fake TenantQueuePort with no Redis,
# the direct replacement for the deleted Go repo's "DRR (6 tests passing)".
DEFAULT_QUANTUM = 10


def message_cost(message: dict) -> int:
    return int(message.get("cost", 1))


class DeficitRoundRobin:
    def __init__(self, store: TenantQueuePort, *, quantum: int = DEFAULT_QUANTUM) -> None:
        self._store = store
        self._quantum = quantum
        self._deficit: dict[str, int] = {}

    async def run_round(self) -> list[dict]:
        """One full pass over active_tenants, each getting `quantum` worth of
        deficit added, then dispatching while affordable. Returns the
        messages admitted this round, in dispatch order.
        """
        admitted: list[dict] = []

        for tenant_id in await self._store.active_tenants():
            self._deficit[tenant_id] = self._deficit.get(tenant_id, 0) + self._quantum

            while not await self._store.is_empty(tenant_id):
                message = await self._peek_cost_gate(tenant_id)
                if message is None:
                    break
                admitted.append(message)

            if await self._store.is_empty(tenant_id):
                # Step 4: reset deficit to 0 on empty — no banking credit
                # while idle (docs/queue.md, the anti-abuse detail).
                self._deficit[tenant_id] = 0
                await self._store.remove_active(tenant_id)

        return admitted

    async def _peek_cost_gate(self, tenant_id: str) -> dict | None:
        # Dequeue-then-requeue-on-reject: the store has no non-destructive
        # peek, so an unaffordable message is dequeued and immediately
        # pushed back to the head via requeue_front, undoing the pop.
        message = await self._store.dequeue(tenant_id)
        if message is None:
            return None

        cost = message_cost(message)
        if self._deficit[tenant_id] < cost:
            await self._store.requeue_front(tenant_id, message)
            return None

        self._deficit[tenant_id] -= cost
        return message

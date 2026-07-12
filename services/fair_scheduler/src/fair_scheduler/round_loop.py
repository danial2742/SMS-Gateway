import asyncio

from gateway_common.logging import get_logger
from gateway_common.metrics import scheduler_active_tenants
from gateway_common.redis.drr_store import RedisDrrStore
from gateway_common.shutdown import GracefulShutdown

from fair_scheduler.drr import DeficitRoundRobin

logger = get_logger()


async def round_loop(
    drr: DeficitRoundRobin,
    store: RedisDrrStore,
    shutdown: GracefulShutdown,
    *,
    round_interval_seconds: float,
) -> None:
    """Drives the pure DRR algorithm against real Redis state each round,
    pushing admitted messages to `dispatch:ready:normal` — the transport
    Normal Workers BLPOP from (plan's resolution for the missing scheduler->
    worker RPC the docs describe only as a diagram arrow).
    """
    while not shutdown.should_stop:
        admitted = await drr.run_round()

        for message in admitted:
            await store.push_ready("normal", message)

        active = await store.active_tenants()
        scheduler_active_tenants.set(len(active))

        if not admitted:
            await asyncio.sleep(round_interval_seconds)
        else:
            logger.info("scheduler_round_admitted", count=len(admitted))

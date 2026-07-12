import asyncio
import signal
from collections.abc import Awaitable, Callable


class GracefulShutdown:
    """SIGTERM-triggered drain helper: workers/relay/scheduler loops check
    `.should_stop` between units of work and exit their loop rather than
    being killed mid-operation. `grace_period_seconds` bounds how long a
    K8s rolling update waits before SIGKILL.
    """

    def __init__(self, grace_period_seconds: int = 30) -> None:
        self.grace_period_seconds = grace_period_seconds
        self._stop_event = asyncio.Event()

    @property
    def should_stop(self) -> bool:
        return self._stop_event.is_set()

    def install(self, loop: asyncio.AbstractEventLoop | None = None) -> None:
        loop = loop or asyncio.get_event_loop()
        for sig in (signal.SIGTERM, signal.SIGINT):
            loop.add_signal_handler(sig, self._stop_event.set)

    async def wait(self) -> None:
        await self._stop_event.wait()

    async def run_until_stopped(self, tick: Callable[[], Awaitable[None]]) -> None:
        while not self.should_stop:
            await tick()

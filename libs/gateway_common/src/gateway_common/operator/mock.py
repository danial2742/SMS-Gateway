import uuid

from gateway_common.operator.protocol import OperatorClientError, OperatorServerError

# Reserved test recipients that force a specific outcome, useful for compose/dev
# and worker integration tests without needing a real operator endpoint.
_FORCE_4XX_SUFFIX = "0000"
_FORCE_5XX_SUFFIX = "9999"


class MockOperatorClient:
    """Configurable fake OperatorClient (docs/testing.md: "a real interface
    substitution, not a database mock") for local `docker compose up` and
    worker tests. Deterministic by recipient suffix so tests can force
    retryable vs. non-retryable failures without extra plumbing.
    """

    def __init__(self, *, fail_rate: float = 0.0) -> None:
        self.fail_rate = fail_rate
        self.calls: list[tuple[str, str, str]] = []

    async def send(self, *, recipient: str, message: str, sms_id: str) -> str:
        self.calls.append((recipient, message, sms_id))

        if recipient.endswith(_FORCE_4XX_SUFFIX):
            raise OperatorClientError(422, "invalid destination")
        if recipient.endswith(_FORCE_5XX_SUFFIX):
            raise OperatorServerError(503, "operator unavailable")

        return f"mock-{uuid.uuid4()}"

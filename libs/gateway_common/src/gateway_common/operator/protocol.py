from typing import Protocol


class OperatorTimeoutError(Exception):
    """Retryable: operator call did not complete within the configured timeout."""


class OperatorServerError(Exception):
    """Retryable: operator returned 5xx."""

    def __init__(self, status_code: int, message: str = "") -> None:
        super().__init__(message or f"operator returned {status_code}")
        self.status_code = status_code


class OperatorClientError(Exception):
    """Non-retryable: operator returned 4xx (invalid destination, blocked, etc.)."""

    def __init__(self, status_code: int, message: str = "") -> None:
        super().__init__(message or f"operator returned {status_code}")
        self.status_code = status_code


class OperatorClient(Protocol):
    """docs/assumptions.md #2: the Operator API is a black-box HTTP endpoint,
    abstracted behind this interface. Its own rate limits/SLA/idempotency
    support are unknown — modeled only as "can time out or return 5xx/4xx."
    Structural typing: HttpOperatorClient and MockOperatorClient both satisfy
    this without inheritance, mirroring the Go interface idiom this replaces.
    """

    async def send(self, *, recipient: str, message: str, sms_id: str) -> str:
        """Returns an operator_message_id on success. Raises
        OperatorTimeoutError/OperatorServerError (retryable) or
        OperatorClientError (non-retryable) on failure.
        """
        ...

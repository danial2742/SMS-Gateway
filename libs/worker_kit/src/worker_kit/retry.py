import random

from gateway_common.domain.enums import Tier
from gateway_common.operator.protocol import OperatorServerError, OperatorTimeoutError

# docs/queue.md Retry Queue table. `attempt_count` (from the Kafka header) is
# read here as "retries already used" — 0 on first delivery. If it's below
# the tier's max, the message is retried once more; the backoff schedule has
# exactly `max_attempts` entries, one per retry.
_EXPRESS_BACKOFF_SECONDS = [0.2, 0.4]
_NORMAL_MAX_ATTEMPTS = 5

# Multiplicative jitter applied on top of the base schedule below, so
# simultaneous failures (e.g. an operator-wide outage recovering) don't all
# retry in lockstep. Centered on 1.0 so the *expected* delay still matches
# the documented fixed/exponential schedule.
_JITTER_LOW = 0.5
_JITTER_HIGH = 1.5


def _express_backoff_base(n: int) -> float:
    return _EXPRESS_BACKOFF_SECONDS[min(n, len(_EXPRESS_BACKOFF_SECONDS) - 1)]


def _normal_backoff_base(n: int) -> float:
    return min(0.5 * (2 ** min(n, _NORMAL_MAX_ATTEMPTS - 1)), 30.0)


def max_attempts(tier: Tier) -> int:
    return len(_EXPRESS_BACKOFF_SECONDS) if tier == Tier.EXPRESS else _NORMAL_MAX_ATTEMPTS


def should_retry(tier: Tier, attempt_count: int) -> bool:
    return attempt_count < max_attempts(tier)


def backoff_seconds(tier: Tier, attempt_count: int) -> float:
    base = (
        _express_backoff_base(attempt_count)
        if tier == Tier.EXPRESS
        else _normal_backoff_base(attempt_count)
    )
    return base * random.uniform(_JITTER_LOW, _JITTER_HIGH)


def is_retryable(exc: Exception) -> bool:
    """Retryable: operator timeout, 5xx, network errors. Non-retryable:
    operator 4xx — retrying cannot change the outcome (docs/queue.md).
    """
    return isinstance(exc, (OperatorTimeoutError, OperatorServerError))

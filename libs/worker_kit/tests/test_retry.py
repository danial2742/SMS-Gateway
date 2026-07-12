import pytest
from gateway_common.domain.enums import Tier
from gateway_common.operator.protocol import (
    OperatorClientError,
    OperatorServerError,
    OperatorTimeoutError,
)
from worker_kit.retry import (
    _express_backoff_base,
    _normal_backoff_base,
    backoff_seconds,
    is_retryable,
    max_attempts,
    should_retry,
)


def test_express_max_attempts_matches_docs_queue_table():
    assert max_attempts(Tier.EXPRESS) == 2


def test_normal_max_attempts_matches_docs_queue_table():
    assert max_attempts(Tier.NORMAL) == 5


def test_express_backoff_schedule_is_fixed_200_400ms():
    # jittered 0.5x-1.5x around the base schedule — assert bounds, not exact
    # values, since backoff_seconds() now applies random jitter.
    assert 0.1 <= backoff_seconds(Tier.EXPRESS, attempt_count=0) <= 0.3
    assert 0.2 <= backoff_seconds(Tier.EXPRESS, attempt_count=1) <= 0.6


def test_normal_backoff_is_exponential():
    assert 0.25 <= backoff_seconds(Tier.NORMAL, attempt_count=0) <= 0.75
    assert 0.5 <= backoff_seconds(Tier.NORMAL, attempt_count=1) <= 1.5
    assert 1.0 <= backoff_seconds(Tier.NORMAL, attempt_count=2) <= 3.0
    assert 2.0 <= backoff_seconds(Tier.NORMAL, attempt_count=3) <= 6.0


def test_express_backoff_base_schedule_is_fixed_200_400ms():
    assert _express_backoff_base(0) == pytest.approx(0.2)
    assert _express_backoff_base(1) == pytest.approx(0.4)


def test_normal_backoff_base_is_exponential():
    assert _normal_backoff_base(0) == pytest.approx(0.5)
    assert _normal_backoff_base(1) == pytest.approx(1.0)
    assert _normal_backoff_base(2) == pytest.approx(2.0)
    assert _normal_backoff_base(3) == pytest.approx(4.0)


def test_normal_backoff_base_beyond_schedule_length_clamps_to_last_entry():
    # should_retry() never lets attempt_count reach here in practice (max
    # attempts=5, so the schedule only ever needs indices 0-4 = 8s max) —
    # this just proves out-of-range input degrades safely rather than
    # raising an IndexError.
    assert _normal_backoff_base(999) == _normal_backoff_base(4)


def test_should_retry_true_below_max_attempts():
    assert should_retry(Tier.EXPRESS, attempt_count=0) is True
    assert should_retry(Tier.EXPRESS, attempt_count=1) is True


def test_should_retry_false_at_max_attempts():
    assert should_retry(Tier.EXPRESS, attempt_count=2) is False
    assert should_retry(Tier.NORMAL, attempt_count=5) is False


def test_timeout_and_5xx_are_retryable():
    assert is_retryable(OperatorTimeoutError("timeout")) is True
    assert is_retryable(OperatorServerError(503)) is True


def test_4xx_is_not_retryable():
    assert is_retryable(OperatorClientError(422)) is False

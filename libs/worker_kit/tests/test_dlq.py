import uuid

import pytest
from gateway_common.domain.enums import Tier
from gateway_common.kafka.producer import GatewayProducer
from gateway_common.metrics import dlq_messages_total, dlq_publish_failed_total
from worker_kit.dlq import publish_to_dlq


class FakeProducer(GatewayProducer):
    """Records send() calls instead of hitting Kafka — no broker needed."""

    def __init__(self) -> None:
        super().__init__(brokers=["localhost:9092"])
        self.sent: list[dict] = []

    async def send(
        self, topic: str, payload: dict, *, tenant_id: uuid.UUID, request_id: str, attempt_count: int = 0
    ) -> None:
        self.sent.append(
            {"topic": topic, "payload": payload, "tenant_id": tenant_id, "request_id": request_id}
        )


@pytest.mark.asyncio
async def test_publish_to_dlq_sends_payload_with_failure_metadata():
    producer = FakeProducer()

    await publish_to_dlq(
        producer,
        "sms.express.dlq",
        {"sms_id": "abc"},
        tenant_id=uuid.uuid4(),
        request_id="req-1",
        error="boom",
        attempt_count=2,
        first_attempted_at="2026-07-11T00:00:00+00:00",
        last_attempted_at="2026-07-11T00:00:05+00:00",
        reason="retries_exhausted",
        tier=Tier.EXPRESS,
    )

    assert len(producer.sent) == 1
    sent = producer.sent[0]
    assert sent["topic"] == "sms.express.dlq"
    assert sent["payload"]["sms_id"] == "abc"
    assert sent["payload"]["error"] == "boom"
    assert sent["payload"]["attempt_count"] == 2


@pytest.mark.asyncio
async def test_publish_to_dlq_metric_uses_explicit_tier_not_topic_substring():
    """Regression test for the fix: tier must come from the explicit `tier`
    param, not from substring-matching dlq_topic — a topic name that
    contains neither "express" nor "normal" must still label correctly.
    """
    producer = FakeProducer()
    before = dlq_messages_total.labels(tier=Tier.EXPRESS, reason="non_retryable")._value.get()

    await publish_to_dlq(
        producer,
        "some-renamed-topic-without-tier-name",
        {"sms_id": "xyz"},
        tenant_id=uuid.uuid4(),
        request_id="req-2",
        error="boom",
        attempt_count=0,
        first_attempted_at="2026-07-11T00:00:00+00:00",
        last_attempted_at="2026-07-11T00:00:00+00:00",
        reason="non_retryable",
        tier=Tier.EXPRESS,
    )

    after = dlq_messages_total.labels(tier=Tier.EXPRESS, reason="non_retryable")._value.get()
    assert after == before + 1


class FlakyThenSucceedsProducer(GatewayProducer):
    """Raises on the first N calls, then succeeds — proves publish_to_dlq's
    bounded retry actually retries rather than giving up on the first error.
    """

    def __init__(self, fail_times: int) -> None:
        super().__init__(brokers=["localhost:9092"])
        self._fail_times = fail_times
        self.calls = 0
        self.sent: list[dict] = []

    async def send(
        self, topic: str, payload: dict, *, tenant_id: uuid.UUID, request_id: str, attempt_count: int = 0
    ) -> None:
        self.calls += 1
        if self.calls <= self._fail_times:
            raise ConnectionError("kafka unavailable")
        self.sent.append({"topic": topic, "payload": payload})


class AlwaysFailsProducer(GatewayProducer):
    def __init__(self) -> None:
        super().__init__(brokers=["localhost:9092"])
        self.calls = 0

    async def send(
        self, topic: str, payload: dict, *, tenant_id: uuid.UUID, request_id: str, attempt_count: int = 0
    ) -> None:
        self.calls += 1
        raise ConnectionError("kafka unavailable")


@pytest.mark.asyncio
async def test_publish_to_dlq_retries_and_eventually_succeeds():
    producer = FlakyThenSucceedsProducer(fail_times=2)

    await publish_to_dlq(
        producer,
        "sms.express.dlq",
        {"sms_id": "abc"},
        tenant_id=uuid.uuid4(),
        request_id="req-3",
        error="boom",
        attempt_count=0,
        first_attempted_at="2026-07-11T00:00:00+00:00",
        last_attempted_at="2026-07-11T00:00:00+00:00",
        reason="retries_exhausted",
        tier=Tier.EXPRESS,
    )

    assert producer.calls == 3
    assert len(producer.sent) == 1


@pytest.mark.asyncio
async def test_publish_to_dlq_swallows_and_counts_when_retries_exhausted():
    producer = AlwaysFailsProducer()
    before = dlq_publish_failed_total.labels(tier=Tier.EXPRESS)._value.get()

    await publish_to_dlq(
        producer,
        "sms.express.dlq",
        {"sms_id": "abc"},
        tenant_id=uuid.uuid4(),
        request_id="req-4",
        error="boom",
        attempt_count=0,
        first_attempted_at="2026-07-11T00:00:00+00:00",
        last_attempted_at="2026-07-11T00:00:00+00:00",
        reason="retries_exhausted",
        tier=Tier.EXPRESS,
    )  # must not raise — last-resort mitigation, not a hard failure

    after = dlq_publish_failed_total.labels(tier=Tier.EXPRESS)._value.get()
    assert after == before + 1

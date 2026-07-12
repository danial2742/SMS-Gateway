from gateway_common.domain.enums import Priority

# Request-id/tenant-id propagate as message headers end-to-end (observability.md
# Correlation IDs) — header keys are shared here so every producer/consumer
# agrees on them.
HEADER_REQUEST_ID = "request_id"
HEADER_ATTEMPT_COUNT = "attempt_count"
HEADER_TENANT_ID = "tenant_id"


def topic_for_priority(priority: Priority, *, express_topic: str, normal_topic: str) -> str:
    return express_topic if priority == Priority.EXPRESS else normal_topic


def dlq_topic_for_tier(tier: str, *, dlq_express: str, dlq_normal: str) -> str:
    return dlq_express if tier == "express" else dlq_normal

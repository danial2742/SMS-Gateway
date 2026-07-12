"""Metric definitions matching docs/observability.md's Metrics table exactly.
One shared registry — every service imports the subset it emits.
"""
from prometheus_client import Counter, Gauge, Histogram

http_request_duration_seconds = Histogram(
    "http_request_duration_seconds", "API RED latency", ["route", "status"]
)

wallet_deduction_duration_seconds = Histogram(
    "wallet_deduction_duration_seconds", "Wallet deduction latency"
)
wallet_insufficient_balance_total = Counter(
    "wallet_insufficient_balance_total", "Insufficient-balance rejections", ["tenant"]
)

outbox_unpublished_rows = Gauge(
    "outbox_unpublished_rows", "Rows in outbox_events not yet published"
)
outbox_oldest_unpublished_age_seconds = Gauge(
    "outbox_oldest_unpublished_age_seconds", "Age of the oldest unpublished outbox row"
)

scheduler_active_tenants = Gauge(
    "scheduler_active_tenants", "Tenants currently active in the DRR round"
)
scheduler_tenant_wait_seconds = Histogram(
    "scheduler_tenant_wait_seconds", "Time a tenant's message waits in the DRR queue"
)
scheduler_deficit_distribution = Histogram(
    "scheduler_deficit_distribution", "Distribution of per-tenant DRR deficit counters"
)

operator_dispatch_duration_seconds = Histogram(
    "operator_dispatch_duration_seconds", "Operator API call latency", ["tier"]
)
operator_dispatch_result_total = Counter(
    "operator_dispatch_result_total", "Operator dispatch outcomes", ["tier", "outcome"]
)

dlq_messages_total = Counter("dlq_messages_total", "Messages routed to DLQ", ["tier", "reason"])
dlq_publish_failed_total = Counter(
    "dlq_publish_failed_total", "DLQ publish attempts that exhausted retries", ["tier"]
)

idempotency_key_conflict_total = Counter(
    "idempotency_key_conflict_total", "Idempotency key conflicts (in-flight or reused)"
)

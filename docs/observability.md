# Observability

How this system is watched in production: logs, metrics, tracing, and the alerting built on top of them. Health checks (liveness/readiness) live in [deployment.md](deployment.md) — this document covers everything used to understand *behavior*, not process health.

## Logging

- **Structured JSON**, one line per event (`structlog`, JSON-rendered, with lazy field binding to avoid the reflection-based-encoding cost that matters at this log volume). See [decisions.md](decisions.md) ADR-010 for the full rationale and alternatives considered.
- **Standard fields:** `timestamp`, `level`, `service`, `tenant_id`, `request_id`, `sms_id`/`batch_id`, `event`, `latency_ms`, `queue`/`topic`, `attempt_count`. Enforced via a shared internal logging wrapper, not left to per-service convention — an ungoverned field vocabulary defeats cross-service correlation.
- **No message content or PII in logs.** SMS body and recipient number are never logged in full — a truncated/hashed reference only, if anything. Hard rule, not a style preference: SMS content is customer data, and centralized log aggregation is a materially larger exposure surface than the database itself. See [security.md](security.md) Audit logging for where this data *is* legitimately retained.
- **Levels:** `ERROR` for anything requiring operator attention (DLQ writes, DB failures, refund failures), `WARN` for retries and degraded-but-recovering states, `INFO` for state transitions (accepted, dispatched, sent, failed), `DEBUG` off in production (`LOG_LEVEL` env var — [deployment.md](deployment.md)).
- **Aggregation:** shipped to Loki or ELK; retention tuned separately from database retention — logs are for operational debugging (weeks), not audit (that's `wallet_ledger`'s job — permanent, queryable via SQL, not grep; see [database.md](database.md)).

## Correlation IDs and request IDs

Every inbound HTTP request is assigned a `request_id` at the API edge (generated if the client didn't supply one via a standard tracing header). This ID:

- Is returned in every error response body (`error.request_id`, [api.md](api.md) Conventions) so a client reporting an issue hands back the exact key needed to find the relevant logs.
- Propagates through Kafka message headers end-to-end: API → outbox row (as part of the persisted payload context) → relay → scheduler → worker → operator call logging. A single log query on `request_id` reconstructs the *entire* lifecycle of one SMS across every service it touched, in order.
- Is distinct from `sms_id`/`batch_id`: `request_id` identifies one HTTP call (useful for "what happened when this specific API call came in," including validation failures that never produce an `sms_id` at all); `sms_id` identifies one message's lifecycle from acceptance through terminal state, which may span multiple internal retries, each a separate dispatch attempt but the same logical message.

This propagation is what makes the [queue.md](queue.md) pipeline debuggable end-to-end despite being asynchronous and multi-hop — without it, correlating "this customer's message" across five independently scaled services would require timestamp-matching guesswork.

## Tracing

**Deferred — not implemented in v1.** The design calls for distributed tracing (OpenTelemetry, exported to a backend such as Tempo or Jaeger) instrumenting the synchronous portion of the request lifecycle in detail — the API request's validation → idempotency check → wallet transaction → outbox insert span tree — where sub-request timing breakdown matters for diagnosing latency regressions on the money-touching write path.

For the asynchronous portion (relay → Kafka → scheduler → worker → operator), tracing context would propagate via the same Kafka message headers carrying `request_id`, but spans across an async hop are necessarily linked rather than a single continuous trace in the traditional synchronous sense — a message can sit in `sms.normal` for a DRR-scheduled interval that has nothing to do with processing latency, and collapsing that wait time into a "slow trace" would be misleading rather than informative. Trace spans would therefore be scoped per-hop (ingest span, publish span, dispatch span) and linked by `request_id`/`sms_id`, not forced into one artificial end-to-end span that conflates "time spent waiting for a fair scheduling turn" with "time spent doing work."

Tracing is the tool reached for when structured logs answer "what happened" but not "where did the time go" — logging is the default, always-on instrument; tracing would be the deeper diagnostic layered on top for latency investigation specifically, sampled rather than exhaustive at this volume to control cost. Until it's built out (a real `TracerProvider` with an exporter, wired per service), rely on `request_id`-correlated structured logs for cross-service investigation.

## Metrics

Prometheus + Grafana. RED (Rate/Errors/Duration) at every service boundary, plus domain-specific signals that map directly to this system's invariants — a metric exists here because it can catch a *specific* claimed guarantee breaking, not as generic coverage:

| Category | Metric | Why it matters |
|---|---|---|
| API | `http_request_duration_seconds{route,status}` | Standard RED |
| Wallet | `wallet_deduction_duration_seconds`, `wallet_insufficient_balance_total{tenant}` | Deduction latency is on the critical path of every request; rejection rate is a customer-facing signal worth its own dashboard |
| Outbox | `outbox_unpublished_rows`, `outbox_oldest_unpublished_age_seconds` | **The single most important health signal for the async pipeline** — a growing/aging backlog is the earliest indicator of a Kafka problem, well before consumer-side symptoms appear |
| Kafka | Consumer lag per topic/partition (Kafka Lag Exporter), broker-level RF/ISR health | Direct visibility into both Express and Normal pipeline health |
| Fair Scheduler | `scheduler_active_tenants`, `scheduler_tenant_wait_seconds{p50,p99}`, `scheduler_deficit_distribution` | Proves the fairness property in production, not just in design — if one tenant's p99 wait diverges sharply from the median at similar volume, the DRR implementation has a bug ([testing.md](testing.md) Concurrency tests) |
| Workers | `operator_dispatch_duration_seconds{tier}`, `operator_dispatch_result_total{tier,outcome}` | Split by `tier=express\|normal` specifically to make the Express SLA independently measurable and alertable |
| DLQ | `dlq_messages_total{tier,reason}`, DLQ topic size | Growth rate is an actionable alert — a spike usually means an operator-side outage |
| Idempotency | `idempotency_key_conflict_total` | Elevated rate suggests client-side retry storms — worth a conversation with that tenant, not necessarily a system problem |

## Grafana dashboards

Dashboards are organized around the questions an on-call engineer actually asks during an incident, not around service boundaries for their own sake:

- **Submission health** — API RED metrics, wallet deduction latency/rejection rate, idempotency conflict rate. Answers "can customers currently submit SMS."
- **Async pipeline health** — outbox backlog age/size, Kafka consumer lag per topic, broker RF/ISR status. Answers "is the pipeline keeping up, and if not, where's the backpressure."
- **Fairness & Express SLA** — scheduler active-tenant count, tenant wait-time percentiles, Express dispatch duration split from Normal. Answers "is any tenant being starved, and is Express meeting its SLA right now."
- **Delivery outcomes** — operator dispatch result breakdown by outcome and tier, DLQ growth rate and reason breakdown, refund rate. Answers "are messages actually getting delivered, and if not, why."
- **Capacity** — per-component resource utilization against the sizing targets in [scalability.md](scalability.md) Capacity planning, autoscaler activity. Answers "are we approaching a documented ceiling."

Each dashboard is scoped to answer its question in under 30 seconds of looking at it during an incident — a dashboard that requires cross-referencing five panels to answer "is Express meeting its SLA" has failed its purpose, however complete its data is.

## Alerting strategy

Alerting is SLO-driven, not metric-threshold-driven for its own sake — every page-level alert corresponds to a customer-visible or money-visible consequence, not an internal metric moving for reasons that don't matter yet:

| Condition | Severity | Why this threshold |
|---|---|---|
| Express p99 dispatch latency breaches SLA | **Page** | Direct violation of the one hard latency guarantee this system makes — [architecture.md](architecture.md) invariant 3 |
| Outbox oldest-unpublished-age above threshold | **Page** | Early warning before a Kafka outage becomes customer-visible ([decisions.md](decisions.md) ADR-004's entire payoff is early detectability here) |
| DLQ growth rate above baseline | **Page** | Usually means an operator-side outage affecting many tenants at once, not an isolated failure |
| Kafka consumer lag on `sms.normal` growing unbounded | **Warn** | Normal has no hard latency SLA, but unbounded growth signals a capacity or scheduler problem that will eventually become customer-visible |
| `scheduler_tenant_wait_seconds` p99 diverging sharply from p50 at similar tenant volume | **Warn** | Signals a DRR fairness regression — not urgent enough to page on its own, but worth same-day investigation before it becomes a starvation complaint |
| `wallet_insufficient_balance_total` spike for a single tenant | **Info / tenant-facing signal only** | Expected, legitimate behavior (a tenant genuinely ran out of balance) — not a system health signal, routed to tenant-success tooling rather than on-call |
| Refund rate spike | **Warn**, escalates to **Page** if sustained | A short spike can be a transient operator blip already covered by retry; a sustained spike means messages are failing permanently at an abnormal rate, which is both a customer trust and a revenue-integrity issue |

The distinction between "page" and "warn" tracks directly to whether the condition threatens a stated invariant ([architecture.md](architecture.md) Non-negotiable invariants) or an early-warning precursor to one — conditions that are merely "interesting" but don't threaten either are dashboard-only, deliberately not alerts, to keep on-call signal-to-noise high enough that a page is always worth waking up for.

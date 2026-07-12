# Architecture Decision Records

Each ADR captures a decision that was genuinely contested — where a competent team could reasonably have gone another way — along with why we didn't. ADRs that are narrow refinements of a decision covered elsewhere are cross-referenced rather than restated. See [architecture.md](architecture.md) for the system they describe, [assumptions.md](assumptions.md) for the constraints they were made under, and [scalability.md](scalability.md) for how each one bends under growth.

---

## ADR-001: PostgreSQL as the system of record

**Status:** Accepted

**Context:** The system needs one datastore holding money (`wallets`, `wallet_ledger`), high-volume operational state (`sms`, `batches`), and internal plumbing (`outbox_events`, `idempotency_keys`), with a hard requirement that wallet deduction and SMS acceptance commit atomically. Candidate stores span relational (PostgreSQL, MySQL), distributed SQL (CockroachDB, Spanner), and NoSQL (DynamoDB, MongoDB).

**Decision:** PostgreSQL is the sole system of record for every table in [database.md](database.md). Redis and Kafka are explicitly non-authoritative — both are reconstructible from Postgres/Kafka's own log if lost.

**Consequences:**

- **Pros:** Native multi-row ACID transactions make the wallet+SMS+outbox write one commit, with no saga or 2PC. `SELECT ... FOR UPDATE SKIP LOCKED` gives the outbox relay safe multi-instance competing-consumer semantics for free. Declarative range partitioning handles the `sms` table's billions-of-rows trajectory natively. Partial indexes keep hot, narrow scans (unpublished outbox rows, pending SMS) cheap regardless of total table size. Operationally mature: Patroni for HA/failover, PgBouncer for connection pooling, decades of tuning knowledge.
- **Cons:** Single-primary write throughput is a hard ceiling — Postgres does not horizontally scale writes on its own. At 10x+ growth this forces application-level tenant sharding (consistent-hash `tenant_id` → shard), which is a real migration, not a config flag. Vertical scaling and read replicas buy time but not indefinitely.
- **Trade-off:** We're trading "scales writes automatically" for "strongly consistent by construction, with a known, deliberate scale-out lever when we need it." At the stated scale (~1,160 msg/sec average, tens of thousands peak), a tuned single primary has 10-50x headroom before that lever needs pulling — see [scalability.md](scalability.md).

**Alternatives Considered:**

- **MySQL/InnoDB** — comparable OLTP profile, but weaker `SKIP LOCKED`-based competing-consumer ergonomics historically, and JSONB (used for `outbox_events.payload`) is a first-class Postgres type with indexing support MySQL's JSON column doesn't match. Not a wrong choice, just no reason to prefer it here.
- **CockroachDB / Spanner (distributed SQL)** — built-in horizontal write scaling sounds attractive, but every transaction pays consensus latency, which sits directly on the money-touching critical path. Neither Express latency SLA nor Normal-tier submission latency benefits from cross-region consensus we don't need at single-region v1 scale. Deferred: revisit only if multi-region active-active becomes a real requirement.
- **DynamoDB / MongoDB (NoSQL)** — no native multi-document/table ACID transaction spanning wallet balance, SMS insert, and outbox event with the isolation guarantees this system depends on; would force the exact distributed-transaction problem (Saga/2PC) that ADR-001's single-database choice was made specifically to avoid.

---

## ADR-002: Redis for idempotency locking and fair-scheduler state, kept non-authoritative

**Status:** Accepted

**Context:** Two hot paths need low-latency shared state across horizontally scaled instances: (1) the idempotency fast-path lock/cache on `POST /sms` and `POST /sms/batch`, and (2) the Fair Scheduler's per-tenant queues and active-tenant set for Deficit Round Robin (DRR). Both need sub-millisecond operations at high throughput, and neither needs durability beyond "survivable if lost, not silently wrong if lost."

**Decision:** Redis backs both. It is explicitly never the source of truth: idempotency's durable guarantee is a Postgres unique constraint (Redis is a latency optimization and lock), and scheduler state is reconstructible from Kafka's still-durable `sms.normal` log if Redis state is lost.

**Consequences:**

- **Pros:** Sub-millisecond `SET NX PX` for idempotency locking and native List/Sorted-Set primitives map directly onto DRR's per-tenant queue + active-tenant ledger, with no need to hand-roll those structures on top of a plain KV store. Horizontally shardable via Redis Cluster if scheduler ops/sec ever approaches a ceiling (not expected at target scale — see [scalability.md](scalability.md)).
- **Cons:** A second stateful system to operate, monitor, and fail over. Introduces a hot-path dependency for Normal-tier submission and dispatch that does not exist for Express (by design — see ADR-005).
- **Trade-off:** We accept a second stateful dependency in the Normal-tier hot path to get primitives (atomic per-tenant queue push/pop, cheap TTL-based locks) that would otherwise have to be built and tested from scratch on Postgres, at a latency cost Postgres row-locking isn't designed for at this call frequency.

**Alternatives Considered:**

- **Postgres advisory locks / unlogged tables for the same job** — rejected. Adds contention to the primary that's already carrying the wallet+SMS write path; using the OLTP primary for a purely ephemeral, high-frequency scheduling ledger conflates two very different durability requirements onto one system.
- **In-process memory per instance** — rejected outright. Idempotency and fairness are both *global* properties; per-pod memory can't enforce "this key is in flight" or "this tenant's turn is next" across a horizontally scaled fleet.
- **Memcached** — no native List/Sorted-Set types, would require reimplementing DRR's queue and active-tenant-set bookkeeping on raw key-value, for no offsetting benefit over Redis.

---

## ADR-003: Kafka over RabbitMQ as the message broker

**Status:** Accepted

**Context:** The async backbone must carry ~100M messages/day (≈1,160 avg/sec, bursting an order of magnitude higher under skewed tenant load) from the Outbox Relay to Express/Normal worker pools, support consumer-group-based horizontal scaling, and allow the Fair Scheduler and DLQ processor to replay or recover independently of consumer ack timing.

**Decision:** Kafka is the broker for `sms.express`, `sms.normal`, and their DLQ topics.

**Consequences:**

- **Pros:** Durable log retention (24h for live topics, 30d for DLQ) lets consumers replay without message-loss semantics tied to ack timing — the Outbox Relay and Fair Scheduler can crash and resume from where they left off. Partition-based consumer groups give natural, near-linear horizontal scaling for both worker pools. Throughput at this volume sits comfortably inside Kafka's designed operating envelope.
- **Cons:** Operationally heavier than RabbitMQ for small deployments — broker cluster, partition/replica planning, and rebalancing behavior all require real operational familiarity. Per-partition strict-FIFO ordering means priority cannot be expressed as a field on a shared topic (this is *why* Express and Normal are physically separate topics — see ADR-005 — not a limitation we work around).
- **Trade-off:** We take on Kafka's steeper operational surface in exchange for log-based replay and partition-scaling headroom that comfortably outlives the stated 100M/day target without a re-architecture.

**Alternatives Considered:**

- **RabbitMQ** — remains a legitimate substitute, not a wrong choice. Its exchange/queue topology maps directly onto this design: Express = dedicated exchange+queue, Normal = dedicated exchange+queue, DLQ = per-queue dead-letter-exchange (`x-dead-letter-exchange`, native support). RabbitMQ's per-message routing flexibility and lower operational floor are real advantages at smaller scale or with an existing RabbitMQ-operations team. It loses out here specifically because: classic-queue disk persistence and per-message routing overhead become a tuning burden at 100M/day sustained volume with an order-of-magnitude burst headroom, versus Kafka's sequential-log design which is built for exactly that throughput profile. If team expertise skews RabbitMQ, this is the one ADR most worth revisiting.
- **AWS SQS/SNS** — rejected for this exercise as it assumes a single-cloud commitment and loses the replay/log-tailing properties the Outbox Relay and DLQ tooling depend on; standard queues also don't offer the ordered-partition-per-tenant model used for scheduler ingestion.

---

## ADR-004: Transactional Outbox with a polling relay

**Status:** Accepted

**Context:** The write path must, in one atomic unit, deduct wallet balance, persist the SMS/Batch, and durably signal the async pipeline to eventually dispatch it. A naive dual-write (`COMMIT`, then separately `publish_to_kafka()`) has two failure windows: commit succeeds, process crashes before publish → customer charged, message never queued (silent loss — the worst failure mode for a paid product); or publish succeeds, commit fails/rolls back → a message dispatches that was never actually paid for.

**Decision:** Write an `outbox_events` row inside the same Postgres transaction as the wallet deduction and SMS/batch insert. A separate Outbox Relay polls `outbox_events WHERE published_at IS NULL` using `SELECT ... FOR UPDATE SKIP LOCKED`, publishes to Kafka, then marks the row published. See [database.md](database.md) for the table and [queue.md](queue.md) for the relay's role in the pipeline.

**Consequences:**

- **Pros:** Atomicity is achieved with zero cross-system coordination — it's a single-database ACID transaction, not a distributed one. Submission availability decouples from Kafka's momentary availability: during a Kafka outage, submissions keep succeeding and the outbox table simply accumulates a backlog that drains automatically on recovery (see [scalability.md](scalability.md) Failure Scenarios). Multiple relay instances run concurrently without double-publishing, for free, via `SKIP LOCKED`.
- **Cons:** Adds a component (the relay) and a table purely for internal plumbing. Polling has an inherent latency floor (sub-second, non-zero) between commit and publish that a WAL-tailing approach wouldn't have. A partial index keeps polls cheap, but it's still poll-based load on the primary, however small.
- **Trade-off:** We accept a small, bounded publish-latency tax and one extra component in exchange for never having to build or reason about a distributed transaction across Postgres and Kafka.

**Alternatives Considered:**

- **Synchronous dual-write** (`COMMIT` then `publish`) — rejected outright; this is the exact failure mode the pattern exists to close.
- **CDC-based relay (Debezium tailing the Postgres WAL)** — lower latency, no polling load on the primary, but adds a Kafka Connect cluster and connector to operate. Not justified at current scale where sub-second polling latency is invisible against Normal tier's multi-second-to-minute delivery SLA. Documented as the natural v2 upgrade path if poll latency or DB load from polling ever becomes measurable — see [scalability.md](scalability.md).
- **2PC across Postgres and Kafka** — rejected outright; Kafka doesn't support XA well, and 2PC's blocking-coordinator failure mode is a worse operational problem than the one it solves.

---

## ADR-005: Express and Normal as physically separate pipelines

**Status:** Accepted

**Context:** Express SMS must have a latency SLA that holds *independent of Normal-tier load*. The naive approach — a `priority` field on a shared topic, or a priority weight inside the Fair Scheduler — still leaves Express sharing some component (a partition, a consumer group, a Redis round-trip) with Normal traffic.

**Decision:** Express and Normal are physically separate end-to-end: separate Kafka topics (`sms.express`, `sms.normal`), separate consumer groups, separate worker pools, and in production separate Kubernetes Deployments with their own resource requests/limits and HPA policies (ideally a dedicated node pool). Full detail in [queue.md](queue.md) and [architecture.md](architecture.md) §7.

**Consequences:**

- **Pros:** Express latency is provably a function of Express volume and Express capacity alone — not an emergent, load-dependent property of a shared component. This is independently testable: you can load-test Normal to saturation and Express's latency profile shouldn't move.
- **Cons:** Operational duplication — two topics, two consumer groups, two deployments, two sets of dashboards and alerts to maintain. No capacity sharing between tiers: Express worker pool must be provisioned for its own peak even when Normal capacity sits idle.
- **Trade-off:** We pay for an independently provable SLA with duplicated infrastructure and worse average resource utilization (idle Express capacity can't absorb Normal overflow, or vice versa). Given that Express's entire value proposition *is* the guarantee, this trade is not close.

**Alternatives Considered:**

- **Priority field on a shared topic** — rejected. Kafka consumes a partition in strict offset order; a consumer can't "jump the queue" within a partition without re-reading and filtering, which defeats sequential-read performance and adds unbounded worst-case latency for Express during a Normal spike.
- **Priority weight inside the Fair Scheduler's DRR** — rejected. Still couples Express to a shared Redis dependency and shared consumer capacity, reintroducing exactly the contention risk physical isolation exists to eliminate.

---

## ADR-006: Centralized Deficit Round Robin for Normal-tier fairness

**Status:** Accepted

**Context:** The Normal tier must satisfy two requirements simultaneously: no tenant can starve another by sending more, and a single active tenant should get 100% of idle capacity. Rate limiting satisfies the first by rejecting/delaying the heavy tenant even when the system is otherwise idle — which directly violates the second requirement.

**Decision:** A Redis-backed Deficit Round Robin (DRR) scheduler sits between `sms.normal` consumption and Normal worker dispatch, giving every active tenant an equal quantum per round and resetting a tenant's deficit to zero when its queue empties (preventing banked-credit bursts). Full algorithm and Redis data model in [queue.md](queue.md) and [architecture.md](architecture.md) §8.

**Consequences:**

- **Pros:** The only evaluated approach satisfying both "no starvation" and "full capacity when idle" at once. Fairness is a global, verifiable property — independent of Kafka partition/consumer assignment — which makes it possible to reason about and test in isolation (`scheduler_tenant_wait_seconds{p50,p99}` should track volume-proportionally, not assignment-dependently).
- **Cons:** Introduces Redis as a hot-path dependency for Normal-tier dispatch (deliberately not for Express — ADR-005). The centralized scheduler is a single logical bottleneck, mitigated by Redis Cluster sharding if it's ever needed (not expected at target scale).
- **Trade-off:** Centralizing fairness state buys a global guarantee at the cost of a shared dependency; we accept that cost specifically because Normal tier carries no hard latency SLA, so a Redis blip degrades throughput rather than violating a promise.

**Alternatives Considered:**

- **FIFO (single shared queue)** — no starvation protection at all; a heavy tenant's burst sits ahead of everyone submitted after it. Simplest to build, directly violates the fairness requirement.
- **Static priority tiers** — solves Express-vs-Normal (which is why we do use tiering for that split) but does nothing for fairness *within* the Normal tier.
- **Rate limiting (token bucket per tenant)** — trivially prevents starvation by capping heavy tenants, but explicitly fails "full capacity when idle": a capped tenant is throttled even when it's the only sender and the system is empty. Correct as an abuse-protection layer (retained at the API tier for that purpose), wrong tool for fairness.
- **Decentralized per-partition DRR** (each consumer applies DRR only across tenants in its assigned partitions) — rejected because fairness becomes dependent on Kafka's partition-to-consumer assignment, which is rebalance-driven and coarse. Two heavy tenants landing on the same consumer's partitions compete; two on different consumers don't — an inconsistent, topology-dependent guarantee instead of a system-wide one.
- **Per-tenant Kafka topics** — rejected; tens of thousands of tenants means tens of thousands of topics, a well-known Kafka anti-pattern that blows out controller/metadata load.

---

## ADR-007: Batch as a first-class domain entity

**Status:** Accepted

**Context:** A batch send (identical message to many recipients) needs an atomic all-or-nothing acceptance decision covering potentially tens of thousands of recipients, without duplicating the message body per recipient or forcing a per-recipient outbox row that would blow out transaction size.

**Decision:** `batches` is a first-class table with its own primary key, `total_cost`, and status lifecycle (`ACCEPTED → IN_PROGRESS → COMPLETED | PARTIALLY_FAILED | FAILED`), referenced by child `sms.batch_id`. Child rows store `message_body = NULL` and inherit content from the parent. The submission transaction inserts exactly **one** `outbox_events` row (`event_type = BatchAccepted`) regardless of recipient count; a downstream fan-out consumer expands it into per-recipient dispatch messages. Full schema and rationale in [database.md](database.md).

**Consequences:**

- **Pros:** Transaction size is O(1) in recipient count — a 1-recipient send and a 50,000-recipient send cost the database the same write, avoiding a huge single-transaction WAL write and long lock duration. Message body is stored once. Batch has a natural row to own aggregate status for `GET /batches/{id}`.
- **Cons:** Any query needing batch context on a child row requires a join or a denormalized copy. Batch status is asynchronously derived — periodically recomputed from child `sms` statuses, not updated synchronously — so `GET /batches/{id}` is eventually consistent with respect to in-flight sends, which must be communicated to API consumers, not assumed away.
- **Trade-off:** One more entity and one more status machine to build and test, in exchange for atomicity and O(1) transaction size at arbitrary batch scale — treated as mandatory, not optional, once "all-or-nothing acceptance at tens of thousands of recipients" is a hard requirement.

**Alternatives Considered:**

- **No batch entity — insert N `sms` rows sharing a correlation `batch_id`, no owning row** — rejected. No natural place for `total_cost`/atomicity bookkeeping, no way to emit a single O(1) outbox event (forces either one outbox row per recipient, which was specifically ruled out for transaction-size reasons, or an awkward workaround), and no single row for a client to poll for aggregate status.
- **Fully client-side batching** (N separate `POST /sms` calls) — rejected; cannot satisfy all-or-nothing acceptance without the client implementing its own distributed rollback across N independent server calls.
- **Duplicate message body per child row** — rejected; storage blowup at 1M-recipient scale (a duplicated `TEXT` column across every child row).

---

## ADR-008: Atomic wallet updates via single-statement check-and-deduct

**Status:** Accepted

**Context:** Balance must never go negative, and two concurrent requests from the same tenant must never both succeed against a balance that only covers one of them (the classic check-then-act race). A naive `SELECT balance` followed by an application-level `if balance >= cost` followed by `UPDATE` is a textbook TOCTOU bug under concurrency.

**Decision:** Deduction is a single SQL statement: `UPDATE wallets SET balance = balance - $cost WHERE tenant_id = $1 AND balance >= $cost RETURNING balance`. Zero rows returned means insufficient balance — no separate read step exists to race against. Combined with an atomic charge-now/refund-on-permanent-failure model rather than reserve-then-capture. Full mechanism in [database.md](database.md) and [architecture.md](architecture.md) §9.

**Consequences:**

- **Pros:** The check and the write are the same statement, so Postgres's own row-level exclusive lock (implicitly taken by any `UPDATE`) provides the serialization — no lost updates, no double-spend, no `SELECT ... FOR UPDATE` needed. `READ COMMITTED` is sufficient; `SERIALIZABLE`'s retry-on-conflict overhead buys nothing here because correctness comes from the row lock, not snapshot isolation. Batch requests compute `total_cost` once and issue one `UPDATE`, so a 50,000-recipient batch is exactly as safe as a single SMS — no per-recipient deduction loop that could partially fail.
- **Cons:** All writes for one tenant serialize through one row. An extremely heavy tenant issuing very high *concurrent* (not sequential) single-SMS calls hits a real throughput ceiling on that row (Postgres sustains roughly 1K-5K row-level `UPDATE`/sec on a single hot row on typical hardware). Charging at submission time rather than on confirmed delivery means a small, bounded window exists where a customer's visible balance is lower than their "true" entitlement for messages that will eventually fail and be refunded.
- **Trade-off:** We trade perfectly real-time-accurate balance display (which reserve-then-capture would give) for a dramatically simpler mechanism — one atomic statement instead of tracked reservation state with its own expiry semantics. This is justified because SMS delivery outcomes resolve in seconds to low minutes (Normal tier's total retry budget caps around a minute), far short of the long-held-reservation timeframes (e.g., ride-hailing fare holds) where reserve-then-capture actually earns its complexity.

**Alternatives Considered:**

- **`SELECT balance` then application-level check then `UPDATE`** — rejected outright; textbook TOCTOU race under concurrent requests.
- **Reserve-then-capture** (hold balance at submission, capture on confirmed delivery, release on failure) — more accurate real-time balance display, but requires tracking reservation state and its expiry (what happens to a reservation for a message stuck mid-retry for hours?), and doubles wallet operations per message. Rejected as complexity the delivery-latency profile doesn't justify.
- **Sub-balance sharding within a tenant** (splitting one tenant's balance across N rows to relieve hot-row contention) — not rejected, deferred. Only relevant for a tenant whose *concurrent* single-SMS call rate genuinely exceeds ~1K-5K/sec, a pattern the batch endpoint exists to make unnecessary. Noted as a future lever in [scalability.md](scalability.md), not built for v1.

---

## ADR-009: Client-supplied idempotency keys, Postgres-backed

**Status:** Accepted

**Context:** Every mutating request over an unreliable network has an ambiguous timeout: did the server apply it or not? For a wallet-deducting endpoint, blindly retrying is unacceptable (double charge), and refusing to let clients retry is also unacceptable (no recovery from a network blip).

**Decision:** `POST /sms` and `POST /sms/batch` require a client-supplied `Idempotency-Key` header. A Redis `SET NX PX` provides a fast in-flight lock and cached-response path; the durable guarantee is a unique constraint on `(tenant_id, idempotency_key)` in Postgres, so correctness survives even if Redis state is lost. Full request/response semantics in [api.md](api.md).

**Consequences:**

- **Pros:** Client retries after a timeout are always safe to resend with the same key — never double-charged, never double-queued — without the client needing any additional coordination. Redis absence (failover, restart) degrades to "slightly slower duplicate detection via the DB constraint," not "duplicate detection stops working."
- **Cons:** Requires client discipline (generating and reusing a key correctly per logical request, not per HTTP attempt) — a client that generates a new key per retry defeats the mechanism entirely, and this can't be enforced server-side. Same-key-different-body reuse (a client bug) must be explicitly detected (`request_hash` mismatch → `422`) rather than silently accepted or silently ignored.
- **Trade-off:** Correctness under retry is placed partly in the client's hands (key discipline) in exchange for not needing the server to infer request identity from content alone, which would be unreliable (two genuinely distinct requests can have identical bodies).

**Alternatives Considered:**

- **Server-generated request IDs only** (no client-supplied key) — rejected; doesn't solve the actual problem, since a client that times out has no way to know the ID the server assigned to correlate a retry against.
- **Content-hash-based dedup** (dedupe purely by hashing the request body, no explicit key) — rejected; two legitimately distinct requests can have identical bodies (same recipient, same message, sent twice on purpose), which would be incorrectly collapsed into one.
- **Redis-only idempotency** (no Postgres constraint) — rejected; Redis failover/restart could lose the fast-path key, and a paid-product double-charge is not an acceptable failure mode for "our cache restarted."

---

## ADR-010: Structured JSON logging, zero PII, correlation via propagated request IDs

**Status:** Accepted

**Context:** At 100M messages/day flowing through half a dozen services, plaintext logs are unqueryable at aggregation scale, and reflection-based JSON encoding adds measurable per-request CPU at this call volume. Separately, SMS body and recipient number are customer data that must not sit in a log aggregation system with a much larger exposure surface than the database itself.

**Decision:** Structured JSON logging (`structlog`, JSON-rendered — lazy field binding avoids the same reflection-cost concern zap/zerolog solve in Go), one line per event, a fixed field set (`timestamp`, `level`, `service`, `tenant_id`, `request_id`, `sms_id`/`batch_id`, `event`, `latency_ms`, `queue`/`topic`, `attempt_count`), with a `request_id` generated at the API edge and propagated through Kafka message headers end-to-end so one query reconstructs an entire SMS's lifecycle. Message content and recipient numbers are never logged in full. Full detail in [observability.md](observability.md).

**Consequences:**

- **Pros:** Queryable via Loki/ELK at production log volume without regex gymnastics. Cross-service correlation is a single field lookup, not log-timestamp correlation guesswork. Safe by construction — there's no "someone forgot to redact this log line" failure mode because content is never captured at all, not redacted after the fact.
- **Cons:** Less human-skimmable in a raw terminal during local development (mitigated by piping through a pretty-printer). Field-naming discipline has to be enforced across services via a shared logging wrapper — an ungoverned per-service field vocabulary defeats the cross-service query benefit.
- **Trade-off:** A small local-dev ergonomics cost for a large win in aggregate queryability, correlation, and default-safe handling of customer data.

**Alternatives Considered:**

- **Plaintext/human-readable logs** — rejected; not machine-parseable at this volume, no reliable structured correlation across services.
- **Logging full message bodies for easier debugging** — rejected as a hard rule, not a style preference. SMS content and recipient numbers are customer data; centralized log aggregation is a materially larger exposure surface than the database, which already stores this data under normal access controls.
- **OpenTelemetry logs API in place of structlog** — a legitimate direction, deferred rather than rejected. OTel is adopted for distributed tracing specifically (see [observability.md](observability.md)) without forcing every log call through the heavier OTel logging pipeline at this stage.

---

## ADR-011: Cursor (keyset) pagination for report queries

**Status:** Accepted

**Context:** `GET /reports/sms` queries a table headed toward tens of billions of rows within a year. Offset-based pagination (`OFFSET`/`LIMIT`) requires Postgres to scan and discard every skipped row on each page request — cost grows linearly with page depth — and is unstable under concurrent inserts, since rows can shift between pages mid-scroll, silently skipping or duplicating results.

**Decision:** Report pagination uses an opaque cursor encoding the last-seen `(created_at, id)` pair, walked via the existing `idx_sms_tenant_time (tenant_id, created_at)` index. No `OFFSET` is ever issued.

**Consequences:**

- **Pros:** Constant per-page cost regardless of how deep into the result set the client is paging — page 2 and page 200,000 cost the same. Stable under concurrent writes, because the cursor anchors to a specific row's key rather than a positional count that shifts as rows are inserted. Uses an index that already exists for other query patterns, no new index needed.
- **Cons:** No random access to "page N" — a client can only walk forward from a cursor, not jump to an arbitrary page. No cheap "total result count" to display alongside pagination controls.
- **Trade-off:** We give up page-jump UX, which is irrelevant for a machine-consumed reporting API (there is no human clicking "page 47"), in exchange for pagination performance that does not degrade as the table grows into the billions of rows — the only property that actually matters for this endpoint's real usage pattern.

**Alternatives Considered:**

- **OFFSET/LIMIT** — the default in most frameworks, rejected specifically because of this table's projected size; fine for small tables, actively harmful at billions of rows.
- **Full result materialization with server-side "page tokens" stored server-side** — rejected as unnecessary added state (a server-side session/cursor store) when the cursor can be entirely self-describing and stateless, encoded from data the client already has no legitimate reason to tamper with beneficially.

---

## ADR-012: Dead Letter Queue with automatic refund on landing

**Status:** Accepted

**Context:** After retry exhaustion (§ [queue.md](queue.md) Retry Strategy), a message must not simply disappear. The customer was charged for it (ADR-008's optimistic-charge model), so silently dropping a permanently failed message would mean keeping payment for undelivered service — not a defensible behavior for a paid product.

**Decision:** On retry exhaustion, the worker publishes the original message plus failure metadata (`error`, `attempt_count`, `first_attempted_at`, `last_attempted_at`) to `sms.dlq.{express|normal}` and sets `sms.status = FAILED_DEAD_LETTER`. A DLQ processor updates the customer-visible status, triggers an automatic, idempotency-guarded refund (same atomic-ledger mechanism as any wallet credit), and retains the message 30 days for operator-side investigation.

**Consequences:**

- **Pros:** No paid-for message is ever silently lost — every terminal failure is both visible to the customer (status transitions to a real terminal state, not left hanging) and financially corrected (automatic refund). 30-day retention supports post-incident investigation and manual replay tooling without needing that tooling built for v1.
- **Cons:** DLQ processing is one more consumer with its own failure modes to monitor — a stuck or crashed DLQ processor means refunds don't fire even though the customer-visible failure already happened, a gap that needs its own alerting (`dlq_messages_total` growth rate, per [observability.md](observability.md)).
- **Trade-off:** We accept the operational surface of one more consumer and topic pair in exchange for the alternative being unacceptable — a paid product cannot let a message vanish without either delivering it or making the customer financially whole.

**Alternatives Considered:**

- **Drop silently after retry exhaustion** — rejected outright; violates the basic contract of a paid, at-least-once-attempted delivery service.
- **Infinite retry (no DLQ, no terminal failure state)** — rejected; a permanently invalid destination (bad number, blocked recipient) would retry forever, burning worker capacity on a message that mathematically cannot succeed, and never resolving the customer's paid-for-nothing state either.
- **Manual (non-automatic) refund, DLQ purely as an ops queue** — rejected; makes financial correctness dependent on a human noticing and acting, which doesn't scale past a handful of daily failures and introduces arbitrary refund latency for the customer.

---

## ADR-013: Wallet and SMS share one service and one transaction, not separate microservices

**Status:** Accepted

**Context:** The requirement that wallet deduction and SMS creation happen atomically is a hard constraint, not a suggestion. If "Wallet Service" and "SMS Service" were independently deployed microservices with independent database connections, satisfying that requirement would force a distributed transaction (2PC) or a Saga with compensating actions on a path that must be fast and strongly consistent, since it's money.

**Decision:** Wallet and SMS are separate *domain modules* — distinct packages, distinct tables, independently testable — but execute inside **one Python/FastAPI service** using **one Postgres transaction** for the write path ([architecture.md](architecture.md) §1). They remain separate services conceptually for reporting/read paths, and could be split into network services later behind a shared database and Unit-of-Work boundary if org boundaries demanded it — not needed for v1.

**Consequences:**

- **Pros:** True atomicity via native ACID, no distributed-transaction latency or complexity, no Saga compensating-action state machine to build and test. The failure mode is the simplest possible one: either the whole transaction commits or none of it does.
- **Cons:** Tighter coupling than "true" microservices — a schema change to `sms` and a schema change to `wallets` are not independently deployable behind separate service APIs the way they would be if split. Wallet and SMS write throughput cannot scale independently; they scale together, as one service.
- **Trade-off:** We give up independent service-level scaling and deployability for Wallet vs. SMS in exchange for correctness by construction. This is an easy trade here specifically because Wallet and SMS have identical traffic patterns and identical scaling needs in this domain — there is no realistic scenario where one needs to scale materially differently from the other, so the "independent scaling" benefit a split would offer is largely theoretical for this system.

**Alternatives Considered:**

- **Saga with compensating transaction** (reserve balance, create SMS, confirm reservation; on failure, compensate by releasing the reservation) — rejected. Adds a full state machine and a window where balance is provisionally held but not committed, for a benefit (independent service scaling) that doesn't materialize given Wallet and SMS's identical traffic profile.
- **2PC across two databases** — rejected outright. Most modern datastores, Kafka included, don't support XA well, and 2PC's blocking-coordinator failure mode is a worse operational problem than the one it solves.

---

## Trade-off summary

| Decision | Chosen | Runner-up | Why chosen wins here |
|---|---|---|---|
| System of record | PostgreSQL | Distributed SQL (CockroachDB/Spanner) | Consensus latency on every txn isn't worth paying for horizontal write scaling we don't need until 10x+ growth |
| Fast ephemeral state | Redis, non-authoritative | Postgres advisory locks | Keeps ephemeral, high-frequency scheduling/locking load off the OLTP primary |
| Message broker | Kafka | RabbitMQ | Log retention + partition-based scaling fit this volume/replay profile; RabbitMQ remains valid with different team expertise |
| Async durability | Outbox + polling relay | CDC (Debezium) | Guarantees atomicity without distributed tx; CDC is a deferred v2 latency optimization, not a v1 requirement |
| Express isolation | Full physical separation | Priority field/weight | Only way to make the SLA provable, not probabilistic |
| Normal fairness | Centralized DRR (Redis) | Decentralized per-partition DRR | Global fairness guarantee vs. partition-assignment-dependent guarantee |
| Batch modeling | First-class entity | Correlation-only child rows | O(1) transaction size and single-row aggregate status need an owning row |
| Wallet deduction | Single atomic UPDATE | Reserve-then-capture | Delivery resolves in seconds; reservation-state complexity buys nothing at this timescale |
| Duplicate submission | Client idempotency key, Postgres-backed | Content-hash dedup | Two legitimately identical requests must not be collapsed into one |
| Logging | Structured JSON, zero PII | Plaintext | Queryable at volume; content-in-logs is a hard no for customer data |
| Report pagination | Cursor/keyset | OFFSET/LIMIT | Constant-cost paging on a billions-of-rows table |
| Terminal failure handling | DLQ + automatic refund | Silent drop / infinite retry | Paid product cannot lose money and message together with no resolution |

See [assumptions.md](assumptions.md) for the constraints these decisions were made under, and [scalability.md](scalability.md) for the future-scaling levers each one leaves on the table.

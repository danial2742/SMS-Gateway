# Security

Authentication and tenant identity resolution are explicitly out of scope for this design ([assumptions.md](assumptions.md) #1) — every service trusts an already-resolved `tenant_id`. That doesn't mean security is out of scope; it means this document covers everything *downstream* of "who is calling," which is where most of a production system's actual security surface lives regardless of how well-built the auth layer is.

## Threat model, briefly

This system's highest-value targets, in order: the wallet (it's money), tenant isolation (one tenant must never read or affect another's data), and the operator relationship (abuse here costs real money per message and can get the platform's operator account suspended). Every control below maps to one of these three.

## Authorization boundaries

Every resource read (`GET /sms/{id}`, `GET /batches/{id}`, report queries) must scope its query by the resolved `tenant_id`, never by resource ID alone — a UUID primary key is not a secret, and treating it as an implicit access-control boundary (an IDOR — insecure direct object reference) would let any tenant enumerate another tenant's message history by guessing or incrementing IDs. Every repository method that fetches an `sms`, `batch`, or wallet row takes `tenant_id` as a mandatory query parameter, not an optional filter — there is no code path that fetches by ID alone. This is enforced at the data-access layer specifically so a new endpoint can't accidentally omit the scope by forgetting a `WHERE` clause at the handler level.

## Rate limiting for abuse prevention

A per-tenant token bucket (default 500 req/sec, configurable per tenant) applies at the API tier. This is explicitly an **abuse/spike protection** mechanism, not a fairness mechanism — fairness among legitimate heavy senders is the Fair Scheduler's job ([decisions.md](decisions.md) ADR-006), and conflating the two was specifically rejected (ADR-006 Alternatives Considered). The rate limit protects the API tier and Postgres primary from:

- A bug in a customer's own retry loop hammering the endpoint.
- A compromised API credential being used for high-volume abuse before it's revoked.
- Any single tenant's traffic pattern degrading the platform for every other tenant sharing the API tier and database primary.

The ceiling is set generously enough that no legitimately well-behaved heavy customer hits it under normal operation — sustained legitimate high volume is expected to flow through the batch endpoint (one request, many recipients), not thousands of individual `POST /sms` calls per second, so a tenant tripping this limit is itself a useful abuse signal. A tenant hitting the ceiling receives `429 Too Many Requests` (see [api.md](api.md)).

## Input validation

Every field crossing a trust boundary is validated before it can influence a database write or an outbound operator call, specifically:

- `recipient` must parse as valid E.164 — rejected before the wallet transaction opens, so a malformed request never touches the balance (no charge for a request that can't succeed).
- `message` length is checked against the single-page limit for its resolved encoding (GSM-7 vs. UCS-2) — see [assumptions.md](assumptions.md) #5.
- `recipients` array length is capped (`BATCH_TOO_LARGE`) to bound both request payload size and transaction size — an unbounded array is a resource-exhaustion vector on both the API process (payload parsing) and Postgres (transaction/WAL size), not just a UX concern.
- Report query date ranges (`from`/`to`) are capped in width — an unbounded range on a billions-of-rows table is a self-inflicted denial-of-service vector even from a legitimate, non-malicious client, let alone a hostile one probing for expensive queries.
- All validation happens **before** any side effect (wallet transaction, outbox write) — validation failure is always a clean no-op, never a partial write to roll back.

Validation is allowlist-shaped where practical (E.164 format, enum values for `priority`/`status`) rather than denylist-shaped (blocking known-bad patterns) — an allowlist fails closed by construction; a denylist only blocks what someone thought to enumerate.

## Idempotency as a security property, not just a correctness one

[decisions.md](decisions.md) ADR-009 covers idempotency's correctness role (no double charge under retry). It's worth calling out separately as a security-relevant control: without idempotency keys, a scripted or automated client — malicious or merely buggy — resending a request under network ambiguity has no bound on how many times a charge could replay. The `(tenant_id, idempotency_key)` unique constraint is a hard backstop at the database level, not just an optimization, which matters specifically because it holds even if a client is actively hostile rather than merely retrying in good faith.

## SQL injection

All database access goes through parameterized queries / prepared statements exclusively — every value in every query shown in [database.md](database.md) is a bound parameter (`$1`, `$2`, ...), never string-interpolated into SQL text. This is a hard rule enforced by code review and, ideally, lint tooling that flags any string-concatenated SQL construction, not a per-developer discipline assumption. The `outbox_events.payload` JSONB column is written via the driver's JSON marshaling into a bound parameter, not via manual JSON-string-building into a query — the same discipline applies to structured data as to scalar values.

## Transport security

- **TLS everywhere in transit:** client → API (TLS terminated at the load balancer, re-encrypted or mTLS to pods depending on cluster policy), API/workers → Postgres (`sslmode=require` at minimum, `verify-full` in production), API/scheduler → Redis (TLS where the Redis deployment supports it — required in production, not just recommended), all services → Kafka (TLS listeners, not the plaintext listener shown in the [deployment.md](deployment.md) Docker Compose example, which is local-dev-only).
- **No plaintext credential transmission** — connection strings carrying credentials are never logged (consistent with [observability.md](observability.md) Logging's no-PII rule, which extends to no-secrets), and are sourced from the secrets mechanism below, never hardcoded or passed as plain CLI arguments visible in process listings.

## Secrets management

Database credentials, Redis auth, and the operator API credential are mounted from the cluster secret store (Kubernetes Secrets backed by an external secrets manager — Vault or a cloud KMS-backed store) as files or via a secrets-CSI-driver volume, never baked into an image or committed to version control. The `.env`-file pattern shown in [deployment.md](deployment.md)'s Docker Compose example is explicitly local-dev-only and must never be the pattern used in a shared or production environment.

- **Rotation:** credentials are rotated on a defined schedule and immediately on suspected compromise; because every service reads secrets from mounted files/volumes rather than baking them into process environment at container-build time, rotation doesn't require an image rebuild — only a secret-store update and, depending on the mechanism, a pod restart or live reload.
- **Least privilege:** the Postgres role used by worker services (which only need to update `sms.status` and insert refund transactions) is scoped narrower than the role used by the API service (which needs the full wallet/sms/outbox write path) — a compromised worker credential should not be sufficient to arbitrarily modify wallet balances outside the refund path it's actually meant to perform.
- **Operator API credential** is treated with the same rigor as database credentials, not as a lower-tier secret — a leaked operator credential allows sending SMS (and incurring cost) directly against the platform's operator account, bypassing this system's wallet/balance controls entirely, which makes it arguably the single most damaging credential to leak in this whole system.

## Audit logging

Distinct from the operational logging in [observability.md](observability.md), which explicitly excludes PII and message content: `wallet_ledger` **is** the audit trail for every financial event (deduction, topup, refund), permanent, immutable (never `UPDATE`d or `DELETE`d), and queryable via SQL for dispute resolution, reconciliation, or a regulatory ask — see [database.md](database.md) Atomic wallet update. This is a deliberate split: operational logs are for "what is the system doing right now" (short retention, safe to aggregate broadly, must never contain customer content); the ledger is for "what happened to this tenant's money, provably and permanently" (long/permanent retention, narrowly scoped to financial events, access-controlled separately from general log aggregation).

If a future requirement needs a broader audit trail (e.g., who viewed a tenant's report data, not just what happened to their wallet), that's a distinct access-log mechanism layered on top of the auth boundary this document assumes is resolved upstream — not something the current ledger design attempts to cover, and not currently built (see [assumptions.md](assumptions.md) Deferred features).

## What this document does not cover

Authentication, authorization *policy* (who is allowed to act as which tenant), and network perimeter design (VPC/firewall topology, WAF rules) are all upstream of or orthogonal to this system's own boundary, per [assumptions.md](assumptions.md) #1 — worth flagging explicitly so "not mentioned here" reads as "out of this system's scope," not "overlooked."

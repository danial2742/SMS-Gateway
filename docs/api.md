# REST API Reference

Base path: `/api/v1`. Auth/tenant resolution is out of scope ([assumptions.md](assumptions.md) #1) — every endpoint below assumes an authenticated tenant context is already resolved upstream and available as `tenant_id`.

## Conventions

- All monetary/cost fields are integers (minor units / credits), never floats — see [database.md](database.md) Design principles.
- `POST` endpoints that mutate the wallet **require** an `Idempotency-Key` header (UUID recommended). Requests without it are rejected with `400`. Full semantics in [decisions.md](decisions.md) ADR-009.
- All responses are JSON. Errors follow a consistent envelope:

```json
{
  "error": {
    "code": "INSUFFICIENT_BALANCE",
    "message": "Wallet balance is insufficient to accept this request.",
    "request_id": "9c3f2b7e-..."
  }
}
```

- `request_id` in every error body matches the `request_id` field emitted in structured logs for that request — see [observability.md](observability.md) Correlation IDs. Include it when reporting an issue; it's the fastest path to the exact log lines for that call.

---

## Wallet

### `GET /api/v1/wallet`

**Purpose:** Retrieve the current balance for the authenticated tenant. Used by clients to check spendable balance before submitting large batches, and by internal dashboards.

**Request:** No body. No query parameters.

**Response `200 OK`**
```json
{ "tenant_id": "uuid", "balance": 74999, "currency": "credits", "updated_at": "2026-07-09T10:15:00Z" }
```

**Validation:** None beyond tenant resolution — read-only endpoint.

**Status codes:** `200` success. `404` if the tenant has no wallet row (should not occur for a resolved tenant; indicates a provisioning bug, not a client error).

**Errors:** `WALLET_NOT_FOUND` (404) — see status codes above.

**Example:**
```bash
curl -s https://api.example.com/api/v1/wallet \
  -H "Authorization: Bearer <token>"
```

---

### `POST /api/v1/wallet/charge`

**Purpose:** Top up a tenant's SMS balance following a successful external payment. Called by an internal billing/payment integration, not directly by an end customer's SMS-sending client.

**Request**
```json
{ "amount": 100000, "method_ref": "psp-transaction-id" }
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `amount` | integer | yes | Credits to add; must be `> 0` |
| `method_ref` | string | no | External payment/PSP reference, stored for reconciliation |

**Response `201 Created`**
```json
{ "topup_id": "uuid", "balance_after": 100000 }
```

**Validation:** `amount` must be a positive integer. Non-integer or non-positive values are rejected before any transaction begins — see [security.md](security.md) Input validation.

**Status codes:** `201` created. `400` invalid body. `422` `INVALID_AMOUNT`.

**Errors:**
| Status | Code | Cause |
|---|---|---|
| 400 | `INVALID_JSON` | malformed request body |
| 422 | `INVALID_AMOUNT` | `amount <= 0` or non-integer |

**Implementation note:** single `UPDATE wallets SET balance = balance + amount` + `wallet_ledger` insert + `topups` insert, one transaction ([database.md](database.md) Transactions). No outbox event is emitted — topups don't need async fan-out, though emitting one for downstream billing analytics is a reasonable future addition.

**Example:**
```bash
curl -s -X POST https://api.example.com/api/v1/wallet/charge \
  -H "Authorization: Bearer <token>" \
  -H "Content-Type: application/json" \
  -d '{"amount": 100000, "method_ref": "psp-txn-8841"}'
```

---

## Single SMS

### `POST /api/v1/sms`

**Purpose:** Submit a single SMS for delivery. Atomically deducts balance, persists the message, and durably queues it for async dispatch — see [sequence-diagrams.md](sequence-diagrams.md) Single SMS for the full request lifecycle.

**Headers:** `Idempotency-Key: <uuid>` (required)

**Request**
```json
{
  "recipient": "+15551234567",
  "message": "Your code is 4821",
  "priority": "NORMAL"
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `recipient` | string | yes | E.164 format |
| `message` | string | yes | Single-page content; see [assumptions.md](assumptions.md) #5 |
| `priority` | string | no | `NORMAL` \| `EXPRESS`, defaults to `NORMAL` |

**Response `202 Accepted`**
```json
{ "sms_id": "uuid", "status": "QUEUED", "cost": 1, "balance_after": 99999 }
```

`202`, not `200` or `201` — acceptance and delivery are different guarantees. The response confirms the message was charged and durably persisted for dispatch, not that it reached the recipient. Poll `GET /api/v1/sms/{id}` for delivery status.

**Validation:**
- `recipient` must parse as valid E.164; malformed numbers rejected before the wallet transaction opens (no charge for a request that can't possibly succeed).
- `message` must fit within the single-page character limit at the target encoding (GSM-7 vs. UCS-2) — see [assumptions.md](assumptions.md) #5.
- `priority`, if present, must be one of the two allowed values.
- `Idempotency-Key` header presence is checked before any other validation.

**Status codes:** `202` accepted. `400` missing/malformed header or body. `402` insufficient balance. `409` idempotency key in flight. `422` semantic validation failure.

**Errors:**
| Status | Code | Cause |
|---|---|---|
| 400 | `MISSING_IDEMPOTENCY_KEY` | header absent |
| 402 | `INSUFFICIENT_BALANCE` | atomic deduction returned 0 rows — [decisions.md](decisions.md) ADR-008 |
| 409 | `IDEMPOTENCY_KEY_IN_FLIGHT` | same key, request still processing |
| 422 | `IDEMPOTENCY_KEY_REUSED` | same key, different request body (`request_hash` mismatch) |
| 422 | `INVALID_RECIPIENT` | not a valid E.164 number |
| 422 | `MESSAGE_TOO_LONG` | exceeds single-page limit for the resolved encoding |

**Example:**
```bash
curl -s -X POST https://api.example.com/api/v1/sms \
  -H "Authorization: Bearer <token>" \
  -H "Idempotency-Key: 6f2c9e10-2b3a-4e1a-9c3f-2b7e9c3f2b7e" \
  -H "Content-Type: application/json" \
  -d '{"recipient": "+15551234567", "message": "Your code is 4821", "priority": "EXPRESS"}'
```

---

### `GET /api/v1/sms/{id}`

**Purpose:** Poll the current status of a single message — either a standalone send or one child of a batch.

**Request:** No body. `id` is the `sms_id` path parameter.

**Response `200 OK`**
```json
{
  "sms_id": "uuid",
  "batch_id": null,
  "recipient": "+15551234567",
  "status": "SENT_TO_OPERATOR",
  "priority": "NORMAL",
  "attempt_count": 1,
  "created_at": "2026-07-09T10:12:00Z",
  "sent_at": "2026-07-09T10:12:03Z"
}
```

`status` ∈ `QUEUED`, `SENT_TO_OPERATOR`, `DELIVERED`, `FAILED`, `FAILED_DEAD_LETTER` — see [database.md](database.md) schema. `DELIVERED` is not populated in v1 absent a delivery-receipt integration ([assumptions.md](assumptions.md) #6); `SENT_TO_OPERATOR` is the practical terminal-success state.

**Validation:** `id` must be a well-formed UUID.

**Status codes:** `200` found. `404` no such message (or belongs to a different tenant — see [security.md](security.md) Authorization boundaries).

**Errors:** `SMS_NOT_FOUND` (404).

**Example:**
```bash
curl -s https://api.example.com/api/v1/sms/6f2c9e10-2b3a-4e1a-9c3f-2b7e9c3f2b7e \
  -H "Authorization: Bearer <token>"
```

---

## Batch SMS

### `POST /api/v1/sms/batch`

**Purpose:** Send identical content to many recipients as one atomic operation. See [decisions.md](decisions.md) ADR-007 for why batches are a first-class entity, and [sequence-diagrams.md](sequence-diagrams.md) Batch SMS for the full flow including fan-out.

**Headers:** `Idempotency-Key: <uuid>` (required)

**Request**
```json
{
  "recipients": ["+15551234567", "+15557654321", "..."],
  "message": "50% off today only",
  "priority": "NORMAL"
}
```

| Field | Type | Required | Notes |
|---|---|---|---|
| `recipients` | array\<string\> | yes | Non-empty, E.164 per entry, bounded by a configured max ([assumptions.md](assumptions.md) #10) |
| `message` | string | yes | Stored once, not per recipient — [database.md](database.md) |
| `priority` | string | no | `NORMAL` \| `EXPRESS`, defaults to `NORMAL` |

**Response `202 Accepted`**
```json
{ "batch_id": "uuid", "recipient_count": 25000, "total_cost": 25000, "status": "ACCEPTED", "balance_after": 74999 }
```

**Validation:**
- `recipients` must be non-empty and at or under the configured maximum length.
- Every entry validated as E.164; **any** invalid entry rejects the **entire** batch — no partial acceptance, consistent with the atomicity invariant.
- `total_cost = recipient_count * unit_cost` is computed and checked against balance in one atomic statement — [database.md](database.md) Concurrency.

**Status codes:** `202` accepted. `402` insufficient balance for the whole batch. `422` validation failure (empty list, oversized list, or any invalid recipient).

**Errors:**
| Status | Code | Cause |
|---|---|---|
| 402 | `INSUFFICIENT_BALANCE` | insufficient for the **entire** batch — nothing is partially accepted |
| 422 | `EMPTY_RECIPIENT_LIST` | `recipients` is empty |
| 422 | `BATCH_TOO_LARGE` | above configured max recipients per request — split client-side into multiple batches |
| 422 | `INVALID_RECIPIENT` | response includes the offending index; whole batch still rejected |

**Example:**
```bash
curl -s -X POST https://api.example.com/api/v1/sms/batch \
  -H "Authorization: Bearer <token>" \
  -H "Idempotency-Key: 8a1e4c22-...-batch" \
  -H "Content-Type: application/json" \
  -d '{"recipients": ["+15551234567", "+15557654321"], "message": "50% off today only"}'
```

---

### `GET /api/v1/batches/{id}`

**Purpose:** Poll aggregate progress of a batch send.

**Request:** No body. `id` is the `batch_id` path parameter.

**Response `200 OK`**
```json
{
  "batch_id": "uuid",
  "status": "IN_PROGRESS",
  "recipient_count": 25000,
  "sent_count": 18342,
  "failed_count": 12,
  "created_at": "2026-07-09T10:00:00Z"
}
```

`sent_count`/`failed_count` are derived from a periodic aggregate over child `sms` rows, **not** computed synchronously per request — this endpoint is eventually consistent with respect to in-flight sends ([decisions.md](decisions.md) ADR-007). `status` reaching `COMPLETED`/`PARTIALLY_FAILED`/`FAILED` reflects that the aggregation job has observed all children reach a terminal state, which lags real-time dispatch by the aggregation interval.

**Validation:** `id` must be a well-formed UUID.

**Status codes:** `200` found. `404` no such batch (or belongs to a different tenant).

**Errors:** `BATCH_NOT_FOUND` (404).

**Example:**
```bash
curl -s https://api.example.com/api/v1/batches/8a1e4c22-... \
  -H "Authorization: Bearer <token>"
```

---

## Reports

### `GET /api/v1/reports/sms`

**Purpose:** Paginated, filterable query over historical SMS records for a tenant. Served from a read replica / reporting store — never the OLTP primary ([database.md](database.md) Indexing & query patterns, [scalability.md](scalability.md) Read replicas).

**Request — query parameters:**

| Param | Type | Required | Notes |
|---|---|---|---|
| `from`, `to` | ISO 8601 timestamp | yes | Bounded range — unbounded scans are rejected (see [security.md](security.md) Input validation) |
| `status` | string | no | Filter by `sms.status` |
| `priority` | string | no | Filter by `NORMAL`/`EXPRESS` |
| `batch_id` | UUID | no | Restrict to one batch's children |
| `cursor` | opaque string | no | From a prior response's `next_cursor`; omit for the first page |
| `limit` | integer | no | Max 200, default 50 |

**Response `200 OK`**
```json
{
  "items": [ { "sms_id": "...", "recipient": "...", "status": "...", "cost": 1, "created_at": "..." } ],
  "next_cursor": "eyJjcmVhdGVkX2F0IjoiMjAyNi0wNy0wOVQxMDoxMjowMFoi...=="
}
```

An empty `next_cursor` (or its absence) signals the last page.

**Validation:** `from`/`to` required and `from < to`; range width may be capped (e.g. 90 days) to bound query cost even with cursor pagination's constant per-page cost — the cap protects against a single query touching an unreasonable number of partitions, not pagination depth. `limit` clamped to `[1, 200]`; values above 200 are clamped, not rejected, since a client requesting more is a benign mistake, not a malicious one. Cursor-based, not offset-based — see [decisions.md](decisions.md) ADR-011.

**Status codes:** `200` success (including zero results — an empty `items` array with no `next_cursor` is a valid, successful response, not a `404`). `400` missing/invalid `from`/`to`. `422` `from >= to` or malformed `cursor`.

**Errors:**
| Status | Code | Cause |
|---|---|---|
| 400 | `MISSING_DATE_RANGE` | `from` or `to` absent |
| 422 | `INVALID_DATE_RANGE` | `from >= to`, or range exceeds the configured maximum width |
| 422 | `INVALID_CURSOR` | `cursor` does not decode to a valid keyset position (tampered or stale beyond retention) |

**Example:**
```bash
curl -s "https://api.example.com/api/v1/reports/sms?from=2026-07-01T00:00:00Z&to=2026-07-09T00:00:00Z&status=FAILED&limit=100" \
  -H "Authorization: Bearer <token>"
```

---

## Health

These two endpoints are unauthenticated and unversioned (not under `/api/v1`) — they're infrastructure probes, not part of the tenant-facing API surface. Full rationale in [deployment.md](deployment.md) Health checks.

### `GET /healthz`

**Purpose:** Liveness probe — "is this process's own event loop responsive." Used by Kubernetes to decide whether to restart the pod.

**Request:** None.

**Response `200 OK`**
```json
{ "status": "ok" }
```

**Validation:** None.

**Status codes:** `200` alive. No dependency checks are performed — a `200` here says nothing about Postgres/Redis/Kafka reachability by design ([deployment.md](deployment.md) Liveness).

**Errors:** None modeled; absence of a `200` (timeout, connection refused, non-2xx) is itself the failure signal Kubernetes acts on.

---

### `GET /readyz`

**Purpose:** Readiness probe — "can this pod currently serve traffic / do useful work." Gates load-balancer admission (API Service) and rolling-deploy sequencing (all services).

**Request:** None.

**Response `200 OK`**
```json
{ "status": "ready", "checks": { "postgres": "ok", "redis": "ok", "kafka": "ok" } }
```

**Response `503 Service Unavailable`**
```json
{ "status": "not_ready", "checks": { "postgres": "ok", "redis": "timeout", "kafka": "ok" } }
```

**Validation:** None — this endpoint performs checks, it doesn't accept input.

**Status codes:** `200` all required dependencies reachable for this service (API checks Postgres + Redis + Kafka metadata; Normal workers check Postgres + Redis + Kafka; Express workers and the relay check only what they actually use — see [deployment.md](deployment.md) Health checks table). `503` if any required dependency check fails.

**Errors:** None modeled beyond the `503` body itself identifying which check failed, for fast operator triage.

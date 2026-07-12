# How This App Works (Plain Explanation)

This doc skips the deep design rationale (that's in [architecture.md](architecture.md), [decisions.md](decisions.md), [queue.md](queue.md)) and just walks through **what happens, in order**, when someone sends an SMS. One example, followed start to finish.

## The pieces, in one sentence each

| Piece | What it does |
|---|---|
| **API** | A web server. Receives "send this SMS" requests, checks the tenant has enough balance, charges them, saves the message to the database. |
| **Postgres (database)** | Stores tenants, wallets, messages, and a to-do list of things that still need to reach Kafka ("outbox"). |
| **Outbox Relay** | A small loop that reads that to-do list and publishes each item to Kafka. |
| **Kafka** | A durable queue. Once a message is in here, it won't be lost even if a worker crashes. |
| **Fair Scheduler** | Only for Normal-priority messages. Makes sure one huge sender can't starve everyone else. |
| **Express Worker / Normal Worker** | Pulls a message off the queue, calls the phone carrier ("Operator API"), records success or failure. |
| **Operator API** | The actual SMS carrier (outside this system). We never built this — it's mocked in dev. |
| **Rate Limiter (API)** | A per-tenant token bucket at the API layer — rejects requests with 429 if a tenant sends faster than their allowed rate. Abuse/spike protection, separate from the Fair Scheduler's fairness job. |
| **Redis** | Backs the rate limiter's token bucket, the idempotency-key locks, the Fair Scheduler's per-tenant queues, and a per-message dispatch lock that stops double-sending. |

## The example: one SMS, start to finish

Business "Acme" has 100,000 credits. Acme calls:

```
POST /api/v1/sms
X-Tenant-ID: acme-tenant-id
Idempotency-Key: acme-req-8841
{"recipient": "+15551234567", "message": "Your OTP is 4821", "priority": "EXPRESS"}
```

`Idempotency-Key` is required on every request. If Acme's client times out and resends the exact same request with the same key, the API won't charge or send twice — it just replays the first response. That mechanism (and what happens if the key gets reused with a *different* body) is covered below.

**Step 1 — API validates and charges (one database transaction)**
The API checks:
- Is `+15551234567` a real-looking phone number? (yes)
- Is the message too long? (no)
- Does Acme have at least 1 credit? (yes, has 100,000)

In a single atomic database write, it:
- Deducts 1 credit from Acme's wallet (99,999 left)
- Inserts a row in the `sms` table: status = `QUEUED`
- Inserts a row in an `outbox_events` table saying "tell Kafka about this new SMS"

All three happen together or not at all — so Acme is never charged without the message being recorded, and never has a message recorded without being charged. That's the whole point of doing it in one transaction.

The API replies `202 Accepted` immediately. Acme doesn't wait for the SMS to actually go out.

**Step 2 — Outbox Relay notices the new row**
A separate small process polls that `outbox_events` table every so often, sees the new row, and publishes it to Kafka on the `sms.express` topic (because priority was `EXPRESS`). It marks the row as published so it won't send it twice.

**Step 3 — Express Worker picks it up**
Because this was `EXPRESS`, it goes straight to an Express Worker (Normal-priority messages instead go through the Fair Scheduler first — see below). The worker:
- Reads the message from Kafka
- Calls the Operator API: "send 'Your OTP is 4821' to +15551234567"
- Gets back an operator message ID
- Updates the `sms` row: status = `SENT_TO_OPERATOR`

Done. Acme's OTP goes out.

## Everything that can go wrong, in order

The happy path above is one way through the system. Here's every stage where that same request can instead fail, walked in the order a request actually hits them.

### 1. Before anything touches the database (API request validation)

These checks run before the wallet transaction even opens, in this order:

| Check | Failure response |
|---|---|
| Body too large | `413 REQUEST_BODY_TOO_LARGE` |
| Per-tenant rate limit exceeded (token bucket, default 50 req/s per `X-Tenant-ID`, backed by Redis) | `429 RATE_LIMIT_EXCEEDED` |
| Missing or invalid `X-Tenant-ID` header | `422` (generic request-validation error) |
| Missing `Idempotency-Key` header | `400 MISSING_IDEMPOTENCY_KEY` |
| Malformed JSON / missing required fields | `400 INVALID_JSON` |
| Recipient isn't a valid E.164 number | `422 INVALID_RECIPIENT` |
| Message too long (160 chars GSM-7, or 70 chars if it needs Unicode/UCS-2) | `422 MESSAGE_TOO_LONG` |
| Batch send with zero recipients | `422 EMPTY_RECIPIENT_LIST` |
| Batch send over 50,000 recipients | `422 BATCH_TOO_LARGE` |

Nothing here charges Acme or writes a row — a request has to pass all of these before the wallet is even looked at. (One quirk: the rate limiter runs *before* the tenant-header check, so a request with no `X-Tenant-ID` at all skips rate limiting and fails on the header check instead.)

### 2. Idempotency-Key conflicts

Every request carries an `Idempotency-Key`. The API hashes the request body and checks it against that key:

- **Same key, same body, already completed** → the original response is replayed. Acme is not charged again and no new message is queued.
- **Same key, different body** (client reused a key for a different request) → `422 IDEMPOTENCY_KEY_REUSED`.
- **Same key, another request with it is still being processed** (a fast client retry racing the original) → `409 IDEMPOTENCY_KEY_IN_FLIGHT`.

### 3. Insufficient balance

If Acme's balance check (`balance >= cost`) fails, the deduct simply updates zero rows → `402 INSUFFICIENT_BALANCE`. Nothing is charged, no `sms` row or outbox row is created.

### 4. The atomic write itself fails

If the database is down or a constraint fails mid-transaction, there's currently no dedicated handling for it — the request falls through to a plain `500 Internal Server Error` with none of the usual JSON error structure (no `request_id`, no error code). This is a known gap, not a designed behavior.

### 5. Outbox Relay can't reach Kafka

The relay has no retry logic of its own. If publishing to Kafka fails partway through a batch, that relay cycle crashes, the affected rows are left unpublished, and the process supervisor restarts the relay — which just tries the same unpublished rows again on its next poll. Because a row is only marked "published" *after* the whole batch succeeds, a crash right after some rows were successfully sent (but before the batch finished) can cause those rows to be sent to Kafka a second time. Kafka delivery here is at-least-once, not exactly-once.

### 6. Fair Scheduler hits a bad message (Normal-priority only)

If a malformed/corrupt event shows up on the `sms.normal` Kafka topic, the scheduler's ingest loop crashes before committing that offset — so on restart, the same bad message is redelivered and can crash it again. There's no dead-lettering at this layer today.

### 7. Operator API call fails

The worker classifies the failure:

- **Retryable** (timeout, connection error, or a 5xx from the carrier): wait a backoff, then republish to the same Kafka topic for another attempt. `sms.status` stays `QUEUED` while this happens; `attempt_count` and `last_error` are updated on each try.
  - **Express**: fixed backoff of 0.2s then 0.4s (2 attempts max), each jittered ×0.5–1.5.
  - **Normal**: exponential backoff `0.5s × 2^n`, capped at 30s (5 attempts max), each jittered ×0.5–1.5.
- **Not retryable** (carrier says "bad number", a 4xx): no point retrying, goes straight to dead-letter.

A per-message Redis lock (60s TTL) stops a redelivered/duplicate Kafka event from dispatching to the operator twice at the same time. And if a message somehow arrives already in a terminal state (already sent, already dead-lettered), the worker just skips it instead of reprocessing.

### 8. Retries exhausted or not retryable → dead-lettered

- The `sms` row is atomically flipped to `FAILED_DEAD_LETTER` — guarded so a redelivered dead-letter event can't do this (and refund) twice.
- Acme's 1 credit is refunded (99,999 → back to normal) and a wallet ledger entry is recorded.
- The original message plus `{error, attempt_count, first_attempted_at, last_attempted_at}` is published to a per-tier dead-letter topic for manual investigation. If that publish itself fails, it's retried up to 3 times; if all 3 fail, it's logged as a critical error and given up on — the refund has already happened at this point and is not undone.

### 9. A genuinely unexpected worker crash

If something outside the above (a bug, an unrelated DB error) blows up mid-dispatch, a safety-net handler catches it and sends the message straight to the dead-letter topic tagged `unexpected_worker_error` — but **skips the refund step**. This is a known gap: this specific failure path does not give Acme their credit back.

### Message status, end to end

| Status | Meaning |
|---|---|
| `QUEUED` | Accepted and charged, waiting for or retrying delivery |
| `SENT_TO_OPERATOR` | Carrier accepted it |
| `FAILED_DEAD_LETTER` | Gave up; refunded (except the crash case above) and parked for review |
| `DELIVERED` | Reserved for a future carrier delivery-receipt webhook — not set by anything today |
| `FAILED` | Also reserved for future use — not set by anything today |

### Error response shape

Every handled error above (except the unhandled `500` in section 4) returns the same JSON shape:

```
{"error": {"code": "INSUFFICIENT_BALANCE", "message": "...", "request_id": "...", "details": {...}}}
```

## Why Normal-priority messages take a detour through the Fair Scheduler

Express and Normal are kept on physically separate Kafka topics and separate worker pools, so a flood of Normal traffic can never delay an Express message — there's no shared queue for it to get stuck behind.

Within Normal traffic, one giant sender (say, a company blasting 500,000 marketing texts) could otherwise crowd out everyone else's normal messages. The Fair Scheduler prevents that: it holds each tenant's Normal messages in their own queue and takes turns round-robin (Deficit Round Robin) — every active tenant gets an equal quantum per round, not weighted by tenant size or plan, so no single active tenant can hog the worker pool. If only one tenant has anything to send, they still get full speed — fairness only kicks in when there's actual contention.

## The one-sentence version

**Charge and record the message atomically → hand it to a durable queue → a worker calls the carrier → success updates the message, failure retries or refunds-and-parks-it for review.**

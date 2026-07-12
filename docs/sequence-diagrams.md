# Sequence Diagrams

## Single SMS

```mermaid
sequenceDiagram
    participant Client
    participant API as API Service
    participant Redis
    participant DB as PostgreSQL
    participant Relay as Outbox Relay
    participant Kafka
    participant Sched as Fair Scheduler
    participant NW as Normal Worker
    participant Op as Operator API

    Client->>API: POST /api/v1/sms {recipient, body, Idempotency-Key}
    API->>Redis: SET idem:{tenant}:{key} NX PX 30s
    alt key already exists (in-flight or done)
        API-->>Client: cached result or 409
    else new key
        API->>DB: BEGIN
        API->>DB: UPDATE wallets SET balance -= cost WHERE balance >= cost RETURNING balance
        alt 0 rows returned
            API->>DB: ROLLBACK
            API-->>Client: 402 Payment Required
        else deducted
            API->>DB: INSERT INTO sms (status=QUEUED)
            API->>DB: INSERT INTO wallet_ledger
            API->>DB: INSERT INTO outbox_events (event=SmsAccepted)
            API->>DB: COMMIT
            API->>Redis: mark idempotency key COMPLETED, cache response
            API-->>Client: 202 Accepted {sms_id, status: QUEUED}
        end
    end

    loop poll unpublished
        Relay->>DB: SELECT ... WHERE published_at IS NULL FOR UPDATE SKIP LOCKED
        Relay->>Kafka: publish to sms.normal (key=tenant_id)
        Relay->>DB: UPDATE outbox_events SET published_at = now()
    end

    Kafka->>Sched: consume sms.normal
    Sched->>Sched: LPUSH queue:tenant:{id}, ZADD active_tenants

    NW->>Sched: Dispatch() — DRR pick next fair message
    Sched-->>NW: sms payload
    NW->>Op: send SMS
    Op-->>NW: ack / result
    NW->>DB: UPDATE sms SET status = SENT_TO_OPERATOR, sent_at = now()
```

## Batch SMS

```mermaid
sequenceDiagram
    participant Client
    participant API as API Service
    participant DB as PostgreSQL
    participant Relay as Outbox Relay
    participant Kafka
    participant Fanout as Fan-out Consumer
    participant Sched as Fair Scheduler
    participant NW as Normal Worker
    participant Op as Operator API

    Client->>API: POST /api/v1/sms/batch {recipients[], body, Idempotency-Key}
    API->>API: total_cost = len(recipients) * unit_cost

    API->>DB: BEGIN
    API->>DB: UPDATE wallets SET balance -= total_cost WHERE balance >= total_cost RETURNING balance
    alt insufficient balance
        API->>DB: ROLLBACK
        API-->>Client: 402 Payment Required — whole batch rejected, zero side effects
    else sufficient
        API->>DB: INSERT INTO batches (status=ACCEPTED, total_cost)
        API->>DB: bulk INSERT INTO sms (one row per recipient, batch_id set, message_body=NULL)
        API->>DB: INSERT INTO wallet_ledger (one entry, delta = -total_cost)
        API->>DB: INSERT INTO outbox_events (event=BatchAccepted, aggregate_id=batch_id)  -- O(1), not O(n)
        API->>DB: COMMIT
        API-->>Client: 202 Accepted {batch_id, recipient_count, status: ACCEPTED}
    end

    Relay->>DB: poll unpublished
    Relay->>Kafka: publish BatchAccepted{batch_id} to sms.normal

    Kafka->>Fanout: consume BatchAccepted
    loop paginate sms WHERE batch_id = ?
        Fanout->>DB: SELECT id, recipient FROM sms WHERE batch_id = ? LIMIT 500 OFFSET ...
        Fanout->>Sched: enqueue each recipient as an individual dispatch unit
    end

    Sched->>Sched: DRR across all active tenants (batch recipients compete fairly, not as one giant burst)
    NW->>Sched: Dispatch()
    Sched-->>NW: sms payload (message_body resolved from batches.message_body)
    NW->>Op: send SMS
    Op-->>NW: ack
    NW->>DB: UPDATE sms SET status = SENT_TO_OPERATOR
    NW->>DB: periodically: recompute batches.status from child sms statuses (COMPLETED / PARTIALLY_FAILED)
```

**Atomicity note:** "atomic" here means the *acceptance decision* — either the entire batch is charged and persisted, or none of it is. It does **not** mean every recipient is guaranteed delivery in lockstep; delivery is inherently per-recipient and best-effort against the operator (a batch can end up `PARTIALLY_FAILED` at the *delivery* level while having been `ACCEPTED` atomically at the *charge* level — these are different guarantees and the docs are explicit about not conflating them).

## Express SMS

```mermaid
sequenceDiagram
    participant Client
    participant API as API Service
    participant DB as PostgreSQL
    participant Relay as Outbox Relay
    participant Kafka as Kafka sms.express
    participant EW as Express Worker
    participant Op as Operator API

    Client->>API: POST /api/v1/sms {..., priority: EXPRESS}
    API->>DB: BEGIN
    API->>DB: atomic deduct
    API->>DB: INSERT sms (priority=EXPRESS)
    API->>DB: INSERT outbox_events (topic=sms.express)
    API->>DB: COMMIT
    API-->>Client: 202 Accepted

    Relay->>DB: poll unpublished
    Relay->>Kafka: publish directly to sms.express (dedicated topic)

    Kafka->>EW: consume — dedicated consumer group, dedicated worker pool
    Note over EW: No Fair Scheduler involvement.<br/>No competition with Normal-tier traffic.<br/>Retry budget: 2 fast attempts (200ms/400ms), then DLQ.
    EW->>Op: send SMS (isolated resource pool, headroom sized for SLA)
    Op-->>EW: ack
    EW->>DB: UPDATE sms SET status = SENT_TO_OPERATOR
```

**Why Express bypasses the scheduler entirely, not just gets scheduled first:** Express's latency guarantee must hold *regardless of Normal-tier load*. If Express shared any component with Normal — the same topic, the same consumer group, or even the same DRR ledger with a "priority weight" — then a large enough Normal surge could still add queueing delay to Express through resource contention on that shared component (CPU, Redis round-trips, consumer lag). Full physical separation (topic → consumer group → worker pool → even a separate node pool/resource quota in production) means Express latency is a function of Express volume and Express capacity alone — a property that's independently provable and testable, not an emergent behavior of a shared scheduler under load.

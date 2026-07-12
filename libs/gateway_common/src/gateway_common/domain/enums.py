from enum import StrEnum


# API-facing SMS priority (NORMAL/EXPRESS) — distinct from Tier below
# (internal worker-pool routing). Same two-value shape is coincidental; do
# not conflate or merge them.
class Priority(StrEnum):
    NORMAL = "NORMAL"
    EXPRESS = "EXPRESS"


# Internal worker-tier routing key (express/normal, lowercase) — distinct
# from Priority above. Kept separate per the layering: API contract vs.
# internal dispatch routing.
class Tier(StrEnum):
    EXPRESS = "express"
    NORMAL = "normal"


class SmsStatus(StrEnum):
    QUEUED = "QUEUED"
    SENT_TO_OPERATOR = "SENT_TO_OPERATOR"
    DELIVERED = "DELIVERED"
    FAILED = "FAILED"
    FAILED_DEAD_LETTER = "FAILED_DEAD_LETTER"


class BatchStatus(StrEnum):
    ACCEPTED = "ACCEPTED"
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"
    PARTIALLY_FAILED = "PARTIALLY_FAILED"
    FAILED = "FAILED"


class LedgerReason(StrEnum):
    TOPUP = "TOPUP"
    SMS_DEDUCT = "SMS_DEDUCT"
    REFUND = "REFUND"


class TopupStatus(StrEnum):
    COMPLETED = "COMPLETED"


class OutboxAggregateType(StrEnum):
    SMS = "sms"
    BATCH = "batch"
    WALLET = "wallet"


class OutboxEventType(StrEnum):
    SMS_ACCEPTED = "SmsAccepted"
    BATCH_ACCEPTED = "BatchAccepted"
    WALLET_REFUNDED = "WalletRefunded"


class IdempotencyStatus(StrEnum):
    IN_PROGRESS = "IN_PROGRESS"
    COMPLETED = "COMPLETED"


class IdempotencyResourceType(StrEnum):
    SMS = "sms"
    BATCH = "batch"
    WALLET_CHARGE = "wallet_charge"

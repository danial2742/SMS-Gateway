import re
from datetime import datetime, timedelta

from gateway_common.domain.errors import (
    BatchTooLargeError,
    EmptyRecipientListError,
    InvalidDateRangeError,
    InvalidRecipientError,
    MessageTooLongError,
)

# Single-page SMS limits at the two encodings the operator protocol supports
# (docs/assumptions.md #5: single-page only, encoding affects the technical
# character limit, not the pricing/billing model).
GSM7_SINGLE_PAGE_LIMIT = 160
UCS2_SINGLE_PAGE_LIMIT = 70

# docs/assumptions.md #10: "low tens of thousands" per batch request;
# docs/database.md Concurrency uses 50,000 as its own worked example.
DEFAULT_BATCH_MAX_RECIPIENTS = 50_000

# docs/api.md Reports: range width capped to bound query cost even with
# cursor pagination's constant per-page cost.
DEFAULT_REPORT_MAX_RANGE_DAYS = 90

_GSM7_BASIC_SET = (
    "@£$¥èéùìòÇ\nØø\rÅåΔ_ΦΓΛΩΠΨΣΘΞ\x1bÆæßÉ !\"#¤%&'()*+,-./0123456789:;<=>?"
    "¡ABCDEFGHIJKLMNOPQRSTUVWXYZÄÖÑÜ§¿abcdefghijklmnopqrstuvwxyzäöñüà"
)


def _is_gsm7(text: str) -> bool:
    return all(ch in _GSM7_BASIC_SET for ch in text)


# E.164: a leading '+', then 1-15 digits, first digit non-zero. Format
# validation only — docs/assumptions.md #13: "format validation as the only
# correctness check performed by this system — no carrier lookup,
# number-portability check, or live reachability probe before acceptance."
# Deliberately NOT using phonenumbers.is_valid_number(), which validates
# against real numbering-plan/carrier data and rejects reserved-for-fictional
# numbers like +15551234567 — the exact example used throughout the docs.
_E164_RE = re.compile(r"^\+[1-9]\d{1,14}$")


def validate_recipient(recipient: str, *, index: int | None = None) -> None:
    if not _E164_RE.match(recipient):
        raise InvalidRecipientError(f"'{recipient}' is not a valid E.164 number", index=index)


def validate_message(message: str) -> None:
    limit = GSM7_SINGLE_PAGE_LIMIT if _is_gsm7(message) else UCS2_SINGLE_PAGE_LIMIT
    if len(message) > limit:
        raise MessageTooLongError(
            f"message exceeds single-page limit of {limit} characters for the resolved encoding"
        )


def validate_recipients(recipients: list[str], *, max_recipients: int = DEFAULT_BATCH_MAX_RECIPIENTS) -> None:
    if not recipients:
        raise EmptyRecipientListError("recipients must be a non-empty list")
    if len(recipients) > max_recipients:
        raise BatchTooLargeError(
            f"batch of {len(recipients)} exceeds the configured maximum of {max_recipients}"
        )
    for i, recipient in enumerate(recipients):
        validate_recipient(recipient, index=i)


def validate_date_range(
    from_: datetime, to: datetime, *, max_range_days: int = DEFAULT_REPORT_MAX_RANGE_DAYS
) -> None:
    if from_ >= to:
        raise InvalidDateRangeError("'from' must be strictly before 'to'")
    if to - from_ > timedelta(days=max_range_days):
        raise InvalidDateRangeError(
            f"date range exceeds the configured maximum of {max_range_days} days"
        )

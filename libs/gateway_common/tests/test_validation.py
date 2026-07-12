import pytest
from gateway_common.domain.errors import (
    BatchTooLargeError,
    EmptyRecipientListError,
    InvalidDateRangeError,
    InvalidRecipientError,
    MessageTooLongError,
)
from gateway_common.validation import (
    GSM7_SINGLE_PAGE_LIMIT,
    UCS2_SINGLE_PAGE_LIMIT,
    validate_date_range,
    validate_message,
    validate_recipient,
    validate_recipients,
)


def test_validate_recipient_accepts_the_docs_example_number():
    # +15551234567 is used throughout docs/api.md and README.md — a fictional
    # reserved NANP number that a real numbering-plan validator (phonenumbers
    # .is_valid_number) would reject. Format-only validation must accept it.
    validate_recipient("+15551234567")


@pytest.mark.parametrize(
    "recipient",
    ["15551234567", "+0123456789", "not-a-number", "+", ""],
)
def test_validate_recipient_rejects_malformed_numbers(recipient):
    with pytest.raises(InvalidRecipientError):
        validate_recipient(recipient)


def test_validate_message_within_gsm7_limit_passes():
    validate_message("x" * GSM7_SINGLE_PAGE_LIMIT)


def test_validate_message_over_gsm7_limit_raises():
    with pytest.raises(MessageTooLongError):
        validate_message("x" * (GSM7_SINGLE_PAGE_LIMIT + 1))


def test_validate_message_ucs2_limit_is_shorter():
    # A non-GSM7 character (e.g. emoji) forces UCS-2 encoding, which has a
    # much shorter single-page limit.
    message = "😀" * (UCS2_SINGLE_PAGE_LIMIT + 1)
    with pytest.raises(MessageTooLongError):
        validate_message(message)


def test_validate_recipients_rejects_empty_list():
    with pytest.raises(EmptyRecipientListError):
        validate_recipients([])


def test_validate_recipients_rejects_over_max():
    with pytest.raises(BatchTooLargeError):
        validate_recipients(["+15551234567"] * 5, max_recipients=3)


def test_validate_recipients_reports_offending_index():
    with pytest.raises(InvalidRecipientError) as exc_info:
        validate_recipients(["+15551234567", "bad-number"])
    assert exc_info.value.extra["index"] == 1


def test_validate_date_range_rejects_inverted_range():
    from datetime import UTC, datetime

    with pytest.raises(InvalidDateRangeError):
        validate_date_range(datetime(2026, 1, 2, tzinfo=UTC), datetime(2026, 1, 1, tzinfo=UTC))


def test_validate_date_range_rejects_over_max_width():
    from datetime import UTC, datetime

    with pytest.raises(InvalidDateRangeError):
        validate_date_range(
            datetime(2026, 1, 1, tzinfo=UTC), datetime(2026, 12, 1, tzinfo=UTC), max_range_days=90
        )

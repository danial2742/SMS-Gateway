import pytest
from gateway_common.domain import errors as e

# One row per docs/api.md error table — code/http_status must match exactly,
# since the API error envelope is a client-visible contract.
EXPECTED = [
    (e.InsufficientBalanceError, "INSUFFICIENT_BALANCE", 402),
    (e.IdempotencyKeyMissingError, "MISSING_IDEMPOTENCY_KEY", 400),
    (e.IdempotencyKeyInFlightError, "IDEMPOTENCY_KEY_IN_FLIGHT", 409),
    (e.IdempotencyKeyReusedError, "IDEMPOTENCY_KEY_REUSED", 422),
    (e.InvalidRecipientError, "INVALID_RECIPIENT", 422),
    (e.MessageTooLongError, "MESSAGE_TOO_LONG", 422),
    (e.EmptyRecipientListError, "EMPTY_RECIPIENT_LIST", 422),
    (e.BatchTooLargeError, "BATCH_TOO_LARGE", 422),
    (e.InvalidAmountError, "INVALID_AMOUNT", 422),
    (e.InvalidJsonError, "INVALID_JSON", 400),
    (e.SmsNotFoundError, "SMS_NOT_FOUND", 404),
    (e.BatchNotFoundError, "BATCH_NOT_FOUND", 404),
    (e.WalletNotFoundError, "WALLET_NOT_FOUND", 404),
    (e.MissingDateRangeError, "MISSING_DATE_RANGE", 400),
    (e.InvalidDateRangeError, "INVALID_DATE_RANGE", 422),
    (e.InvalidCursorError, "INVALID_CURSOR", 422),
    (e.RateLimitExceededError, "RATE_LIMIT_EXCEEDED", 429),
]


@pytest.mark.parametrize("error_cls,code,http_status", EXPECTED)
def test_error_code_and_status(error_cls, code, http_status):
    err = error_cls("message")
    assert err.code == code
    assert err.http_status == http_status
    assert isinstance(err, e.GatewayError)

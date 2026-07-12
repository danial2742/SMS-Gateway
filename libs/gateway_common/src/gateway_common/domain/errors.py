class GatewayError(Exception):
    """Base for every domain error the API/worker layer raises. Maps 1:1 to a
    row in docs/api.md's error tables — `code` and `http_status` are the
    contract, not implementation detail.
    """

    code: str = "INTERNAL_ERROR"
    http_status: int = 500

    def __init__(self, message: str, **extra: object) -> None:
        super().__init__(message)
        self.message = message
        self.extra = extra


class InsufficientBalanceError(GatewayError):
    code = "INSUFFICIENT_BALANCE"
    http_status = 402


class IdempotencyKeyMissingError(GatewayError):
    code = "MISSING_IDEMPOTENCY_KEY"
    http_status = 400


class IdempotencyKeyInFlightError(GatewayError):
    code = "IDEMPOTENCY_KEY_IN_FLIGHT"
    http_status = 409


class IdempotencyKeyReusedError(GatewayError):
    code = "IDEMPOTENCY_KEY_REUSED"
    http_status = 422


class InvalidRecipientError(GatewayError):
    code = "INVALID_RECIPIENT"
    http_status = 422


class MessageTooLongError(GatewayError):
    code = "MESSAGE_TOO_LONG"
    http_status = 422


class EmptyRecipientListError(GatewayError):
    code = "EMPTY_RECIPIENT_LIST"
    http_status = 422


class BatchTooLargeError(GatewayError):
    code = "BATCH_TOO_LARGE"
    http_status = 422


class InvalidAmountError(GatewayError):
    code = "INVALID_AMOUNT"
    http_status = 422


class InvalidJsonError(GatewayError):
    code = "INVALID_JSON"
    http_status = 400


class SmsNotFoundError(GatewayError):
    code = "SMS_NOT_FOUND"
    http_status = 404


class BatchNotFoundError(GatewayError):
    code = "BATCH_NOT_FOUND"
    http_status = 404


class WalletNotFoundError(GatewayError):
    code = "WALLET_NOT_FOUND"
    http_status = 404


class MissingDateRangeError(GatewayError):
    code = "MISSING_DATE_RANGE"
    http_status = 400


class InvalidDateRangeError(GatewayError):
    code = "INVALID_DATE_RANGE"
    http_status = 422


class InvalidCursorError(GatewayError):
    code = "INVALID_CURSOR"
    http_status = 422


class RateLimitExceededError(GatewayError):
    code = "RATE_LIMIT_EXCEEDED"
    http_status = 429

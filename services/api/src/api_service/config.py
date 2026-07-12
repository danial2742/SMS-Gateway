from gateway_common.config import DatabaseSettings, KafkaSettings, RedisSettings


class ApiSettings(DatabaseSettings, RedisSettings, KafkaSettings):
    idempotency_key_ttl_seconds: int = 30
    rate_limit_rps: int = 50
    report_max_range_days: int = 90
    batch_max_recipients: int = 50_000
    max_request_body_bytes: int = 10_000_000


settings = ApiSettings()

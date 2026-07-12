from pydantic_settings import BaseSettings, SettingsConfigDict


class BaseServiceSettings(BaseSettings):
    """Base config every service extends. Validated eagerly at import time —
    a missing/malformed required env var must fail startup immediately
    (deployment.md: fail the liveness probe, not serve degraded traffic).
    """

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    log_level: str = "info"
    shutdown_grace_period_seconds: int = 30
    metrics_port: int = 9000
    health_port: int = 8081


class DatabaseSettings(BaseServiceSettings):
    database_url: str
    database_read_url: str | None = None


class RedisSettings(BaseServiceSettings):
    redis_url: str = "redis://localhost:6379/0"


class OperatorSettings(BaseServiceSettings):
    operator_mode: str = "mock"  # "mock" | "http" — mock is the local/dev/compose default
    operator_api_url: str = "http://operator-mock:9100"
    operator_api_timeout_ms: int = 5000


class KafkaSettings(BaseServiceSettings):
    kafka_brokers: str = "localhost:9092"
    kafka_topic_express: str = "sms.express"
    kafka_topic_normal: str = "sms.normal"
    kafka_topic_dlq_express: str = "sms.dlq.express"
    kafka_topic_dlq_normal: str = "sms.dlq.normal"

    @property
    def brokers_list(self) -> list[str]:
        return [b.strip() for b in self.kafka_brokers.split(",") if b.strip()]

from gateway_common.config import DatabaseSettings, KafkaSettings, RedisSettings


class SchedulerSettings(DatabaseSettings, RedisSettings, KafkaSettings):
    consumer_group: str = "fair-scheduler"
    quantum: int = 10
    round_interval_seconds: float = 0.2


settings = SchedulerSettings()

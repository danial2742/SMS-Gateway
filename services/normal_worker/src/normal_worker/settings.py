from gateway_common.config import DatabaseSettings, KafkaSettings, OperatorSettings, RedisSettings


class NormalWorkerSettings(DatabaseSettings, RedisSettings, KafkaSettings, OperatorSettings):
    blpop_timeout_seconds: int = 5


settings = NormalWorkerSettings()

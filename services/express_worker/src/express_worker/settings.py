from gateway_common.config import DatabaseSettings, KafkaSettings, OperatorSettings, RedisSettings


class ExpressWorkerSettings(DatabaseSettings, RedisSettings, KafkaSettings, OperatorSettings):
    consumer_group: str = "express-workers"


settings = ExpressWorkerSettings()

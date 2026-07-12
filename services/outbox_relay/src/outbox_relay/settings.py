from gateway_common.config import DatabaseSettings, KafkaSettings


class RelaySettings(DatabaseSettings, KafkaSettings):
    poll_interval_seconds: float = 0.5
    batch_size: int = 100


settings = RelaySettings()

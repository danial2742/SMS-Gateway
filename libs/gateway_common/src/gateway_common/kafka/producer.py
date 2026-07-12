import json
import uuid

from aiokafka import AIOKafkaProducer

from gateway_common.kafka.topics import HEADER_ATTEMPT_COUNT, HEADER_REQUEST_ID


class GatewayProducer:
    """Thin aiokafka wrapper: keys every message by tenant_id (partition_key,
    per docs/database.md outbox_events) and stamps request_id/attempt_count
    headers for end-to-end correlation (observability.md).
    """

    def __init__(self, brokers: list[str]) -> None:
        self._producer = AIOKafkaProducer(bootstrap_servers=brokers)

    async def start(self) -> None:
        await self._producer.start()

    async def stop(self) -> None:
        await self._producer.stop()

    @property
    def client(self) -> AIOKafkaProducer:
        return self._producer

    async def send(
        self,
        topic: str,
        payload: dict,
        *,
        tenant_id: uuid.UUID,
        request_id: str,
        attempt_count: int = 0,
    ) -> None:
        headers = [
            (HEADER_REQUEST_ID, request_id.encode()),
            (HEADER_ATTEMPT_COUNT, str(attempt_count).encode()),
        ]
        await self._producer.send_and_wait(
            topic,
            key=str(tenant_id).encode(),
            value=json.dumps(payload).encode(),
            headers=headers,
        )

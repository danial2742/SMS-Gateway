import json

from aiokafka import AIOKafkaConsumer

from gateway_common.kafka.topics import HEADER_ATTEMPT_COUNT, HEADER_REQUEST_ID


class ConsumedMessage:
    __slots__ = ("payload", "request_id", "attempt_count", "raw")

    def __init__(self, payload: dict, request_id: str, attempt_count: int, raw: object) -> None:
        self.payload = payload
        self.request_id = request_id
        self.attempt_count = attempt_count
        self.raw = raw


def make_consumer(
    *topics: str, brokers: list[str], group_id: str, auto_offset_reset: str = "earliest"
) -> AIOKafkaConsumer:
    return AIOKafkaConsumer(
        *topics,
        bootstrap_servers=brokers,
        group_id=group_id,
        auto_offset_reset=auto_offset_reset,
        enable_auto_commit=False,
    )


def decode_message(raw) -> ConsumedMessage:
    headers = dict(raw.headers or [])
    request_id = headers.get(HEADER_REQUEST_ID, b"").decode()
    attempt_count = int(headers.get(HEADER_ATTEMPT_COUNT, b"0").decode() or "0")
    payload = json.loads(raw.value.decode())
    return ConsumedMessage(payload=payload, request_id=request_id, attempt_count=attempt_count, raw=raw)

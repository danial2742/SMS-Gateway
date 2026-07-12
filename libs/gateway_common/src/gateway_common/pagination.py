import base64
import json
import uuid
from datetime import datetime

from gateway_common.domain.errors import InvalidCursorError

DEFAULT_PAGE_LIMIT = 50
MAX_PAGE_LIMIT = 200


class Cursor:
    """Opaque keyset-pagination cursor over (created_at, id) — ADR-011:
    cursor, not offset, so per-page cost stays constant regardless of depth.
    """

    __slots__ = ("created_at", "id")

    def __init__(self, created_at: datetime, id: uuid.UUID) -> None:
        self.created_at = created_at
        self.id = id

    def encode(self) -> str:
        payload = {"created_at": self.created_at.isoformat(), "id": str(self.id)}
        raw = json.dumps(payload, separators=(",", ":")).encode()
        return base64.urlsafe_b64encode(raw).decode()

    @classmethod
    def decode(cls, cursor: str) -> "Cursor":
        try:
            raw = base64.urlsafe_b64decode(cursor.encode())
            payload = json.loads(raw)
            return cls(
                created_at=datetime.fromisoformat(payload["created_at"]),
                id=uuid.UUID(payload["id"]),
            )
        except Exception as exc:
            raise InvalidCursorError("cursor does not decode to a valid keyset position") from exc


def clamp_limit(limit: int | None) -> int:
    if limit is None:
        return DEFAULT_PAGE_LIMIT
    return max(1, min(limit, MAX_PAGE_LIMIT))

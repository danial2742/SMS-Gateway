import uuid
from datetime import UTC, datetime

import pytest
from gateway_common.domain.errors import InvalidCursorError
from gateway_common.pagination import Cursor, clamp_limit


def test_cursor_roundtrips_through_encode_decode():
    original = Cursor(created_at=datetime(2026, 7, 9, 10, 12, tzinfo=UTC), id=uuid.uuid4())
    decoded = Cursor.decode(original.encode())
    assert decoded.created_at == original.created_at
    assert decoded.id == original.id


def test_cursor_decode_rejects_garbage():
    with pytest.raises(InvalidCursorError):
        Cursor.decode("not-a-valid-cursor")


@pytest.mark.parametrize(
    "limit,expected",
    [(None, 50), (1, 1), (200, 200), (500, 200), (0, 1), (-5, 1)],
)
def test_clamp_limit(limit, expected):
    assert clamp_limit(limit) == expected

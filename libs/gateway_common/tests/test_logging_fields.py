from gateway_common.logging import GOVERNED_FIELDS, _merge_context
from gateway_common.request_context import request_id_var, tenant_id_var

# docs/observability.md Logging: fixed field set, no message content or PII.
BANNED_KEYS = {"message_body", "message", "recipient", "phone_number"}


def test_governed_field_set_matches_docs():
    assert GOVERNED_FIELDS == {
        "timestamp",
        "level",
        "service",
        "tenant_id",
        "request_id",
        "sms_id",
        "batch_id",
        "event",
        "latency_ms",
        "queue",
        "topic",
        "attempt_count",
    }


def test_merge_context_injects_request_and_tenant_id_from_contextvars():
    request_id_var.set("req-123")
    tenant_id_var.set("tenant-abc")

    event_dict = _merge_context(None, "info", {"event": "sms_dispatched", "sms_id": "sms-1"})

    assert event_dict["request_id"] == "req-123"
    assert event_dict["tenant_id"] == "tenant-abc"
    assert not BANNED_KEYS & event_dict.keys()


def test_merge_context_does_not_overwrite_explicit_fields():
    request_id_var.set("req-from-context")

    event_dict = _merge_context(None, "info", {"request_id": "req-explicit"})

    assert event_dict["request_id"] == "req-explicit"


def test_merge_context_omits_ids_when_context_is_unset():
    request_id_var.set("")
    tenant_id_var.set("")

    event_dict = _merge_context(None, "info", {"event": "startup"})

    assert "request_id" not in event_dict
    assert "tenant_id" not in event_dict

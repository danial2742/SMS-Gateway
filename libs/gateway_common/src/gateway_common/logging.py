import logging
import sys

import structlog
from structlog.typing import EventDict, WrappedLogger

from gateway_common.request_context import request_id_var, tenant_id_var

# Governed field set (docs/observability.md Logging): timestamp, level, service,
# tenant_id, request_id, sms_id/batch_id, event, latency_ms, queue/topic,
# attempt_count. This is a documented convention, checked only by
# libs/gateway_common/tests/test_logging_fields.py's exact-set assertion —
# nothing here rejects a log call that adds fields outside this set.
GOVERNED_FIELDS = frozenset(
    {
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
)


def _merge_context(logger: WrappedLogger, method_name: str, event_dict: EventDict) -> EventDict:
    request_id = request_id_var.get()
    tenant_id = tenant_id_var.get()
    if request_id:
        event_dict.setdefault("request_id", request_id)
    if tenant_id:
        event_dict.setdefault("tenant_id", tenant_id)
    return event_dict


def configure_logging(service_name: str, level: str = "info") -> None:
    logging.basicConfig(stream=sys.stdout, level=level.upper(), format="%(message)s")

    structlog.configure(
        processors=[
            structlog.contextvars.merge_contextvars,
            _merge_context,
            structlog.stdlib.add_log_level,
            structlog.processors.TimeStamper(fmt="iso", key="timestamp"),
            structlog.processors.EventRenamer("event"),
            structlog.processors.format_exc_info,
            structlog.processors.JSONRenderer(),
        ],
        wrapper_class=structlog.make_filtering_bound_logger(
            logging.getLevelNamesMapping().get(level.upper(), logging.INFO)
        ),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(),
        cache_logger_on_first_use=True,
    )
    structlog.contextvars.bind_contextvars(service=service_name)


def get_logger() -> structlog.stdlib.BoundLogger:
    return structlog.get_logger()

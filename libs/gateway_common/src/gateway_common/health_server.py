import asyncio
import json
from collections.abc import Awaitable, Callable

from gateway_common.logging import get_logger

logger = get_logger()

# Non-ASGI services (relay, scheduler, workers) still need /healthz and
# /readyz for K8s probes (docs/deployment.md Health checks: "readiness still
# gates rolling deploys" for worker services too). A raw asyncio TCP server
# avoids pulling FastAPI/uvicorn into services that otherwise have no HTTP
# surface at all.
ReadinessCheck = Callable[[], Awaitable[dict[str, str]]]

_READ_TIMEOUT_SECONDS = 5.0


async def _read_request_line(reader: asyncio.StreamReader) -> bytes:
    request_line = await reader.readline()
    while True:
        line = await reader.readline()
        if not line or line in (b"\r\n", b"\n"):
            break
    return request_line


async def _handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter, checks: ReadinessCheck) -> None:
    try:
        # A hung/slow client that opens a connection and never sends a full
        # request would otherwise leave this task (and the connection) alive
        # indefinitely — the generic except below already logs+closes on any
        # exception, so a timeout here is handled the same way.
        request_line = await asyncio.wait_for(_read_request_line(reader), timeout=_READ_TIMEOUT_SECONDS)

        path = request_line.decode(errors="ignore").split(" ")[1] if b" " in request_line else "/"

        if path.startswith("/readyz"):
            checks_result = await checks()
            all_ok = all(v == "ok" for v in checks_result.values())
            status = "ready" if all_ok else "not_ready"
            body = json.dumps({"status": status, "checks": checks_result}).encode()
            status_line = b"HTTP/1.1 200 OK\r\n" if all_ok else b"HTTP/1.1 503 Service Unavailable\r\n"
        else:
            body = json.dumps({"status": "ok"}).encode()
            status_line = b"HTTP/1.1 200 OK\r\n"

        writer.write(
            status_line
            + b"Content-Type: application/json\r\n"
            + f"Content-Length: {len(body)}\r\n\r\n".encode()
            + body
        )
        await writer.drain()
    except Exception as exc:
        logger.warning("health_server_handler_error", error=str(exc))
    finally:
        writer.close()


async def serve_health(port: int, readiness_checks: ReadinessCheck) -> asyncio.AbstractServer:
    return await asyncio.start_server(
        lambda r, w: _handle(r, w, readiness_checks), host="0.0.0.0", port=port
    )

from fastapi import APIRouter, Request, Response
from gateway_common.health import ping_kafka, ping_postgres, ping_redis, readiness_payload

router = APIRouter(tags=["health"])


@router.get(
    "/healthz",
    summary="Liveness probe",
    description="Is this process's own event loop responsive. No dependency checks are performed by design.",
)
async def healthz() -> dict:
    # Deliberately shallow — no dependency checks (deployment.md Liveness):
    # only answers "is this process's own event loop responsive."
    return {"status": "ok"}


@router.get(
    "/readyz",
    summary="Readiness probe",
    description=(
        "Can this pod currently serve traffic. Checks Postgres, Redis, and Kafka reachability. "
        "Returns 503 if any required dependency check fails."
    ),
)
async def readyz(request: Request, response: Response) -> dict:
    state = request.app.state
    checks = {
        "postgres": await ping_postgres(state.db_sessions.primary_engine),
        "redis": await ping_redis(state.redis),
        "kafka": await ping_kafka(state.kafka_health_producer),
    }
    body, status_code = await readiness_payload(checks)
    response.status_code = status_code
    return body

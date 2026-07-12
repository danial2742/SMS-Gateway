import time

from gateway_common.request_context import get_request_id
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response

# Per-tenant token bucket (docs/security.md Rate limiting): abuse/spike
# protection at the API tier, not a fairness mechanism (that's the Fair
# Scheduler's job, deliberately kept separate per ADR-006). Refills
# continuously via a Lua script so bucket state stays atomic under
# concurrent requests without a round-trip race.
_TOKEN_BUCKET_LUA = """
local key = KEYS[1]
local capacity = tonumber(ARGV[1])
local refill_per_sec = tonumber(ARGV[2])
local now = tonumber(ARGV[3])

local bucket = redis.call("HMGET", key, "tokens", "updated_at")
local tokens = tonumber(bucket[1])
local updated_at = tonumber(bucket[2])

if tokens == nil then
    tokens = capacity
    updated_at = now
end

local elapsed = math.max(0, now - updated_at)
tokens = math.min(capacity, tokens + elapsed * refill_per_sec)

local allowed = 0
if tokens >= 1 then
    tokens = tokens - 1
    allowed = 1
end

redis.call("HSET", key, "tokens", tokens, "updated_at", now)
redis.call("EXPIRE", key, 60)

return allowed
"""


class RateLimitMiddleware(BaseHTTPMiddleware):
    def __init__(self, app, redis, rps: int) -> None:
        super().__init__(app)
        self._redis = redis
        self._rps = rps
        self._script = redis.register_script(_TOKEN_BUCKET_LUA)

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        tenant_id = request.headers.get("X-Tenant-ID")
        if tenant_id is None:
            return await call_next(request)

        allowed = await self._script(
            keys=[f"ratelimit:{tenant_id}"], args=[self._rps, self._rps, time.time()]
        )
        if not int(allowed):
            return JSONResponse(
                status_code=429,
                content={
                    "error": {
                        "code": "RATE_LIMIT_EXCEEDED",
                        "message": "per-tenant request rate exceeded",
                        "request_id": get_request_id(),
                    }
                },
            )
        return await call_next(request)

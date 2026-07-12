from gateway_common.request_context import new_request_id, request_id_var
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import Response


class RequestIdMiddleware(BaseHTTPMiddleware):
    """Assigns request_id at the API edge (observability.md Correlation IDs):
    honors an inbound X-Request-ID if the client supplied one, generates one
    otherwise. Echoed back on the response and available to every error body
    via the contextvar.
    """

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        request_id = request.headers.get("X-Request-ID") or new_request_id()
        token = request_id_var.set(request_id)
        try:
            response = await call_next(request)
        finally:
            request_id_var.reset(token)
        response.headers["X-Request-ID"] = request_id
        return response

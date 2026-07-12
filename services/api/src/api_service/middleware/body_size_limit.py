from gateway_common.request_context import get_request_id
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint
from starlette.requests import Request
from starlette.responses import JSONResponse, Response


class BodySizeLimitMiddleware(BaseHTTPMiddleware):
    """Rejects requests whose declared Content-Length exceeds max_bytes
    before any parsing happens — batch/report endpoints otherwise validate
    size only after the full JSON body is parsed into memory.
    """

    def __init__(self, app, max_bytes: int) -> None:
        super().__init__(app)
        self._max_bytes = max_bytes

    async def dispatch(self, request: Request, call_next: RequestResponseEndpoint) -> Response:
        content_length = request.headers.get("content-length")
        if content_length is not None and content_length.isdigit() and int(content_length) > self._max_bytes:
            return JSONResponse(
                status_code=413,
                content={
                    "error": {
                        "code": "REQUEST_BODY_TOO_LARGE",
                        "message": "request body exceeds the maximum allowed size",
                        "request_id": get_request_id(),
                    }
                },
            )
        return await call_next(request)

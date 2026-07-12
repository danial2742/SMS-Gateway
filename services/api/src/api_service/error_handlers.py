from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from gateway_common.domain.errors import GatewayError, InvalidJsonError
from gateway_common.request_context import get_request_id


def _envelope(code: str, message: str, details: dict | None = None) -> dict:
    body = {"error": {"code": code, "message": message, "request_id": get_request_id()}}
    if details:
        body["error"]["details"] = details
    return body


def register_error_handlers(app: FastAPI) -> None:
    @app.exception_handler(GatewayError)
    async def gateway_error_handler(request: Request, exc: GatewayError) -> JSONResponse:
        return JSONResponse(
            status_code=exc.http_status, content=_envelope(exc.code, exc.message, exc.extra or None)
        )

    @app.exception_handler(RequestValidationError)
    async def validation_error_handler(
        request: Request, exc: RequestValidationError
    ) -> JSONResponse:
        fallback = InvalidJsonError("request body failed schema validation")
        return JSONResponse(
            status_code=fallback.http_status, content=_envelope(fallback.code, str(exc.errors()))
        )

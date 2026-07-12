import uuid

import httpx

from gateway_common.operator.protocol import (
    OperatorClientError,
    OperatorServerError,
    OperatorTimeoutError,
)


class HttpOperatorClient:
    """Real OperatorClient implementation: a plain httpx-based call against
    the (unspecified, black-box) operator HTTP endpoint — docs/assumptions.md #2.
    """

    def __init__(self, base_url: str, timeout_ms: int) -> None:
        self._client = httpx.AsyncClient(base_url=base_url, timeout=timeout_ms / 1000)

    async def send(self, *, recipient: str, message: str, sms_id: str) -> str:
        try:
            response = await self._client.post(
                "/messages", json={"to": recipient, "body": message, "client_ref": sms_id}
            )
        except httpx.TimeoutException as exc:
            raise OperatorTimeoutError(str(exc)) from exc
        except httpx.ConnectError as exc:
            raise OperatorTimeoutError(str(exc)) from exc

        if response.status_code >= 500:
            raise OperatorServerError(response.status_code, response.text)
        if response.status_code >= 400:
            raise OperatorClientError(response.status_code, response.text)

        return response.json().get("message_id", str(uuid.uuid4()))

    async def aclose(self) -> None:
        await self._client.aclose()

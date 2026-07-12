from gateway_common.config import OperatorSettings
from gateway_common.operator.http_client import HttpOperatorClient
from gateway_common.operator.mock import MockOperatorClient
from gateway_common.operator.protocol import OperatorClient


def make_operator_client(settings: OperatorSettings) -> OperatorClient:
    if settings.operator_mode == "mock":
        return MockOperatorClient()
    return HttpOperatorClient(settings.operator_api_url, settings.operator_api_timeout_ms)

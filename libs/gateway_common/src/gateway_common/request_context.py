import uuid
from contextvars import ContextVar

request_id_var: ContextVar[str] = ContextVar("request_id", default="")
tenant_id_var: ContextVar[str] = ContextVar("tenant_id", default="")


def new_request_id() -> str:
    return str(uuid.uuid4())


def get_request_id() -> str:
    return request_id_var.get()


def get_tenant_id() -> str:
    return tenant_id_var.get()

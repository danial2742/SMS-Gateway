from pydantic import BaseModel


class ErrorBody(BaseModel):
    code: str
    message: str
    request_id: str


class ErrorEnvelope(BaseModel):
    error: ErrorBody

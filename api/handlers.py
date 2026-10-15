"""Safe HTTP exception envelopes without Pydantic inputs or DB diagnostics."""

from __future__ import annotations

from fastapi import FastAPI, Request
from fastapi.exceptions import RequestValidationError
from fastapi.responses import JSONResponse
from sqlalchemy.exc import DBAPIError, IntegrityError
from sqlalchemy.exc import TimeoutError as DatabaseTimeoutError
from starlette.exceptions import HTTPException

from api.middleware import error_payload
from core.errors import DomainError
from core.logging import logger
from schemas import contracts, effects
from schemas.contracts import ErrorCode

_FIELD_NAMES = {
    "body",
    "path",
    "query",
    "after_sequence",
    "limit",
    "conversation_id",
    "message_id",
    "turn_id",
    "delivery_attempt_id",
}
for _object in vars(contracts).values():
    if isinstance(_object, type) and issubclass(_object, contracts.Contract):
        _FIELD_NAMES.update(_object.model_fields)
for _object in vars(effects).values():
    if isinstance(_object, type) and issubclass(_object, contracts.Contract):
        _FIELD_NAMES.update(_object.model_fields)


def error_response(request: Request, code: ErrorCode) -> JSONResponse:
    error = DomainError(code)
    logger.warning("", extra={"stage": "request", "code": code.value})
    return JSONResponse(
        status_code=error.status_code,
        content=error_payload(code, getattr(request.state, "request_id", "")),
    )


async def domain_error_handler(request: Request, error: DomainError) -> JSONResponse:
    return error_response(request, error.code)


async def validation_error_handler(request: Request, error: RequestValidationError) -> JSONResponse:
    response = error_response(request, ErrorCode.VALIDATION_ERROR)
    content = error_payload(ErrorCode.VALIDATION_ERROR, request.state.request_id)
    fields = []
    for issue in error.errors()[:64]:
        location: list[str | int] = []
        for value in issue.get("loc", ())[:8]:
            if isinstance(value, str):
                location.append(value if value in _FIELD_NAMES else "field")
            elif isinstance(value, int) and not isinstance(value, bool) and 0 <= value <= 65536:
                location.append(value)
            else:
                location.append("field")
        fields.append({"location": location, "code": "validation_error"})
    detail = content["detail"]
    assert isinstance(detail, dict)
    detail["fields"] = fields
    return JSONResponse(status_code=response.status_code, content=content)


async def http_error_handler(request: Request, error: HTTPException) -> JSONResponse:
    code = ErrorCode.NOT_FOUND if error.status_code == 404 else ErrorCode.VALIDATION_ERROR
    return error_response(request, code)


async def database_error_handler(request: Request, error: DBAPIError) -> JSONResponse:
    code = (
        ErrorCode.INTERNAL_ERROR
        if isinstance(error, IntegrityError)
        else ErrorCode.SERVICE_UNAVAILABLE
    )
    return error_response(request, code)


async def database_timeout_handler(request: Request, error: Exception) -> JSONResponse:
    return error_response(request, ErrorCode.SERVICE_UNAVAILABLE)


async def unexpected_error_handler(request: Request, error: Exception) -> JSONResponse:
    return error_response(request, ErrorCode.INTERNAL_ERROR)


def install_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DomainError, domain_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(RequestValidationError, validation_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(HTTPException, http_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(DBAPIError, database_error_handler)  # type: ignore[arg-type]
    app.add_exception_handler(DatabaseTimeoutError, database_timeout_handler)
    app.add_exception_handler(TimeoutError, database_timeout_handler)
    app.add_exception_handler(Exception, unexpected_error_handler)

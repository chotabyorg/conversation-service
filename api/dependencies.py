"""Only middleware-established authentication is trusted by HTTP handlers."""

from typing import Annotated

from fastapi import Depends, Request, Security
from fastapi.security import APIKeyHeader

from core.errors import DomainError
from db.database import Database
from schemas.contracts import ErrorCode, Principal

_service_header = APIKeyHeader(name="X-Service-Token", auto_error=False)


def get_principal(
    request: Request,
    credential: Annotated[str | None, Security(_service_header)],
) -> Principal:
    principal = getattr(request.state, "principal", None)
    if not isinstance(principal, Principal):
        raise DomainError(ErrorCode.UNAUTHENTICATED)
    return principal


def get_database(request: Request) -> Database:
    return request.app.state.database  # type: ignore[no-any-return]


TrustedPrincipal = Annotated[Principal, Depends(get_principal)]
AppDatabase = Annotated[Database, Depends(get_database)]

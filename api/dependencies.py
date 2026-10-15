"""Only middleware-established authentication is trusted by HTTP handlers."""

from typing import Annotated

from fastapi import Depends, Request, Security
from fastapi.security import APIKeyHeader

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from schemas.contracts import ErrorCode, Principal
from schemas.handoff import HumanPrincipal
from services.managed_client import ManagedAIPort

_service_header = APIKeyHeader(name="X-Service-Token", auto_error=False)


def get_principal(
    request: Request,
    credential: Annotated[str | None, Security(_service_header)],
) -> Principal:
    principal = getattr(request.state, "principal", None)
    if not isinstance(principal, Principal):
        raise DomainError(ErrorCode.UNAUTHENTICATED)
    return principal


def get_human_principal(request: Request) -> HumanPrincipal:
    principal = getattr(request.state, "human_principal", None)
    if not isinstance(principal, HumanPrincipal):
        raise DomainError(ErrorCode.UNAUTHENTICATED)
    return principal


def get_database(request: Request) -> Database:
    return request.app.state.database  # type: ignore[no-any-return]


def get_settings(request: Request) -> Settings:
    return request.app.state.settings  # type: ignore[no-any-return]


def get_managed_port(request: Request) -> ManagedAIPort:
    return request.app.state.managed_port  # type: ignore[no-any-return]


TrustedPrincipal = Annotated[Principal, Depends(get_principal)]
TrustedHuman = Annotated[HumanPrincipal, Depends(get_human_principal)]
AppDatabase = Annotated[Database, Depends(get_database)]
AppSettings = Annotated[Settings, Depends(get_settings)]
AppManagedPort = Annotated[ManagedAIPort, Depends(get_managed_port)]

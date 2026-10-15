"""Narrow lifecycle-only grant routes for CP and AI owners."""

from fastapi import APIRouter

from api.dependencies import AppDatabase, AppSettings, TrustedPrincipal
from core.errors import DomainError
from schemas.contracts import ErrorCode
from schemas.handoff import (
    GrantRevoke,
    InboxGrantProvision,
    InboxGrantProvisioned,
    InboxGrantRevoked,
)
from services import inbox_lifecycle

router = APIRouter(prefix="/internal/v1/inbox-grants")


@router.post("/provision", response_model=InboxGrantProvisioned)
async def provision_route(
    request: InboxGrantProvision,
    database: AppDatabase,
    settings: AppSettings,
    principal: TrustedPrincipal,
) -> InboxGrantProvisioned:
    if principal.principal_id != "control-plane-inbox-lifecycle":
        raise DomainError(ErrorCode.NOT_FOUND)
    return await inbox_lifecycle.provision_grant(database, settings, request)


@router.post("/revoke", response_model=InboxGrantRevoked)
async def revoke_route(
    request: GrantRevoke,
    database: AppDatabase,
    settings: AppSettings,
    principal: TrustedPrincipal,
) -> InboxGrantRevoked:
    allowed_source = {
        "control-plane-inbox-lifecycle": "CONTROL_PLANE_BINDING",
        "ai-handoff-lifecycle": "AI_AGENT",
    }.get(principal.principal_id)
    if principal.role != "INBOX_LIFECYCLE" or request.lifecycle_source != allowed_source:
        raise DomainError(ErrorCode.NOT_FOUND)
    return await inbox_lifecycle.revoke_grant(database, settings, request)

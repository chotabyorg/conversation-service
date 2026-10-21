"""Create-only visitor capabilities and transaction-local locked authorization.

No public routes or legacy Website transport consume these capabilities yet.
Future Handoff callers acquire their common grant scope before this root lock,
then authorize and read/write content in the same transaction, never check/use.
"""
import hashlib
import hmac
import re
import secrets
from datetime import timedelta
from uuid import uuid4

from pydantic import SecretStr
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import Conversation, WebsiteSessionCapability
from schemas.contracts import ErrorCode, Principal
from schemas.website import WEBSITE_NAMESPACE
from schemas.website_session import WebsiteSessionAccess, WebsiteSessionCreated, WebsiteSessionScope
from services.common import db_clock


def authorize_scope(principal: Principal, request: WebsiteSessionScope, settings: Settings) -> None:
    """Bind the private caller to the current server-pinned integration generation."""
    if not settings.website_session_capabilities_enabled or principal.role not in {
        "WEBSITE", "FULL",
    }:
        raise DomainError(ErrorCode.NOT_FOUND)
    if not any(
        grant.owner_id == request.owner_id and grant.integration_id == request.integration_id
        and grant.channel.value == "WEBSITE_WIDGET"
        and grant.provider_namespace == WEBSITE_NAMESPACE
        for grant in principal.grants
    ) or not any(
        binding.owner_id == request.owner_id and binding.integration_id == request.integration_id
        and binding.agent_id == request.agent_id
        and binding.capability_generation == request.binding_generation
        for binding in settings.website_bindings
    ):
        raise DomainError(ErrorCode.NOT_FOUND)


async def create(
    database: Database, principal: Principal, request: WebsiteSessionScope, settings: Settings,
) -> WebsiteSessionCreated:
    """Issue a fresh empty root and return its raw credential exactly once."""
    authorize_scope(principal, request, settings)
    session_id, conversation_id = uuid4(), uuid4()
    token = secrets.token_urlsafe(32)
    digest = hashlib.sha256(token.encode("ascii")).hexdigest()
    async with database.session_factory() as session, session.begin():
        created = await session.scalar(
            insert(Conversation).values(
                id=conversation_id, owner_id=request.owner_id,
                integration_id=request.integration_id, channel="WEBSITE_WIDGET",
                provider_thread_id=str(session_id),
            ).on_conflict_do_nothing(constraint="uq_conversation_identity").returning(Conversation.id)
        )
        if created is None:
            # Even a cryptographic collision must not attach an existing root.
            raise DomainError(ErrorCode.NOT_FOUND)
        clock = await db_clock(session)
        expires_at = clock + timedelta(seconds=settings.website_session_ttl_seconds)
        session.add(WebsiteSessionCapability(
            conversation_id=conversation_id, owner_id=request.owner_id,
            integration_id=request.integration_id, channel="WEBSITE_WIDGET",
            provider_thread_id=str(session_id), agent_id=request.agent_id,
            binding_generation=request.binding_generation, token_digest=digest,
            issued_at=clock, expires_at=expires_at,
        ))
        await session.flush()
    return WebsiteSessionCreated(
        session_id=session_id, session_token=SecretStr(token), expires_at=expires_at,
    )


async def locked_credential(
    session: AsyncSession, principal: Principal, request: WebsiteSessionAccess, settings: Settings,
) -> Conversation:
    """Check only the visitor credential; this does not authorize content access.

    Caller owns the transaction and must keep it through the protected operation.
    Revocation takes this same root lock. A failed check exposes no root metadata.
    Future content paths must also validate the common active grant epoch, both
    supplier proofs and their durable revocation floors in this transaction.
    """
    if not session.in_transaction():
        raise RuntimeError("Website capability access requires the caller transaction")
    authorize_scope(principal, request, settings)
    raw = request.session_token.get_secret_value()
    if re.fullmatch(r"[A-Za-z0-9_-]{43}", raw) is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    digest = hashlib.sha256(raw.encode("ascii")).hexdigest()
    root = await session.scalar(select(Conversation).where(
        Conversation.owner_id == request.owner_id,
        Conversation.integration_id == request.integration_id,
        Conversation.channel == "WEBSITE_WIDGET",
        Conversation.provider_thread_id == str(request.session_id),
    ).with_for_update().execution_options(populate_existing=True))
    if root is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    capability = await session.get(WebsiteSessionCapability, root.id, populate_existing=True)
    clock = await db_clock(session)
    if capability is None or not hmac.compare_digest(capability.token_digest, digest) or (
        capability.owner_id != request.owner_id
        or capability.integration_id != request.integration_id
        or capability.agent_id != request.agent_id
        or capability.binding_generation != request.binding_generation
        or capability.channel != "WEBSITE_WIDGET"
        or capability.provider_thread_id != str(request.session_id)
        or capability.revoked_at is not None or clock >= capability.expires_at
    ):
        raise DomainError(ErrorCode.NOT_FOUND)
    return root

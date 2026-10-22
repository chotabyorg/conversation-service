"""Consume the existing Inbox scope/receipts/proof protocol for visitor sessions."""

from dataclasses import dataclass
from uuid import UUID

from pydantic import ValidationError
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings
from core.errors import DomainError
from models import InboxGrantLifecycleReceipt, InboxGrantScope
from schemas.contracts import ErrorCode
from schemas.handoff import InboxGrantProvision, InboxGrantProvisioned
from schemas.website_session import WebsiteSessionScope
from services import inbox_lifecycle


@dataclass(frozen=True)
class WebsiteGrantEpoch:
    grant_generation: int
    agent_lifecycle_generation: int
    provision_command_id: UUID


async def lock_scope(
    session: AsyncSession,
    request: WebsiteSessionScope,
    *,
    shared: bool = False,
) -> InboxGrantScope:
    if not session.in_transaction():
        raise RuntimeError("Website grant access requires the caller transaction")
    values = dict(
        owner_id=request.owner_id,
        integration_id=request.integration_id,
        channel="WEBSITE_WIDGET",
        agent_id=request.agent_id,
    )
    if not shared:
        return await inbox_lifecycle._lock_scope(session, values)
    # FOR SHARE permits parallel visitors and conflicts with lifecycle writers.
    # Content reads never allocate or exclusively lock an integration scope.
    scope = await session.scalar(
        select(InboxGrantScope)
        .where(*[getattr(InboxGrantScope, key) == value for key, value in values.items()])
        .with_for_update(read=True)
        .execution_options(populate_existing=True)
    )
    if scope is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    return scope


async def current_epoch(
    session: AsyncSession,
    scope: InboxGrantScope,
    request: WebsiteSessionScope,
    settings: Settings,
    *,
    expected: WebsiteGrantEpoch | None = None,
) -> WebsiteGrantEpoch:
    """Caller retains the shared scope lock through root locking and protected use."""
    if not session.in_transaction():
        raise RuntimeError("Website grant access requires the caller transaction")
    if (
        not scope.active
        or scope.account_id is None
        or scope.agent_owner_id is None
        or scope.channel != "WEBSITE_WIDGET"
        or scope.owner_id != request.owner_id
        or scope.integration_id != request.integration_id
        or scope.agent_id != request.agent_id
        or scope.cp_binding_generation != request.binding_generation
        or scope.agent_lifecycle_generation is None
        or scope.grant_generation < 1
        or (
            scope.cp_binding_high_water is not None
            and request.binding_generation <= scope.cp_binding_high_water
        )
        or (
            scope.agent_lifecycle_high_water is not None
            and scope.agent_lifecycle_generation <= scope.agent_lifecycle_high_water
        )
        or (
            expected is not None
            and (
                expected.grant_generation != scope.grant_generation
                or expected.agent_lifecycle_generation != scope.agent_lifecycle_generation
            )
        )
    ):
        raise DomainError(ErrorCode.NOT_FOUND)
    statement = select(InboxGrantLifecycleReceipt).where(
        InboxGrantLifecycleReceipt.owner_id == request.owner_id,
        InboxGrantLifecycleReceipt.integration_id == request.integration_id,
        InboxGrantLifecycleReceipt.channel == "WEBSITE_WIDGET",
        InboxGrantLifecycleReceipt.agent_id == request.agent_id,
        InboxGrantLifecycleReceipt.kind == "PROVISION",
        InboxGrantLifecycleReceipt.acknowledgement["grant_generation"].astext
        == str(scope.grant_generation),
    )
    if expected is not None:
        statement = statement.where(
            InboxGrantLifecycleReceipt.command_id == expected.provision_command_id
        )
    receipt = await session.scalar(
        statement.order_by(
            InboxGrantLifecycleReceipt.created_at.desc(), InboxGrantLifecycleReceipt.id.desc()
        ).limit(1)
    )
    if receipt is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    try:
        ack = InboxGrantProvisioned.model_validate(receipt.acknowledgement)
    except (ValidationError, ValueError, TypeError):
        raise DomainError(ErrorCode.NOT_FOUND) from None
    if (
        ack.command_id != receipt.command_id
        or ack.owner_id != request.owner_id
        or ack.integration_id != request.integration_id
        or ack.channel != "WEBSITE_WIDGET"
        or ack.agent_id != request.agent_id
        or ack.account_id != scope.account_id
        or ack.agent_owner_id != scope.agent_owner_id
        or ack.binding_generation != request.binding_generation
        or ack.agent_lifecycle_generation != scope.agent_lifecycle_generation
        or ack.grant_generation != scope.grant_generation
    ):
        raise DomainError(ErrorCode.NOT_FOUND)
    proof_request = InboxGrantProvision(
        contract_version=1,
        command_id=receipt.command_id,
        owner_id=request.owner_id,
        integration_id=request.integration_id,
        channel="WEBSITE_WIDGET",
        agent_id=request.agent_id,
        expected_binding_generation=request.binding_generation,
    )
    if (
        ack.request_fingerprint != receipt.request_fingerprint
        or receipt.request_fingerprint != inbox_lifecycle._wire_fingerprint(proof_request)
    ):
        raise DomainError(ErrorCode.NOT_FOUND)
    try:
        cp, ai = await inbox_lifecycle._current_proofs(settings, proof_request)
    except DomainError as error:
        if error.code in {ErrorCode.NOT_FOUND, ErrorCode.GRANT_CONFLICT}:
            raise DomainError(ErrorCode.NOT_FOUND) from None
        raise
    if (
        cp.account_id != scope.account_id
        or ai.agent_owner_id != scope.agent_owner_id
        or cp.binding_generation != scope.cp_binding_generation
        or ai.agent_lifecycle_generation != scope.agent_lifecycle_generation
    ):
        raise DomainError(ErrorCode.NOT_FOUND)
    return WebsiteGrantEpoch(
        scope.grant_generation, scope.agent_lifecycle_generation, receipt.command_id
    )

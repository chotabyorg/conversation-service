"""Commit positive supervisor evidence to the existing outbound writer journal."""
from __future__ import annotations

import asyncio
from uuid import uuid5

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import Conversation, DeliveryAttempt, InboxGrantScope, WebsiteSessionCapability
from schemas.contracts import ErrorCode, Principal
from schemas.writer_retirement import (
    RetireCommand,
    TerminationEvidence,
    WebsiteWriterRetireCommand,
    WebsiteWriterTerminationProof,
    WriterRetired,
    WriterTerminationProof,
)
from services.common import db_clock, fingerprint
from services.delivery_ledger import _record_reconciliation
from services.inbox_lifecycle import _post_proof
from services.telegram_delivery_v2 import V2_NAMESPACE, _load_intent

WRITER_PROOF_DEADLINE_SECONDS = 8.0


def _authorize(principal: Principal, request: RetireCommand, settings: Settings) -> None:
    if (principal.principal_id != "delivery-writer-supervisor"
        or principal.role != "INBOX_LIFECYCLE"):
        raise DomainError(ErrorCode.NOT_FOUND)
    # Website recovery is authorized against retained scope/capability rows below,
    # including revoked/expired sessions and configuration rollback. It cannot
    # allocate a root, grant, send permission or lifecycle row.
    if not isinstance(request, WebsiteWriterRetireCommand) and (
        request.integration_id not in settings.handoff_cohort_integration_ids
        or not any(binding.owner_id == request.owner_id
                   and binding.integration_id == request.integration_id
                   and binding.agent_id == request.agent_id
                   and binding.channel.value == "TELEGRAM"
                   for binding in settings.managed_ai_bindings)):
        raise DomainError(ErrorCode.NOT_FOUND)
    if settings.writer_supervisor_base_url is None or settings.writer_supervisor_token is None:
        raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)


async def _load_writer(session: AsyncSession, principal: Principal,
                       settings: Settings, request: RetireCommand
                       ) -> tuple[InboxGrantScope, Conversation, DeliveryAttempt]:
    if not isinstance(request, WebsiteWriterRetireCommand):
        tg_grant, tg_root, tg_intent, _ = await _load_intent(session, principal, settings, request)
        return tg_grant, tg_root, tg_intent
    # Match existing retained authority first; unlike provisioning this path must
    # never create a scope for a guessed or no-longer-configured integration.
    grant = await session.scalar(select(InboxGrantScope).filter_by(
        owner_id=request.owner_id, integration_id=request.integration_id,
        channel="WEBSITE_WIDGET", agent_id=request.agent_id).with_for_update()
        .execution_options(populate_existing=True))
    if grant is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    root = await session.scalar(select(Conversation).filter_by(id=request.conversation_id,
        owner_id=request.owner_id, integration_id=request.integration_id,
        channel="WEBSITE_WIDGET", provider_thread_id=str(request.session_id)).with_for_update()
        .execution_options(populate_existing=True))
    if root is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    capability = await session.scalar(select(WebsiteSessionCapability).filter_by(
        conversation_id=root.id, owner_id=request.owner_id, integration_id=request.integration_id,
        channel="WEBSITE_WIDGET", provider_thread_id=str(request.session_id),
        agent_id=request.agent_id, binding_generation=request.binding_generation))
    if capability is None or capability.provision_command_id is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    from services.website_delivery import _load

    _grant, intent, _ = await _load(session, root, request, request.agent_id)
    return grant, root, intent


async def _retained(
    session: AsyncSession, intent: DeliveryAttempt, request: RetireCommand, digest: str,
) -> WriterRetired | None:
    existing = await session.scalar(select(DeliveryAttempt.id).where(
        DeliveryAttempt.conversation_id == intent.conversation_id,
        DeliveryAttempt.writer_retirement_command_id == request.command_id,
    ))
    if existing is not None and existing != intent.id:
        raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
    if intent.writer_retirement_command_id is not None:
        if (intent.writer_retirement_command_id != request.command_id
            or intent.writer_retirement_fingerprint != digest
            or intent.writer_retirement_receipt is None):
            raise DomainError(ErrorCode.DELIVERY_RECONCILIATION_CONFLICT)
        value = WriterRetired.model_validate(intent.writer_retirement_receipt["result"])
        return value.model_copy(update={"replayed": True})
    return None


def _eligible(intent: DeliveryAttempt, request: RetireCommand) -> None:
    if (intent.writer_state != "LIVE"
        or intent.writer_supplier_instance_id != request.supplier_instance_id
        or intent.writer_process_incarnation_id != request.process_incarnation_id):
        raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
    if intent.revision != request.expected_delivery_revision:
        raise DomainError(ErrorCode.DELIVERY_REVISION_CONFLICT)


async def _termination_proof(
    request: RetireCommand, digest: str, settings: Settings,
) -> TerminationEvidence:
    if settings.writer_supervisor_base_url is None or settings.writer_supervisor_token is None:
        raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
    try:
        async with asyncio.timeout(WRITER_PROOF_DEADLINE_SECONDS):
            proof_type = (WebsiteWriterTerminationProof
                          if isinstance(request, WebsiteWriterRetireCommand)
                          else WriterTerminationProof)
            proof = await _post_proof(settings.writer_supervisor_base_url,
                "/internal/v1/writer-termination/proof",
                {"X-Writer-Supervisor-Token": settings.writer_supervisor_token.get_secret_value()},
                request, proof_type)
    except TimeoutError:
        raise DomainError(ErrorCode.SERVICE_UNAVAILABLE) from None
    if (proof.request_fingerprint != digest or any(
        proof.model_dump(mode="json").get(key) != value
        for key, value in request.model_dump(mode="json").items()
    )):
        raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
    return proof


async def retire_writer(
    database: Database, principal: Principal, request: RetireCommand, settings: Settings,
) -> WriterRetired:
    _authorize(principal, request, settings)
    digest = fingerprint(request.model_dump(mode="json"))
    # Replay precedes external dependencies and current delivery revision checks.
    async with database.session_factory() as session, session.begin():
        _, _, intent = await _load_writer(session, principal, settings, request)
        retained = await _retained(session, intent, request, digest)
        if retained is not None:
            return retained
        _eligible(intent, request)

    # A proof is read-only, immutable end/fencing evidence. No DB checkout or
    # Conversation lock is held while the supervisor is contacted.
    proof = await _termination_proof(request, digest, settings)

    async with database.session_factory() as session, session.begin():
        _, conversation, intent = await _load_writer(session, principal, settings, request)
        retained = await _retained(session, intent, request, digest)
        if retained is not None:
            return retained
        _eligible(intent, request)
        clock = await db_clock(session)
        if intent.status == "DISPATCH_REQUESTED":
            _record_reconciliation(session, intent, conversation.id, "UNKNOWN",
                key=uuid5(V2_NAMESPACE, f"writer-retired:{request.command_id}"),
                expected_revision=intent.revision, error_code="TRANSPORT_ERROR", clock=clock)
        elif intent.status not in {"UNKNOWN", "FAILED", "PROVIDER_ACCEPTED"}:
            raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
        intent.writer_state = "RETIRED"
        intent.writer_ended_at = clock
        result = WriterRetired(contract_version=1, command_id=request.command_id,
            outbound_intent_id=intent.id, response_attempt_id=request.response_attempt_id,
            supplier_instance_id=request.supplier_instance_id,
            process_incarnation_id=request.process_incarnation_id, proof_ref=request.proof_ref,
            proof_fingerprint=fingerprint(proof.model_dump(mode="json")),
            writer_state="RETIRED", retired_at=clock, replayed=False)
        intent.writer_retirement_command_id = request.command_id
        intent.writer_retirement_fingerprint = digest
        intent.writer_retirement_receipt = {
            "result": result.model_dump(mode="json"), "proof": proof.model_dump(mode="json"),
        }
        await session.flush()
        return result

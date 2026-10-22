"""Credential Website adapter over the common ordered intent and writer ledger."""
from typing import Literal, cast
from uuid import UUID, uuid5

from sqlalchemy import exists, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import AgentTurn, Conversation, DeliveryAttempt, InboxGrantScope
from schemas.contracts import Contract, ErrorCode, Principal
from schemas.website_delivery import (
    WebsiteDeliveryBegin,
    WebsiteDeliveryBegun,
    WebsiteDeliveryComplete,
    WebsiteDeliveryCompleted,
    WebsiteDeliveryDescriptor,
    WebsiteDeliveryRecovery,
    WebsiteDeliveryWorkResult,
    WebsitePublicFrame,
)
from schemas.website_session import WebsiteSessionAccess
from services import website_session
from services.common import db_clock
from services.delivery_ledger import _record_reconciliation
from services.text_delivery import (
    Source,
    WriterCompletionEvidence,
    _current_catch_up_source,
    _ensure_agent_intent,
    _settling_fence,
    _source,
    _valid_authority,
    begin_writer,
    complete_writer,
    lower_pending,
)

# These fixed derivations are retained transport identities, never new per poll.
ATTEMPT_NAMESPACE = UUID("601fc120-cc55-46ac-88d8-f83ccf4c5da3")
PUBLIC_NAMESPACE = UUID("5b4eef3a-5bfa-4a17-a628-d1672878552a")
MAX_FRAME_BYTES = 65_536
MAX_RECOVERY_BYTES = 4_096


def _key(intent_id: UUID, status: str, reference: str = "") -> UUID:
    return uuid5(ATTEMPT_NAMESPACE, f"{intent_id}:{status}:{reference}")


def _descriptor(intent: DeliveryAttempt, source: Source) -> WebsiteDeliveryDescriptor:
    if intent.outbound_ordinal is None:
        raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
    return WebsiteDeliveryDescriptor(outbound_intent_id=intent.id,
        outbound_ordinal=intent.outbound_ordinal, source_kind=source[0], source_id=source[1],
        response_attempt_id=uuid5(ATTEMPT_NAMESPACE, str(intent.id)),
        delivery_id=uuid5(PUBLIC_NAMESPACE, str(intent.id)))


def _recovery(intent: DeliveryAttempt) -> WebsiteDeliveryRecovery:
    statuses = dict(GENERATED="PENDING", DISPATCH_REQUESTED="WRITE_IN_PROGRESS",
        PROVIDER_ACCEPTED="TRANSPORT_ACCEPTED", FAILED="FAILED", UNKNOWN="UNKNOWN",
        SUPERSEDED="SUPERSEDED")
    if intent.status not in statuses:
        raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
    return WebsiteDeliveryRecovery.model_validate(dict(
        delivery_id=uuid5(PUBLIC_NAMESPACE, str(intent.id)), status=statuses[intent.status],
        writer_state=intent.writer_state))


def _frame(intent: DeliveryAttempt, source: Source) -> WebsitePublicFrame:
    return WebsitePublicFrame(delivery_id=uuid5(PUBLIC_NAMESPACE, str(intent.id)),
        actor="AGENT" if source[0] == "AGENT_TURN" else "HUMAN", text=source[2])


def _fits(value: Contract, maximum: int = MAX_FRAME_BYTES) -> bool:
    return len(value.model_dump_json().encode("utf-8")) <= maximum


async def _grant(session: AsyncSession, root: Conversation, agent_id: UUID) -> InboxGrantScope:
    # The caller already holds this scope before its root, including late evidence.
    grant = await session.scalar(select(InboxGrantScope).filter_by(owner_id=root.owner_id,
        integration_id=root.integration_id, channel="WEBSITE_WIDGET", agent_id=agent_id))
    if grant is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    return grant


async def _supersede(session: AsyncSession, intent: DeliveryAttempt,
                     root: Conversation, source: Source) -> None:
    expected, fenced = _settling_fence(source, root)
    _record_reconciliation(session, intent, root.id, "SUPERSEDED",
        key=_key(intent.id, "SUPERSEDED"), expected_revision=intent.revision,
        expected_settling_revision=expected, result_settling_revision=fenced,
        clock=await db_clock(session))


async def work(database: Database, principal: Principal, request: WebsiteSessionAccess,
               settings: Settings) -> WebsiteDeliveryWorkResult:
    """Discover at most one frame; discovery never acquires a writer."""
    async with database.session_factory() as session, session.begin():
        root = await website_session.locked_access(session, principal, request, settings)
        grant = await _grant(session, root, request.agent_id)
        turns = await session.scalars(select(AgentTurn).where(
            AgentTurn.conversation_id == root.id, AgentTurn.managed_contract_version == 3,
            AgentTurn.managed_projection_agent_id == request.agent_id,
            AgentTurn.status == "COMPLETED", AgentTurn.outcome == "DRAFT_READY",
            AgentTurn.draft_text.is_not(None), AgentTurn.retired_at.is_(None),
            AgentTurn.authority_revision == root.authority_revision,
            ~exists(select(DeliveryAttempt.id).where(
                DeliveryAttempt.source_turn_id == AgentTurn.id)),
        ).order_by(AgentTurn.created_at, AgentTurn.id).limit(100).with_for_update())
        for turn in turns:
            if root.control_mode == "AGENT":
                await _ensure_agent_intent(session, root, turn, grant, await db_clock(session))
        intents = await session.scalars(select(DeliveryAttempt).where(
            DeliveryAttempt.conversation_id == root.id,
            DeliveryAttempt.outbound_ordinal.is_not(None),
            DeliveryAttempt.writer_state.in_(("NONE", "LIVE")),
            DeliveryAttempt.status.in_(("GENERATED", "DISPATCH_REQUESTED", "UNKNOWN")),
        ).order_by(DeliveryAttempt.outbound_ordinal).limit(100).with_for_update())
        for intent in intents:
            source = await _source(session, intent, root, grant, request.agent_id)
            if source is None:
                # An invalid predecessor remains a barrier, never skip its evidence.
                return WebsiteDeliveryWorkResult(state="EMPTY")
            descriptor = _descriptor(intent, source)
            if intent.writer_state == "LIVE":
                result = WebsiteDeliveryWorkResult(state="RECOVER_ONLY", item=descriptor,
                    recovery=_recovery(intent))
                assert _fits(result, MAX_RECOVERY_BYTES)
                return result
            if (not _valid_authority(intent, source, root, grant)
                    or not await _current_catch_up_source(session, source, root)):
                await _supersede(session, intent, root, source)
                continue
            if await lower_pending(session, intent):
                return WebsiteDeliveryWorkResult(state="EMPTY")
            result = WebsiteDeliveryWorkResult(state="SEND_CANDIDATE", item=descriptor,
                                                frame=_frame(intent, source))
            if not _fits(result):
                return WebsiteDeliveryWorkResult(state="FRAME_TOO_LARGE", item=descriptor,
                    recovery=_recovery(intent))
            return result
        return WebsiteDeliveryWorkResult(state="EMPTY")


async def _load(
    session: AsyncSession, root: Conversation, request: WebsiteDeliveryBegin,
) -> tuple[InboxGrantScope, DeliveryAttempt, Source]:
    grant = await _grant(session, root, request.agent_id)
    intent = await session.scalar(select(DeliveryAttempt).where(
        DeliveryAttempt.conversation_id == root.id,
        DeliveryAttempt.id == request.outbound_intent_id,
        DeliveryAttempt.outbound_ordinal == request.outbound_ordinal,
    ).with_for_update().execution_options(populate_existing=True))
    if intent is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    source = await _source(session, intent, root, grant, request.agent_id)
    if source is None:
        raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
    descriptor = _descriptor(intent, source)
    if any(getattr(request, field) != getattr(descriptor, field)
            for field in WebsiteDeliveryDescriptor.model_fields):
        raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
    return grant, intent, source


async def begin(database: Database, principal: Principal, request: WebsiteDeliveryBegin,
                settings: Settings) -> WebsiteDeliveryBegun:
    async with database.session_factory() as session, session.begin():
        root = await website_session.locked_access(session, principal, request, settings)
        grant, intent, source = await _load(session, root, request)
        descriptor = _descriptor(intent, source)
        current = False
        if (not request.recover_only and intent.writer_state == "NONE"
                and intent.status == "GENERATED"):
            candidate = WebsiteDeliveryBegun(state="ACQUIRED", item=descriptor, send=True,
                replayed=False, recovery=_recovery(intent).model_copy(update={
                    "status": "WRITE_IN_PROGRESS", "writer_state": "LIVE"}),
                frame=_frame(intent, source))
            if not _fits(candidate):
                return WebsiteDeliveryBegun(state="FRAME_TOO_LARGE", item=descriptor,
                    send=False, replayed=False, recovery=_recovery(intent))
            current = (_valid_authority(intent, source, root, grant)
                and await _current_catch_up_source(session, source, root))
        decision = await begin_writer(session, intent, root, source,
            recover_only=request.recover_only, current=current,
            supplier_instance_id=request.supplier_instance_id,
            process_incarnation_id=request.process_incarnation_id,
            superseded_key=_key(intent.id, "SUPERSEDED"),
            dispatch_key=_key(intent.id, "DISPATCH_REQUESTED"))
        state = "ACQUIRED" if decision.send else "RECOVER_ONLY" if decision.replayed else "NO_SEND"
        result = WebsiteDeliveryBegun.model_validate(dict(state=state, item=descriptor,
            send=decision.send, replayed=decision.replayed, recovery=_recovery(intent),
            frame=_frame(intent, source) if decision.send else None))
        assert _fits(result, MAX_FRAME_BYTES if decision.send else MAX_RECOVERY_BYTES)
        return result


async def complete(database: Database, principal: Principal,
                   request: WebsiteDeliveryComplete) -> WebsiteDeliveryCompleted:
    async with database.session_factory() as session, session.begin():
        root = await website_session.locked_original_writer(session, principal, request)
        _grant_value, intent, source = await _load(session, root, request)
        outcome = {"WRITE_COMPLETED": "PROVIDER_ACCEPTED", "WRITE_FAILED": "FAILED",
                    "UNKNOWN": "UNKNOWN"}[request.outcome]
        provider_id = str(request.delivery_id) if request.outcome == "WRITE_COMPLETED" else None
        error = "PROVIDER_REJECTED" if request.outcome == "WRITE_FAILED" else request.error_code
        # DTO validation already binds positive task join to this original identity.
        normalized = WriterCompletionEvidence(request.supplier_instance_id,
            request.process_incarnation_id, cast(Literal["PROVIDER_ACCEPTED", "FAILED", "UNKNOWN"],
            outcome), provider_id, error, request.writer_end is not None)
        decision = await complete_writer(session, intent, root, source, normalized,
            recover_only=request.recover_only,
            reconciliation_key=_key(intent.id, outcome, provider_id or error or ""))
        result = WebsiteDeliveryCompleted(recovery=_recovery(intent), replayed=decision.replayed)
        assert _fits(result, MAX_RECOVERY_BYTES)
        return result

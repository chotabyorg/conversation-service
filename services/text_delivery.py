"""Common text intent/source, authority and catch-up rules on the existing ledger."""
from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Literal, TypeAlias
from uuid import UUID, uuid4

from sqlalchemy import and_, or_, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.errors import DomainError
from models import AgentTurn, Conversation, DeliveryAttempt, HandoffCase, InboxGrantScope, Message
from schemas.contracts import ErrorCode
from services import catch_up
from services.common import db_clock, fingerprint
from services.delivery_ledger import _record_reconciliation

AgentSource: TypeAlias = tuple[Literal["AGENT_TURN"], UUID, str, None, AgentTurn]
HumanSource: TypeAlias = tuple[Literal["OUTBOUND_MESSAGE"], UUID, str, HandoffCase, Message]
Source: TypeAlias = AgentSource | HumanSource


async def lower_pending(session: AsyncSession, intent: DeliveryAttempt) -> bool:
    """Only positive termination clears an acquired predecessor in the FIFO."""
    if intent.outbound_ordinal is None:
        raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
    return await session.scalar(select(DeliveryAttempt.id).where(
        DeliveryAttempt.conversation_id == intent.conversation_id,
        or_(DeliveryAttempt.writer_state == "LIVE", and_(
            DeliveryAttempt.outbound_ordinal < intent.outbound_ordinal,
            DeliveryAttempt.outbound_ordinal.is_not(None),
            DeliveryAttempt.writer_state == "NONE", DeliveryAttempt.status == "GENERATED")),
    ).limit(1)) is not None


@dataclass(frozen=True)
class WriterAcquisition:
    send: bool
    replayed: bool


@dataclass(frozen=True)
class WriterCompletionEvidence:
    supplier_instance_id: UUID
    process_incarnation_id: UUID
    outcome: Literal["PROVIDER_ACCEPTED", "FAILED", "UNKNOWN"]
    provider_message_id: str | None
    error_code: str | None
    task_joined: bool


@dataclass(frozen=True)
class WriterCompletion:
    outcome: Literal["PROVIDER_ACCEPTED", "FAILED", "UNKNOWN"]
    replayed: bool


async def complete_writer(
    session: AsyncSession, intent: DeliveryAttempt, conversation: Conversation, source: Source,
    evidence: WriterCompletionEvidence, *, recover_only: bool, reconciliation_key: UUID,
) -> WriterCompletion:
    """Retain original outcome evidence independently from positive writer end."""
    if (intent.writer_supplier_instance_id != evidence.supplier_instance_id
            or intent.writer_process_incarnation_id != evidence.process_incarnation_id
            or intent.writer_state not in {"LIVE", "ENDED", "RETIRED"}):
        raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
    if (not recover_only and intent.writer_state == "LIVE"
            and intent.status not in {"DISPATCH_REQUESTED", "UNKNOWN"}):
        raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
    same = (intent.status == evidence.outcome
        and intent.provider_message_id == evidence.provider_message_id
        and intent.error_code == evidence.error_code)
    if intent.status in {"PROVIDER_ACCEPTED", "FAILED", "UNKNOWN"} and not same:
        if intent.status != "UNKNOWN" or evidence.outcome == "UNKNOWN":
            raise DomainError(ErrorCode.DELIVERY_RECONCILIATION_CONFLICT)
    if not same:
        if intent.status not in {"DISPATCH_REQUESTED", "UNKNOWN"}:
            raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
        clock = await db_clock(session)
        _record_reconciliation(session, intent, conversation.id, evidence.outcome,
            provider_message_id=evidence.provider_message_id, error_code=evidence.error_code,
            key=reconciliation_key, expected_revision=intent.revision, clock=clock)
        if evidence.outcome == "PROVIDER_ACCEPTED" and source[0] == "AGENT_TURN":
            turn = source[4]
            existing = await session.scalar(select(Message.id).where(
                Message.conversation_id == conversation.id,
                Message.source_agent_turn_id == turn.id))
            if existing is None:
                conversation.last_message_sequence += 1
                conversation.updated_at = clock
                session.add(Message(id=uuid4(), conversation_id=conversation.id,
                    sequence=conversation.last_message_sequence,
                    actor="AGENT", direction="OUTBOUND",
                    content_type="TEXT", text=source[2], source_agent_turn_id=turn.id,
                    created_at=clock))
    ended = intent.writer_state in {"ENDED", "RETIRED"}
    if evidence.task_joined:
        if intent.writer_state == "LIVE":
            intent.writer_state, intent.writer_ended_at = "ENDED", await db_clock(session)
            ended = True
        elif not ended:
            raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
    await session.flush()
    return WriterCompletion(outcome=evidence.outcome,
        replayed=same and (not evidence.task_joined or ended))


async def begin_writer(
    session: AsyncSession, intent: DeliveryAttempt, conversation: Conversation, source: Source,
    *, recover_only: bool, current: bool, supplier_instance_id: UUID,
    process_incarnation_id: UUID, superseded_key: UUID, dispatch_key: UUID,
) -> WriterAcquisition:
    """Acquire exactly one writer while the caller holds scope/root/intent locks."""
    if recover_only or intent.writer_state != "NONE" or intent.status != "GENERATED":
        return WriterAcquisition(send=False, replayed=True)
    if not current:
        expected, fenced = _settling_fence(source, conversation)
        _record_reconciliation(session, intent, conversation.id, "SUPERSEDED",
            key=superseded_key, expected_revision=intent.revision,
            expected_settling_revision=expected, result_settling_revision=fenced,
            clock=await db_clock(session))
        return WriterAcquisition(send=False, replayed=False)
    if await lower_pending(session, intent):
        return WriterAcquisition(send=False, replayed=False)
    clock = await db_clock(session)
    intent.writer_state, intent.writer_started_at = "LIVE", clock
    intent.writer_supplier_instance_id = supplier_instance_id
    intent.writer_process_incarnation_id = process_incarnation_id
    expected, fenced = _settling_fence(source, conversation)
    _record_reconciliation(session, intent, conversation.id, "DISPATCH_REQUESTED",
        key=dispatch_key, expected_revision=intent.revision,
        authority_revision=intent.authority_revision,
        expected_settling_revision=expected, result_settling_revision=fenced, clock=clock)
    await session.flush()
    return WriterAcquisition(send=True, replayed=False)



async def _source(
    session: AsyncSession,
    intent: DeliveryAttempt,
    conversation: Conversation,
    scope: InboxGrantScope,
    agent_id: UUID,
) -> Source | None:
    if intent.source_turn_id is not None:
        turn = await session.scalar(
            select(AgentTurn)
            .where(
                AgentTurn.id == intent.source_turn_id,
                AgentTurn.conversation_id == conversation.id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if (
            turn is None
            or turn.managed_contract_version != 3
            or turn.managed_projection_agent_id != agent_id
            or scope.owner_id != conversation.owner_id
            or scope.integration_id != conversation.integration_id
            or scope.channel != conversation.channel
            or scope.agent_id != agent_id
        ):
            return None
        if turn.status != "COMPLETED" or turn.outcome != "DRAFT_READY" or turn.draft_text is None:
            return None
        return ("AGENT_TURN", turn.id, turn.draft_text, None, turn)
    if intent.source_message_id is None:
        return None
    message = await session.scalar(
        select(Message)
        .where(
            Message.id == intent.source_message_id,
            Message.conversation_id == conversation.id,
            Message.actor == "HUMAN",
            Message.direction == "OUTBOUND",
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if message is None or intent.case_id is None:
        return None
    case = await session.scalar(
        select(HandoffCase)
        .where(
            HandoffCase.id == intent.case_id,
            HandoffCase.conversation_id == conversation.id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if (
        case is None
        or case.owner_id != conversation.owner_id
        or case.integration_id != conversation.integration_id
        or case.channel != conversation.channel
        or case.agent_id != agent_id
        or scope.owner_id != conversation.owner_id
        or scope.integration_id != conversation.integration_id
        or scope.channel != conversation.channel
        or scope.agent_id != agent_id
    ):
        return None
    return ("OUTBOUND_MESSAGE", message.id, message.text, case, message)


async def _current_catch_up_source(
    session: AsyncSession,
    source: Source,
    conversation: Conversation,
) -> bool:
    return (
        source[0] != "AGENT_TURN"
        or not source[4].catch_up_version
        or await catch_up.communication_current(session, conversation, source[4])
    )


async def _ensure_agent_intent(
    session: AsyncSession,
    conversation: Conversation,
    turn: AgentTurn,
    scope: InboxGrantScope,
    clock: datetime,
) -> DeliveryAttempt:
    current = await session.scalar(
        select(DeliveryAttempt)
        .where(
            DeliveryAttempt.source_turn_id == turn.id,
            DeliveryAttempt.conversation_id == conversation.id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if current is not None:
        return current
    ordinal = await session.scalar(
        select(DeliveryAttempt.outbound_ordinal)
        .where(
            DeliveryAttempt.conversation_id == conversation.id,
            DeliveryAttempt.outbound_ordinal.is_not(None),
        )
        .order_by(DeliveryAttempt.outbound_ordinal.desc())
        .limit(1)
    )
    intent_id = uuid4()
    current = DeliveryAttempt(
        id=intent_id,
        conversation_id=conversation.id,
        delivery_key=turn.id,
        source_turn_id=turn.id,
        source_message_id=None,
        outbound_ordinal=(ordinal or 0) + 1,
        grant_generation=scope.grant_generation,
        creation_fingerprint=fingerprint(
            {
                "contract_version": 2,
                "source_kind": "AGENT_TURN",
                "source_id": str(turn.id),
                "authority_revision": turn.authority_revision,
            }
        ),
        authority_revision=turn.authority_revision,
        expected_settling_revision=(
            turn.settling_revision if turn.settling_burst_id is not None else None
        ),
        fenced_settling_revision=(
            conversation.settling_revision if turn.settling_burst_id is not None else None
        ),
        writer_state="NONE",
        status="GENERATED",
        revision=0,
        created_at=clock,
        updated_at=clock,
    )
    session.add(current)
    await session.flush()
    return current


def _settling_fence(source: Source, conversation: Conversation) -> tuple[int | None, int | None]:
    if source[0] != "AGENT_TURN":
        return None, None
    turn = source[4]
    if turn.settling_burst_id is None:
        return None, None
    return turn.settling_revision, conversation.settling_revision


def _valid_authority(
    intent: DeliveryAttempt,
    source: Source,
    conversation: Conversation,
    scope: InboxGrantScope,
) -> bool:
    if (
        not scope.active
        or scope.agent_id is None
        or scope.grant_generation < 1
    ):
        return False
    if source[0] == "AGENT_TURN":
        turn = source[4]
        settling_stale = (
            turn.settling_burst_id is not None
            and conversation.settling_revision > turn.settling_revision
        )
        return (
            conversation.control_mode == "AGENT"
            and conversation.authority_revision == turn.authority_revision
            and conversation.input_revision == turn.input_revision
            and turn.authority_revision == intent.authority_revision
            and intent.grant_generation == scope.grant_generation
            and turn.retired_at is None
            and not settling_stale
            and (
                conversation.handoff_resume_floor_sequence is None
                or turn.input_first_sequence > conversation.handoff_resume_floor_sequence
            )
        )
    case = source[3]
    return (
        conversation.control_mode == "HUMAN"
        and case.status == "CLAIMED"
        and case.assigned_account_id is not None
        and case.authority_revision == conversation.authority_revision
        and intent.authority_revision == conversation.authority_revision
        and case.assignment_revision == intent.assignment_revision
        and intent.grant_generation == scope.grant_generation
    )

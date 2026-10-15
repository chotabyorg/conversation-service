"""Owner-only Inbox projection and atomic case commands."""

from __future__ import annotations

import base64
import binascii
import re
from typing import Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.errors import DomainError
from db.database import Database
from models import (
    Conversation,
    DeliveryAttempt,
    HandoffCase,
    InboxCommandReceipt,
    InboxGrantScope,
    Message,
)
from schemas.contracts import ErrorCode
from schemas.handoff import (
    CaseDetail,
    CasePage,
    CaseSummary,
    ClaimCommand,
    CommandReceipt,
    HumanPrincipal,
    InboxCommandResult,
    MessageView,
    ReturnToAgentCommand,
    SendHumanCommand,
)
from services.common import db_clock, fingerprint

CURSOR_BYTES = 16


def _cursor(value: str | None) -> UUID | None:
    if value is None:
        return None
    if len(value) > 64 or re.fullmatch(r"[A-Za-z0-9_-]+", value) is None:
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    try:
        raw = base64.urlsafe_b64decode(value + "=" * (-len(value) % 4))
        if len(raw) != CURSOR_BYTES:
            raise ValueError
        return UUID(bytes=raw)
    except (ValueError, binascii.Error):
        raise DomainError(ErrorCode.VALIDATION_ERROR) from None


def encode_cursor(case_id: UUID) -> str:
    return base64.urlsafe_b64encode(case_id.bytes).decode("ascii").rstrip("=")


def _summary(case: HandoffCase, conversation: Conversation) -> CaseSummary:
    return CaseSummary(
        case_id=case.id,
        conversation_id=conversation.id,
        owner_id=case.owner_id,
        integration_id=case.integration_id,
        channel=cast(Literal["TELEGRAM"], case.channel),
        provider_thread_id=conversation.provider_thread_id,
        status=cast(Literal["OPEN", "CLAIMED", "RETURNED"], case.status),
        case_revision=case.case_revision,
        authority_revision=case.authority_revision,
        assignment_revision=case.assignment_revision,
        assigned_account_id=case.assigned_account_id,
        reason=cast(
            Literal[
                "CUSTOMER_REQUEST",
                "AGENT_LIMIT",
                "BUSINESS_UNCERTAIN",
                "POLICY_REQUIRES_HUMAN",
                "OTHER",
            ],
            case.reason,
        ),
        note=case.note,
        created_at=case.created_at,
        updated_at=case.updated_at,
    )


async def list_cases(
    database: Database,
    principal: HumanPrincipal,
    limit: int,
    cursor: str | None,
) -> CasePage:
    if not 1 <= limit <= 50:
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    after = _cursor(cursor)
    async with database.session_factory() as session:
        statement = (
            select(HandoffCase, Conversation)
            .join(Conversation, Conversation.id == HandoffCase.conversation_id)
            .join(
                InboxGrantScope,
                (
                    (InboxGrantScope.owner_id == HandoffCase.owner_id)
                    & (InboxGrantScope.integration_id == HandoffCase.integration_id)
                    & (InboxGrantScope.channel == HandoffCase.channel)
                    & (InboxGrantScope.agent_id == HandoffCase.agent_id)
                ),
            )
            .where(
                InboxGrantScope.active.is_(True),
                InboxGrantScope.account_id == principal.account_id,
                HandoffCase.status.in_(("OPEN", "CLAIMED")),
            )
        )
        if after is not None:
            statement = statement.where(HandoffCase.id > after)
        rows = (await session.execute(statement.order_by(HandoffCase.id).limit(limit + 1))).all()
        has_more = len(rows) > limit
        rows = rows[:limit]
        return CasePage(
            items=[_summary(case, conversation) for case, conversation in rows],
            next_cursor=encode_cursor(rows[-1][0].id) if has_more and rows else None,
        )


async def _authorized_case(
    session: AsyncSession,
    principal: HumanPrincipal,
    case_id: UUID,
    *,
    lock: bool,
) -> tuple[HandoffCase, Conversation, InboxGrantScope]:
    discovered = await session.scalar(select(HandoffCase).where(HandoffCase.id == case_id))
    if discovered is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    grant = await session.scalar(
        select(InboxGrantScope)
        .where(
            InboxGrantScope.owner_id == discovered.owner_id,
            InboxGrantScope.integration_id == discovered.integration_id,
            InboxGrantScope.channel == discovered.channel,
            InboxGrantScope.agent_id == discovered.agent_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
        if lock
        else select(InboxGrantScope).where(
            InboxGrantScope.owner_id == discovered.owner_id,
            InboxGrantScope.integration_id == discovered.integration_id,
            InboxGrantScope.channel == discovered.channel,
            InboxGrantScope.agent_id == discovered.agent_id,
        )
    )
    if grant is None or not grant.active or grant.account_id != principal.account_id:
        raise DomainError(ErrorCode.NOT_FOUND)
    conversation_statement = select(Conversation).where(
        Conversation.id == discovered.conversation_id,
        Conversation.owner_id == discovered.owner_id,
        Conversation.integration_id == discovered.integration_id,
        Conversation.channel == discovered.channel,
    )
    if lock:
        conversation_statement = conversation_statement.with_for_update().execution_options(
            populate_existing=True
        )
    conversation = await session.scalar(conversation_statement)
    if conversation is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    case_statement = select(HandoffCase).where(
        HandoffCase.id == case_id,
        HandoffCase.conversation_id == conversation.id,
    )
    if lock:
        case_statement = case_statement.with_for_update().execution_options(populate_existing=True)
    case = await session.scalar(case_statement)
    if case is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    return case, conversation, grant


async def case_detail(
    database: Database,
    principal: HumanPrincipal,
    case_id: UUID,
    after_sequence: int,
    limit: int,
) -> CaseDetail:
    if after_sequence < 0 or not 1 <= limit <= 100:
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    # Keep the grant row lock until the projection is complete. Revocation takes
    # the same scope lock first, so a detail read is ordered either before a
    # revoke (and may return its pre-revoke snapshot) or after it (and returns
    # not-found); it cannot validate ownership and then read history after the
    # revoke has committed.
    async with database.session_factory() as session, session.begin():
        case, conversation, _ = await _authorized_case(session, principal, case_id, lock=True)
        rows = (
            await session.scalars(
                select(Message)
                .where(
                    Message.conversation_id == conversation.id,
                    Message.sequence > after_sequence,
                )
                .order_by(Message.sequence)
                .limit(limit + 1)
            )
        ).all()
        has_more = len(rows) > limit
        rows = list(rows[:limit])
        views: list[MessageView] = []
        for message in rows:
            attempt = await session.scalar(
                select(DeliveryAttempt).where(
                    DeliveryAttempt.source_message_id == message.id,
                    DeliveryAttempt.conversation_id == conversation.id,
                )
            )
            state: Literal[
                "PENDING", "PROVIDER_ACCEPTED", "FAILED", "UNKNOWN", "SUPERSEDED"
            ] | None = None
            blocked = False
            provider_id = None
            if attempt is not None:
                state = cast(
                    Literal[
                        "PENDING", "PROVIDER_ACCEPTED", "FAILED", "UNKNOWN", "SUPERSEDED"
                    ]
                    | None,
                    {
                    "GENERATED": "PENDING",
                    "DISPATCH_REQUESTED": "PENDING",
                    "SUPERSEDED": "SUPERSEDED",
                    "PROVIDER_ACCEPTED": "PROVIDER_ACCEPTED",
                    "DELIVERED": "PROVIDER_ACCEPTED",
                    "READ": "PROVIDER_ACCEPTED",
                    "FAILED": "FAILED",
                    "UNKNOWN": "UNKNOWN",
                    "PARTIAL": "UNKNOWN",
                    "INTERRUPTED": "UNKNOWN",
                    }.get(attempt.status),
                )
                provider_id = attempt.provider_message_id
                if attempt.outbound_ordinal is not None:
                    blocked = bool(
                        await session.scalar(
                            select(DeliveryAttempt.id)
                            .where(
                                DeliveryAttempt.conversation_id == conversation.id,
                                DeliveryAttempt.outbound_ordinal < attempt.outbound_ordinal,
                                DeliveryAttempt.writer_state == "LIVE",
                            )
                            .limit(1)
                        )
                    )
            views.append(
                MessageView(
                    message_id=message.id,
                    sequence=message.sequence,
                    actor=cast(Literal["CUSTOMER", "AGENT", "HUMAN", "SYSTEM"], message.actor),
                    direction=cast(Literal["INBOUND", "OUTBOUND"], message.direction),
                    content_type=cast(Literal["TEXT"], message.content_type),
                    text=message.text,
                    delivery_state=state,
                    provider_message_id=provider_id,
                    delivery_blocked_by_live_writer=blocked,
                )
            )
        return CaseDetail(
            case=_summary(case, conversation),
            messages=views,
            next_after_sequence=rows[-1].sequence if has_more and rows else None,
        )


def _receipt_model(
    row: InboxCommandReceipt,
    *,
    replayed: bool,
) -> CommandReceipt:
    return CommandReceipt(
        command_id=row.command_id,
        kind=cast(Literal["CLAIM", "SEND_HUMAN", "RETURN_TO_AGENT"], row.kind),
        case_id=row.case_id,
        applied_case_revision=row.applied_case_revision,
        applied_authority_revision=row.applied_authority_revision,
        applied_assignment_revision=row.applied_assignment_revision,
        message_id=row.message_id,
        outbound_intent_id=row.outbound_intent_id,
        replayed=replayed,
    )


async def _existing_command(
    session: AsyncSession,
    case: HandoffCase,
    principal: HumanPrincipal,
    request: ClaimCommand | SendHumanCommand | ReturnToAgentCommand,
    kind: str,
    digest: str,
) -> InboxCommandReceipt | None:
    previous = cast(InboxCommandReceipt | None, await session.scalar(
        select(InboxCommandReceipt).where(
            InboxCommandReceipt.owner_id == case.owner_id,
            InboxCommandReceipt.integration_id == case.integration_id,
            InboxCommandReceipt.channel == case.channel,
            InboxCommandReceipt.conversation_id == case.conversation_id,
            InboxCommandReceipt.actor_account_id == principal.account_id,
            InboxCommandReceipt.command_id == request.command_id,
        )
    ))
    if previous is not None and (
        previous.case_id != case.id
        or previous.request_fingerprint != digest
        or previous.kind != kind
    ):
        raise DomainError(ErrorCode.COMMAND_CONFLICT)
    return previous


async def _next_ordinal(session: AsyncSession, conversation_id: UUID) -> int:
    current = await session.scalar(
        select(func.max(DeliveryAttempt.outbound_ordinal)).where(
            DeliveryAttempt.conversation_id == conversation_id,
            DeliveryAttempt.outbound_ordinal.is_not(None),
        )
    )
    return (current or 0) + 1


async def _run_command(
    database: Database,
    principal: HumanPrincipal,
    case_id: UUID,
    request: ClaimCommand | SendHumanCommand | ReturnToAgentCommand,
    kind: str,
) -> InboxCommandResult:
    async with database.session_factory() as session, session.begin():
        case, conversation, grant = await _authorized_case(session, principal, case_id, lock=True)
        # The URL path is part of command identity. A client cannot replay the
        # same command UUID against another handoff case in this conversation.
        digest = fingerprint(
            {
                "case_id": str(case.id),
                "command": request.model_dump(mode="json"),
            }
        )
        previous = await _existing_command(session, case, principal, request, kind, digest)
        if previous is not None:
            result = InboxCommandResult(
                receipt=_receipt_model(previous, replayed=True),
                case=_summary(case, conversation),
            )
        else:
            if (
                request.expected_case_revision != case.case_revision
                or request.expected_authority_revision != conversation.authority_revision
                or case.authority_revision != conversation.authority_revision
                or conversation.control_mode != "HUMAN"
            ):
                raise DomainError(ErrorCode.REVISION_CONFLICT)
            message_id = None
            outbound_intent_id = None
            applied_assignment: int | None = None
            clock = await db_clock(session)
            if kind == "CLAIM":
                if case.status != "OPEN":
                    raise DomainError(ErrorCode.REVISION_CONFLICT)
                case.status = "CLAIMED"
                case.assigned_account_id = principal.account_id
                case.assignment_revision += 1
                case.case_revision += 1
                case.updated_at = clock
                applied_assignment = case.assignment_revision
            elif kind == "SEND_HUMAN":
                assert isinstance(request, SendHumanCommand)
                if (
                    case.status != "CLAIMED"
                    or case.assigned_account_id != principal.account_id
                    or request.expected_assignment_revision != case.assignment_revision
                ):
                    raise DomainError(ErrorCode.REVISION_CONFLICT)
                conversation.last_message_sequence += 1
                conversation.updated_at = clock
                message_id = uuid4()
                outbound_intent_id = uuid4()
                message = Message(
                    id=message_id,
                    conversation_id=conversation.id,
                    sequence=conversation.last_message_sequence,
                    input_revision=None,
                    settling_revision=None,
                    settling_policy_version=None,
                    settling_quiet_window_ms=None,
                    settling_max_wait_ms=None,
                    settling_burst_id=None,
                    actor="HUMAN",
                    direction="OUTBOUND",
                    content_type="TEXT",
                    text=request.text,
                    source_inbound_event_id=None,
                    source_agent_turn_id=None,
                    created_at=clock,
                )
                case.case_revision += 1
                case.updated_at = clock
                # The delivery intent has an immediate composite FK to this
                # canonical message, so persist it first within the transaction.
                session.add(message)
                await session.flush()
                ordinal = await _next_ordinal(session, conversation.id)
                session.add(
                    DeliveryAttempt(
                        id=outbound_intent_id,
                        conversation_id=conversation.id,
                        delivery_key=outbound_intent_id,
                        source_turn_id=None,
                        source_message_id=message_id,
                        outbound_ordinal=ordinal,
                        case_id=case.id,
                        case_revision=case.case_revision,
                        assignment_revision=case.assignment_revision,
                        grant_generation=grant.grant_generation,
                        writer_state="NONE",
                        creation_fingerprint=fingerprint(
                            {
                                "contract_version": 1,
                                "case_id": str(case.id),
                                "command_id": str(request.command_id),
                                "message_id": str(message_id),
                                "text": request.text,
                            }
                        ),
                        authority_revision=conversation.authority_revision,
                        status="GENERATED",
                        revision=0,
                        created_at=clock,
                        updated_at=clock,
                    )
                )
                applied_assignment = case.assignment_revision
            else:
                assert kind == "RETURN_TO_AGENT"
                if case.status != "CLAIMED" or case.assigned_account_id != principal.account_id:
                    raise DomainError(ErrorCode.REVISION_CONFLICT)
                from services.authority import advance_authority
                from services.telegram import _reconciliation_key, _record_reconciliation
                from services.telegram_delivery_v2 import response_attempt_id

                case.status = "RETURNED"
                case.assigned_account_id = None
                case.assignment_revision += 1
                case.case_revision += 1
                case.updated_at = clock
                conversation.control_mode = "AGENT"
                advance_authority(conversation)
                conversation.handoff_resume_floor_sequence = conversation.last_message_sequence
                conversation.updated_at = clock
                case.authority_revision = conversation.authority_revision
                # Return retires only HUMAN intents that no supplier has ever
                # acquired. A LIVE writer remains a FIFO barrier until its
                # original supplier provides positive TASK_JOINED evidence.
                pending_human = (
                    await session.scalars(
                        select(DeliveryAttempt)
                        .where(
                            DeliveryAttempt.case_id == case.id,
                            DeliveryAttempt.writer_state == "NONE",
                            DeliveryAttempt.status == "GENERATED",
                        )
                        .order_by(DeliveryAttempt.outbound_ordinal, DeliveryAttempt.id)
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                ).all()
                for intent in pending_human:
                    _record_reconciliation(
                        session,
                        intent,
                        conversation.id,
                        "SUPERSEDED",
                        key=_reconciliation_key(
                            response_attempt_id(intent.id), "SUPERSEDED"
                        ),
                        expected_revision=intent.revision,
                        clock=clock,
                    )
                # Close the previous settling membership and retire only active old
                # turns. Terminal outcome evidence stays immutable and retained.
                from models import AgentTurn, SettlingBurst

                bursts = (
                    await session.scalars(
                        select(SettlingBurst)
                        .where(
                            SettlingBurst.conversation_id == conversation.id,
                            SettlingBurst.closed_at.is_(None),
                        )
                        .with_for_update()
                    )
                ).all()
                for burst in bursts:
                    burst.closed_at = burst.ready_at
                active_turns = (
                    await session.scalars(
                        select(AgentTurn)
                        .where(
                            AgentTurn.conversation_id == conversation.id,
                            AgentTurn.status.in_(("PENDING", "CLAIMED")),
                            AgentTurn.retired_at.is_(None),
                        )
                        .with_for_update()
                    )
                ).all()
                for turn in active_turns:
                    turn.retired_at = clock
                applied_assignment = case.assignment_revision
            receipt = InboxCommandReceipt(
                id=uuid4(),
                conversation_id=conversation.id,
                case_id=case.id,
                owner_id=case.owner_id,
                integration_id=case.integration_id,
                channel=case.channel,
                actor_account_id=principal.account_id,
                command_id=request.command_id,
                kind=kind,
                request_fingerprint=digest,
                applied_case_revision=case.case_revision,
                applied_authority_revision=conversation.authority_revision,
                applied_assignment_revision=applied_assignment,
                message_id=message_id,
                outbound_intent_id=outbound_intent_id,
                created_at=clock,
            )
            session.add(receipt)
            await session.flush()
            result = InboxCommandResult(
                receipt=_receipt_model(receipt, replayed=False),
                case=_summary(case, conversation),
            )
    return result


async def claim_case(
    database: Database,
    principal: HumanPrincipal,
    case_id: UUID,
    request: ClaimCommand,
) -> InboxCommandResult:
    return await _run_command(database, principal, case_id, request, "CLAIM")


async def send_human(
    database: Database,
    principal: HumanPrincipal,
    case_id: UUID,
    request: SendHumanCommand,
) -> InboxCommandResult:
    return await _run_command(database, principal, case_id, request, "SEND_HUMAN")


async def return_to_agent(
    database: Database,
    principal: HumanPrincipal,
    case_id: UUID,
    request: ReturnToAgentCommand,
) -> InboxCommandResult:
    return await _run_command(database, principal, case_id, request, "RETURN_TO_AGENT")

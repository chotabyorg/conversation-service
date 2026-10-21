"""Generalized, single-writer Telegram delivery for enrolled Handoff roots."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, TypeAlias, cast
from uuid import UUID, uuid4, uuid5

from sqlalchemy import and_, or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import ManagedBinding, Settings
from core.errors import DomainError
from db.database import Database
from models import (
    AgentTurn,
    Conversation,
    DeliveryAttempt,
    HandoffCase,
    InboxGrantScope,
    Message,
    TelegramProviderAccount,
)
from schemas.contracts import ErrorCode, Principal
from schemas.handoff import (
    TelegramBeginRequest,
    TelegramBeginResult,
    TelegramCompleteRequest,
    TelegramCompleteResult,
    TelegramWorkItem,
    TelegramWorkRequest,
    TelegramWorkResult,
)
from services import catch_up
from services.common import db_clock, fingerprint
from services.telegram import _reconciliation_key, _record_reconciliation, _telegram_account

V2_NAMESPACE = UUID("ec0df7d3-b0f4-4c6a-b16d-a02f80ebcde3")
HANDOFF_TG_NAMESPACE = "main-chota.telegram.v1"
AgentSource: TypeAlias = tuple[Literal["AGENT_TURN"], UUID, str, None, AgentTurn]
HumanSource: TypeAlias = tuple[Literal["OUTBOUND_MESSAGE"], UUID, str, HandoffCase, Message]
Source: TypeAlias = AgentSource | HumanSource


def response_attempt_id(intent_id: UUID) -> UUID:
    return uuid5(V2_NAMESPACE, str(intent_id))


def authorize(
    principal: Principal, owner_id: str, integration_id: UUID, agent_id: UUID, settings: Settings
) -> ManagedBinding:
    if principal.role != "TELEGRAM":
        raise DomainError(ErrorCode.NOT_FOUND)
    if not any(
        grant.owner_id == owner_id
        and grant.integration_id == integration_id
        and grant.channel.value == "TELEGRAM"
        and grant.provider_namespace == HANDOFF_TG_NAMESPACE
        for grant in principal.grants
    ):
        raise DomainError(ErrorCode.NOT_FOUND)
    binding = next(
        (
            item
            for item in settings.managed_ai_bindings
            if item.owner_id == owner_id
            and item.integration_id == integration_id
            and item.agent_id == agent_id
            and item.channel.value == "TELEGRAM"
        ),
        None,
    )
    if binding is None or integration_id not in settings.handoff_cohort_integration_ids:
        raise DomainError(ErrorCode.NOT_FOUND)
    return binding


async def _locked_scope(
    session: AsyncSession, owner_id: str, integration_id: UUID, agent_id: UUID
) -> InboxGrantScope:
    values = dict(
        owner_id=owner_id, integration_id=integration_id, channel="TELEGRAM", agent_id=agent_id
    )
    await session.execute(
        insert(InboxGrantScope)
        .values(**values)
        .on_conflict_do_nothing(
            index_elements=["owner_id", "integration_id", "channel", "agent_id"]
        )
    )
    scope = cast(
        InboxGrantScope | None,
        await session.scalar(
            select(InboxGrantScope)
            .where(*[getattr(InboxGrantScope, key) == value for key, value in values.items()])
            .with_for_update()
            .execution_options(populate_existing=True)
        ),
    )
    if scope is None:
        raise RuntimeError("Telegram grant scope unavailable")
    return scope


async def _locked_root(
    session: AsyncSession,
    request: TelegramBeginRequest,
    *,
    need_scope: bool = True,
) -> tuple[InboxGrantScope | None, Conversation]:
    conversation = await session.scalar(
        select(Conversation).where(
            Conversation.id == request.conversation_id,
            Conversation.owner_id == request.owner_id,
            Conversation.integration_id == request.integration_id,
            Conversation.channel == "TELEGRAM",
            Conversation.provider_thread_id == request.provider_thread_id,
        )
    )
    if conversation is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    scope = (
        await _locked_scope(session, request.owner_id, request.integration_id, request.agent_id)
        if need_scope
        else None
    )
    conversation = await session.scalar(
        select(Conversation)
        .where(
            Conversation.id == conversation.id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if conversation is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    return scope, conversation


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


def _valid_for_new_send(
    intent: DeliveryAttempt,
    source: Source,
    conversation: Conversation,
    scope: InboxGrantScope,
    account: TelegramProviderAccount | None,
    request_bot_id: int,
) -> bool:
    if (
        not scope.active
        or scope.agent_id is None
        or scope.grant_generation < 1
        or account is None
        or account.state != "BOUND"
        or account.bot_id != str(request_bot_id)
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


async def _candidate_item(
    session: AsyncSession,
    intent: DeliveryAttempt,
    conversation: Conversation,
    scope: InboxGrantScope,
    request: TelegramWorkRequest,
    account: TelegramProviderAccount | None,
) -> TelegramWorkItem | None:
    source = await _source(session, intent, conversation, scope, request.agent_id)
    if source is None:
        return None
    if intent.outbound_ordinal is None:
        return None
    kind, source_id, content, _extra, _value = source
    if intent.writer_state == "LIVE":
        work_kind: Literal["SEND_CANDIDATE", "RECOVER_ONLY"] = "RECOVER_ONLY"
    elif intent.writer_state != "NONE" or intent.status != "GENERATED":
        return None
    elif not _valid_for_new_send(intent, source, conversation, scope, account, int(request.bot_id)):
        return None
    else:
        work_kind = "SEND_CANDIDATE"
    if work_kind == "SEND_CANDIDATE":
        if not await _current_catch_up_source(session, source, conversation):
            return None
        lower = await session.scalar(
            select(DeliveryAttempt.id)
            .where(
                DeliveryAttempt.conversation_id == conversation.id,
                DeliveryAttempt.outbound_ordinal < intent.outbound_ordinal,
                DeliveryAttempt.outbound_ordinal.is_not(None),
                or_(
                    DeliveryAttempt.writer_state == "LIVE",
                    and_(
                        DeliveryAttempt.writer_state == "NONE",
                        DeliveryAttempt.status == "GENERATED",
                    ),
                ),
            )
            .limit(1)
        )
        if lower is not None:
            return None
    return TelegramWorkItem(
        contract_version=2,
        owner_id=request.owner_id,
        integration_id=request.integration_id,
        agent_id=request.agent_id,
        conversation_id=conversation.id,
        provider_thread_id=conversation.provider_thread_id,
        bot_id=request.bot_id,
        outbound_intent_id=intent.id,
        outbound_ordinal=intent.outbound_ordinal,
        source_kind=kind,
        source_id=source_id,
        response_attempt_id=response_attempt_id(intent.id),
        work_kind=work_kind,
        text=content,
    )


async def work(
    database: Database, principal: Principal, request: TelegramWorkRequest, settings: Settings
) -> TelegramWorkResult:
    authorize(principal, request.owner_id, request.integration_id, request.agent_id, settings)
    items: list[TelegramWorkItem] = []
    cursor: UUID | None = None
    while len(items) < 100:
        query = (
            select(Conversation.id)
            .join(
                InboxGrantScope,
                and_(
                    InboxGrantScope.owner_id == Conversation.owner_id,
                    InboxGrantScope.integration_id == Conversation.integration_id,
                    InboxGrantScope.channel == Conversation.channel,
                    InboxGrantScope.agent_id == request.agent_id,
                ),
            )
            .where(
                Conversation.owner_id == request.owner_id,
                Conversation.integration_id == request.integration_id,
                Conversation.channel == "TELEGRAM",
                InboxGrantScope.active.is_(True),
                InboxGrantScope.grant_generation >= 1,
            )
        )
        if cursor is not None:
            query = query.where(Conversation.id > cursor)
        async with database.session_factory() as session:
            ids = (await session.scalars(query.order_by(Conversation.id).limit(100))).all()
        if not ids:
            break
        for conversation_id in ids:
            if len(items) >= 100:
                break
            async with database.session_factory() as session, session.begin():
                scope = await _locked_scope(
                    session, request.owner_id, request.integration_id, request.agent_id
                )
                conversation = await session.scalar(
                    select(Conversation)
                    .where(
                        Conversation.id == conversation_id,
                        Conversation.owner_id == request.owner_id,
                        Conversation.integration_id == request.integration_id,
                        Conversation.channel == "TELEGRAM",
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
                if conversation is None:
                    continue
                account = await _telegram_account(session, request.owner_id, request.integration_id)
                turns = (
                    await session.scalars(
                        select(AgentTurn)
                        .where(
                            AgentTurn.conversation_id == conversation.id,
                            AgentTurn.managed_contract_version == 3,
                            AgentTurn.status == "COMPLETED",
                            AgentTurn.outcome == "DRAFT_READY",
                            AgentTurn.draft_text.is_not(None),
                            AgentTurn.retired_at.is_(None),
                        )
                        .order_by(AgentTurn.created_at, AgentTurn.id)
                        .with_for_update()
                    )
                ).all()
                clock = await db_clock(session)
                for turn in turns:
                    if turn.draft_text is None:
                        continue
                    if (
                        conversation.control_mode != "AGENT"
                        or turn.authority_revision != conversation.authority_revision
                        or (
                            conversation.handoff_resume_floor_sequence is not None
                            and turn.input_first_sequence
                            <= conversation.handoff_resume_floor_sequence
                        )
                    ):
                        continue
                    intent = await _ensure_agent_intent(session, conversation, turn, scope, clock)
                    source: AgentSource = ("AGENT_TURN", turn.id, turn.draft_text, None, turn)
                    expected_settling, current_settling = _settling_fence(source, conversation)
                    if (
                        intent.writer_state == "NONE"
                        and intent.status == "GENERATED"
                        and (
                            not _valid_for_new_send(
                                intent,
                                source,
                                conversation,
                                scope,
                                account,
                                int(request.bot_id),
                            )
                            or not await _current_catch_up_source(session, source, conversation)
                        )
                    ):
                        from services.telegram import _reconciliation_key, _record_reconciliation

                        _record_reconciliation(
                            session,
                            intent,
                            conversation.id,
                            "SUPERSEDED",
                            key=_reconciliation_key(response_attempt_id(intent.id), "SUPERSEDED"),
                            expected_revision=intent.revision,
                            expected_settling_revision=expected_settling,
                            result_settling_revision=current_settling,
                            clock=clock,
                        )
                intents = (
                    await session.scalars(
                        select(DeliveryAttempt)
                        .where(
                            DeliveryAttempt.conversation_id == conversation.id,
                            DeliveryAttempt.outbound_ordinal.is_not(None),
                        )
                        .order_by(DeliveryAttempt.outbound_ordinal)
                        .with_for_update()
                    )
                ).all()
                for intent in intents:
                    item = await _candidate_item(
                        session, intent, conversation, scope, request, account
                    )
                    if item is not None:
                        items.append(item)
                        if len(items) >= 100:
                            break
        cursor = ids[-1]
    items.sort(key=lambda item: (item.conversation_id, item.outbound_ordinal))
    return TelegramWorkResult(items=items)


async def _load_intent(
    session: AsyncSession,
    principal: Principal,
    settings: Settings,
    request: TelegramBeginRequest,
) -> tuple[InboxGrantScope, Conversation, DeliveryAttempt, Source]:
    scope = await _locked_scope(session, request.owner_id, request.integration_id, request.agent_id)
    conversation = await session.scalar(
        select(Conversation)
        .where(
            Conversation.id == request.conversation_id,
            Conversation.owner_id == request.owner_id,
            Conversation.integration_id == request.integration_id,
            Conversation.channel == "TELEGRAM",
            Conversation.provider_thread_id == request.provider_thread_id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if conversation is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    intent = await session.scalar(
        select(DeliveryAttempt)
        .where(
            DeliveryAttempt.id == request.outbound_intent_id,
            DeliveryAttempt.conversation_id == conversation.id,
            DeliveryAttempt.outbound_ordinal == request.outbound_ordinal,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if intent is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    if intent.outbound_ordinal is None:
        raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
    source = await _source(session, intent, conversation, scope, request.agent_id)
    if source is None:
        raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
    kind, source_id, _content, _extra, _value = source
    if (
        kind != request.source_kind
        or source_id != request.source_id
        or request.response_attempt_id != response_attempt_id(intent.id)
    ):
        raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
    return scope, conversation, intent, source


def _begin_echo(
    request: TelegramBeginRequest,
    text_value: str,
    writer_state: Literal["NONE", "LIVE", "ENDED", "RETIRED"],
    *,
    send: bool,
    replayed: bool,
) -> TelegramBeginResult:
    payload = request.model_dump(mode="python")
    payload["text"] = text_value
    return TelegramBeginResult(
        **payload,
        send=send,
        writer_state=writer_state,
        replayed=replayed,
    )


async def begin(
    database: Database, principal: Principal, request: TelegramBeginRequest, settings: Settings
) -> TelegramBeginResult:
    authorize(principal, request.owner_id, request.integration_id, request.agent_id, settings)
    async with database.session_factory() as session, session.begin():
        scope, conversation, intent, source = await _load_intent(
            session, principal, settings, request
        )
        account = await _telegram_account(session, request.owner_id, request.integration_id)
        writer_state = cast(Literal["NONE", "LIVE", "ENDED", "RETIRED"], intent.writer_state)
        if request.work_kind == "RECOVER_ONLY":
            return _begin_echo(request, source[2], writer_state, send=False, replayed=True)
        if intent.writer_state != "NONE":
            return _begin_echo(request, source[2], writer_state, send=False, replayed=True)
        if intent.status != "GENERATED":
            return _begin_echo(request, source[2], writer_state, send=False, replayed=True)
        now_valid = _valid_for_new_send(
            intent, source, conversation, scope, account, int(request.bot_id)
        ) and await _current_catch_up_source(session, source, conversation)
        if not now_valid:
            clock = await db_clock(session)
            expected_settling, fenced_settling = _settling_fence(source, conversation)
            _record_reconciliation(
                session,
                intent,
                conversation.id,
                "SUPERSEDED",
                key=_reconciliation_key(response_attempt_id(intent.id), "SUPERSEDED"),
                expected_revision=intent.revision,
                expected_settling_revision=expected_settling,
                result_settling_revision=fenced_settling,
                clock=clock,
            )
            return _begin_echo(request, source[2], writer_state, send=False, replayed=False)
        lower = await session.scalar(
            select(DeliveryAttempt.id)
            .where(
                DeliveryAttempt.conversation_id == conversation.id,
                DeliveryAttempt.outbound_ordinal < intent.outbound_ordinal,
                DeliveryAttempt.outbound_ordinal.is_not(None),
                or_(
                    DeliveryAttempt.writer_state == "LIVE",
                    and_(
                        DeliveryAttempt.writer_state == "NONE",
                        DeliveryAttempt.status == "GENERATED",
                    ),
                ),
            )
            .limit(1)
        )
        if lower is not None:
            return _begin_echo(request, source[2], "NONE", send=False, replayed=False)
        clock = await db_clock(session)
        intent.writer_state = "LIVE"
        intent.writer_supplier_instance_id = request.supplier_instance_id
        intent.writer_process_incarnation_id = request.process_incarnation_id
        intent.writer_started_at = clock
        expected_settling, fenced_settling = _settling_fence(source, conversation)
        _record_reconciliation(
            session,
            intent,
            conversation.id,
            "DISPATCH_REQUESTED",
            key=_reconciliation_key(response_attempt_id(intent.id), "DISPATCH_REQUESTED"),
            expected_revision=intent.revision,
            authority_revision=intent.authority_revision,
            expected_settling_revision=expected_settling,
            result_settling_revision=fenced_settling,
            clock=clock,
        )
        await session.flush()
        return _begin_echo(
            request,
            source[2],
            cast(Literal["NONE", "LIVE", "ENDED", "RETIRED"], intent.writer_state),
            send=True,
            replayed=False,
        )


async def complete(
    database: Database, principal: Principal, request: TelegramCompleteRequest, settings: Settings
) -> TelegramCompleteResult:
    authorize(principal, request.owner_id, request.integration_id, request.agent_id, settings)
    async with database.session_factory() as session, session.begin():
        scope, conversation, intent, source = await _load_intent(
            session, principal, settings, request
        )
        if (
            intent.writer_supplier_instance_id != request.supplier_instance_id
            or intent.writer_process_incarnation_id != request.process_incarnation_id
            or intent.writer_state not in {"LIVE", "ENDED", "RETIRED"}
        ):
            raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
        if request.work_kind != "RECOVER_ONLY" and intent.writer_state == "LIVE":
            # A begin request is a one-shot acquisition. Completion can repeat its
            # original work descriptor, but that identity must still agree here.
            if intent.status not in {"DISPATCH_REQUESTED", "UNKNOWN"}:
                raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
        desired_status = request.outcome
        same_evidence = (
            intent.status == desired_status
            and intent.provider_message_id == request.provider_message_id
            and intent.error_code == request.error_code
        )
        if intent.status in {"PROVIDER_ACCEPTED", "FAILED"} and not same_evidence:
            raise DomainError(ErrorCode.DELIVERY_RECONCILIATION_CONFLICT)
        if intent.status == "UNKNOWN" and desired_status == "UNKNOWN" and not same_evidence:
            raise DomainError(ErrorCode.DELIVERY_RECONCILIATION_CONFLICT)
        if not same_evidence:
            if intent.status not in {"DISPATCH_REQUESTED", "UNKNOWN"}:
                raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
            clock = await db_clock(session)
            from services.telegram import _reconciliation_key

            ref = request.provider_message_id or (request.error_code or "")
            _record_reconciliation(
                session,
                intent,
                conversation.id,
                desired_status,
                provider_message_id=request.provider_message_id,
                error_code=request.error_code,
                key=_reconciliation_key(response_attempt_id(intent.id), desired_status, ref),
                expected_revision=intent.revision,
                clock=clock,
            )
            if desired_status == "PROVIDER_ACCEPTED" and source[0] == "AGENT_TURN":
                turn: AgentTurn = source[4]
                exists_message = await session.scalar(
                    select(Message.id).where(
                        Message.conversation_id == conversation.id,
                        Message.source_agent_turn_id == turn.id,
                    )
                )
                if exists_message is None:
                    conversation.last_message_sequence += 1
                    conversation.updated_at = clock
                    session.add(
                        Message(
                            id=uuid4(),
                            conversation_id=conversation.id,
                            sequence=conversation.last_message_sequence,
                            actor="AGENT",
                            direction="OUTBOUND",
                            content_type="TEXT",
                            text=source[2],
                            source_agent_turn_id=turn.id,
                            created_at=clock,
                        )
                    )
        ended = intent.writer_state in {"ENDED", "RETIRED"}
        if request.writer_end is not None:
            if (
                intent.writer_state == "LIVE"
                and request.writer_end.supplier_instance_id == intent.writer_supplier_instance_id
                and request.writer_end.process_incarnation_id
                == intent.writer_process_incarnation_id
            ):
                intent.writer_state = "ENDED"
                intent.writer_ended_at = await db_clock(session)
                ended = True
            elif intent.writer_state not in {"ENDED", "RETIRED"}:
                raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
        await session.flush()
        return TelegramCompleteResult(
            contract_version=2,
            outbound_intent_id=intent.id,
            response_attempt_id=response_attempt_id(intent.id),
            outcome=desired_status,
            writer_state=cast(Literal["LIVE", "ENDED", "RETIRED"], intent.writer_state),
            replayed=same_evidence and (request.writer_end is None or ended),
        )

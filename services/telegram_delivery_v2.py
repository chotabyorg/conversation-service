"""Generalized, single-writer Telegram delivery for enrolled Handoff roots."""

from __future__ import annotations

from typing import Literal, cast
from uuid import UUID, uuid5

from sqlalchemy import and_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import ManagedBinding, Settings
from core.errors import DomainError
from db.database import Database
from models import (
    AgentTurn,
    Conversation,
    DeliveryAttempt,
    InboxGrantScope,
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
from services.common import db_clock
from services.telegram import _reconciliation_key, _telegram_account
from services.text_delivery import (
    AgentSource,
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

V2_NAMESPACE = UUID("ec0df7d3-b0f4-4c6a-b16d-a02f80ebcde3")
HANDOFF_TG_NAMESPACE = "main-chota.telegram.v1"


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



def _valid_for_new_send(
    intent: DeliveryAttempt, source: Source, conversation: Conversation,
    scope: InboxGrantScope, account: TelegramProviderAccount | None,
    request_bot_id: int,
) -> bool:
    return (account is not None and account.state == "BOUND"
        and account.bot_id == str(request_bot_id)
        and _valid_authority(intent, source, conversation, scope))





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
        if await lower_pending(session, intent):
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
        current = False
        if (request.work_kind != "RECOVER_ONLY" and intent.writer_state == "NONE"
                and intent.status == "GENERATED"):
            current = _valid_for_new_send(
                intent, source, conversation, scope, account, int(request.bot_id)
            ) and await _current_catch_up_source(session, source, conversation)
        decision = await begin_writer(session, intent, conversation, source,
            recover_only=request.work_kind == "RECOVER_ONLY", current=current,
            supplier_instance_id=request.supplier_instance_id,
            process_incarnation_id=request.process_incarnation_id,
            superseded_key=_reconciliation_key(response_attempt_id(intent.id), "SUPERSEDED"),
            dispatch_key=_reconciliation_key(response_attempt_id(intent.id), "DISPATCH_REQUESTED"))
        return _begin_echo(request, source[2],
            cast(Literal["NONE", "LIVE", "ENDED", "RETIRED"], intent.writer_state),
            send=decision.send, replayed=decision.replayed)


async def complete(
    database: Database, principal: Principal, request: TelegramCompleteRequest, settings: Settings
) -> TelegramCompleteResult:
    authorize(principal, request.owner_id, request.integration_id, request.agent_id, settings)
    async with database.session_factory() as session, session.begin():
        scope, conversation, intent, source = await _load_intent(
            session, principal, settings, request
        )
        evidence = WriterCompletionEvidence(
            supplier_instance_id=request.supplier_instance_id,
            process_incarnation_id=request.process_incarnation_id, outcome=request.outcome,
            provider_message_id=request.provider_message_id, error_code=request.error_code,
            task_joined=request.writer_end is not None)
        ref = request.provider_message_id or (request.error_code or "")
        decision = await complete_writer(session, intent, conversation, source, evidence,
            recover_only=request.work_kind == "RECOVER_ONLY",
            reconciliation_key=_reconciliation_key(
                response_attempt_id(intent.id), request.outcome, ref))
        return TelegramCompleteResult(contract_version=2, outbound_intent_id=intent.id,
            response_attempt_id=response_attempt_id(intent.id), outcome=decision.outcome,
            writer_state=cast(Literal["LIVE", "ENDED", "RETIRED"], intent.writer_state),
            replayed=decision.replayed)

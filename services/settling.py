"""PostgreSQL-authoritative bounded burst selection shared by managed workers."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID, uuid4, uuid5

from sqlalchemy import and_, exists, func, literal, or_, select, tuple_
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from core.config import ManagedBinding, Settings
from core.errors import DomainError
from db.database import Database
from models import (
    AgentTurn,
    Conversation,
    Message,
    TelegramAdmission,
    TelegramImmediateTurn,
    TurnInputMessage,
    WebsiteAdmission,
    WebsiteImmediateTurn,
)
from schemas.contracts import Channel, ErrorCode, Principal
from schemas.managed import MAX_MESSAGES
from services.common import db_clock, fingerprint, scope_filter
from services.history_projection import freeze_website_projection

MAX_BURST_MESSAGES = MAX_MESSAGES
CANDIDATE_PAGE_SIZE = 100
_SELECTION_NAMESPACE = UUID("d05d07b0-70a5-4f27-9628-801b852bd49e")


def _bindings(settings: Settings, channel: Channel) -> tuple[ManagedBinding, ...]:
    bindings = tuple(binding for binding in settings.managed_ai_bindings
                     if binding.channel == channel)
    if channel == Channel.WEBSITE_WIDGET:
        website = {(entry.owner_id, entry.integration_id, entry.agent_id)
                   for entry in settings.website_bindings}
        bindings = tuple(binding for binding in bindings
                         if (binding.owner_id, binding.integration_id, binding.agent_id) in website)
    return bindings


def _binding_clause(bindings: tuple[ManagedBinding, ...]) -> ColumnElement[bool] | None:
    if not bindings:
        return None
    return or_(*(and_(Conversation.owner_id == binding.owner_id,
                      Conversation.integration_id == binding.integration_id,
                      Conversation.channel == binding.channel.value)
                 for binding in bindings))


async def _record_selection(
    session: AsyncSession,
    turn: AgentTurn,
    selected: list[Message],
    channel: Channel,
) -> None:
    session.add_all(TurnInputMessage(
        message_id=message.id,
        conversation_id=turn.conversation_id,
        turn_id=turn.id,
        sequence=message.sequence,
        ordinal=ordinal,
    ) for ordinal, message in enumerate(selected))
    if channel == Channel.TELEGRAM:
        session.add_all(TelegramImmediateTurn(
            message_id=message.id, conversation_id=turn.conversation_id, turn_id=turn.id
        ) for message in selected)
    elif channel == Channel.WEBSITE_WIDGET:
        session.add_all(WebsiteImmediateTurn(
            message_id=message.id, conversation_id=turn.conversation_id, turn_id=turn.id
        ) for message in selected)


def _burst(messages: list[Message], now: datetime) -> tuple[list[Message], datetime] | None:
    if not messages or messages[0].settling_policy_version is None:
        return None
    first = messages[0]
    quiet_ms = first.settling_quiet_window_ms
    max_ms = first.settling_max_wait_ms
    if quiet_ms is None or max_ms is None:
        return None
    max_deadline = first.created_at + timedelta(milliseconds=max_ms)
    quiet_deadline = min(first.created_at + timedelta(milliseconds=quiet_ms), max_deadline)
    selected = [first]
    for message in messages[1:MAX_BURST_MESSAGES]:
        if message.sequence != selected[-1].sequence + 1:
            break
        if (message.settling_policy_version != first.settling_policy_version
                or message.settling_quiet_window_ms != quiet_ms
                or message.settling_max_wait_ms != max_ms):
            break
        if message.created_at >= min(quiet_deadline, max_deadline):
            break
        selected.append(message)
        quiet_deadline = min(message.created_at + timedelta(milliseconds=quiet_ms), max_deadline)
    deadline = min(quiet_deadline, max_deadline)
    if now < deadline:
        return None
    return selected, deadline


async def _pending_messages(session: AsyncSession, conversation_id: UUID) -> list[Message]:
    assigned = exists(select(TurnInputMessage.message_id).where(
        TurnInputMessage.message_id == Message.id))
    in_frozen_legacy_range = exists(select(AgentTurn.id).where(
        AgentTurn.conversation_id == Message.conversation_id,
        Message.sequence >= AgentTurn.input_first_sequence,
        Message.sequence <= AgentTurn.input_last_sequence,
    ))
    return list((await session.scalars(select(Message).where(
        Message.conversation_id == conversation_id,
        Message.actor == "CUSTOMER",
        Message.direction == "INBOUND",
        ~assigned,
        ~in_frozen_legacy_range,
    ).order_by(Message.sequence).limit(MAX_BURST_MESSAGES + 1))).all())


def _turn_for_selection(
    conversation: Conversation, selected: list[Message], clock: datetime
) -> AgentTurn:
    first, last = selected[0], selected[-1]
    selection_key = uuid5(_SELECTION_NAMESPACE, f"{conversation.id}:{first.id}:{last.id}")
    boundaries = {
        "contract_version": 1,
        "conversation_id": str(conversation.id),
        "selection_key": str(selection_key),
        "input_first_sequence": first.sequence,
        "input_last_sequence": last.sequence,
        "context_last_sequence": last.sequence,
        "input_revision": last.input_revision,
        "settling_revision": conversation.settling_revision,
        "authority_revision": conversation.authority_revision,
        "message_ids": [str(message.id) for message in selected],
        "policy_version": first.settling_policy_version,
    }
    return AgentTurn(
        id=uuid4(), conversation_id=conversation.id, selection_key=selection_key,
        creation_fingerprint=fingerprint(boundaries),
        input_first_sequence=first.sequence, input_last_sequence=last.sequence,
        context_last_sequence=last.sequence, input_revision=last.input_revision,
        settling_revision=conversation.settling_revision,
        authority_revision=conversation.authority_revision, status="PENDING", claim_epoch=0,
        operation_refs=[], created_at=clock,
    )


async def _maximal_projectable_prefix(
    session: AsyncSession,
    conversation: Conversation,
    messages: list[Message],
    binding: ManagedBinding,
) -> list[Message]:
    """Use the production ManagedTurn v2 projection to find the largest valid prefix."""
    low, high, best = 1, min(len(messages), MAX_MESSAGES), 0
    while low <= high:
        middle = (low + high) // 2
        candidate = messages[:middle]
        probe = _turn_for_selection(conversation, candidate, candidate[-1].created_at)
        try:
            await freeze_website_projection(session, conversation, probe, binding.agent_id)
        except DomainError as exc:
            if exc.code != ErrorCode.VALIDATION_ERROR:
                raise
            high = middle - 1
        else:
            best = middle
            low = middle + 1
    return messages[:best]


async def _create_turn(
    session: AsyncSession,
    conversation: Conversation,
    selected: list[Message],
    binding: ManagedBinding,
    channel: Channel,
    clock: datetime,
) -> UUID:
    first, last = selected[0], selected[-1]
    if first.input_revision is None or last.input_revision is None:
        raise RuntimeError("Settled inbound Message is missing its revision snapshot")
    if any(message.sequence != first.sequence + index for index, message in enumerate(selected)):
        raise RuntimeError("Settled input is not a contiguous canonical Message range")
    turn = _turn_for_selection(conversation, selected, clock)
    if channel == Channel.WEBSITE_WIDGET or channel == Channel.TELEGRAM:
        await freeze_website_projection(session, conversation, turn, binding.agent_id)
    session.add(turn)
    await session.flush()
    await _record_selection(session, turn, selected, channel)
    await session.flush()
    return turn.id


async def select_settled_turn(
    database: Database,
    principal: Principal,
    settings: Settings,
    channels: tuple[Channel, ...] = (Channel.TELEGRAM, Channel.WEBSITE_WIDGET),
) -> UUID | None:
    """Select oldest due work through bounded keyset pages."""
    bindings = tuple(binding for channel in channels for binding in _bindings(settings, channel))
    scope = _binding_clause(bindings)
    if scope is None:
        return None
    pending = ~exists(select(TurnInputMessage.message_id).where(
        TurnInputMessage.message_id == Message.id)) & ~exists(select(AgentTurn.id).where(
            AgentTurn.conversation_id == Message.conversation_id,
            Message.sequence >= AgentTurn.input_first_sequence,
            Message.sequence <= AgentTurn.input_last_sequence,
        ))
    candidate_rows = select(
        Conversation.id.label("conversation_id"),
        func.min(Message.created_at).label("oldest_created_at"),
    ).join(Message, Message.conversation_id == Conversation.id).where(
        Message.actor == "CUSTOMER", Message.direction == "INBOUND",
        Message.settling_policy_version.is_not(None), pending,
        scope_filter(principal), scope,
    ).group_by(Conversation.id).subquery()
    cursor: tuple[datetime, UUID] | None = None
    while True:
        async with database.session_factory() as session, session.begin():
            page_query = select(
                candidate_rows.c.conversation_id,
                candidate_rows.c.oldest_created_at,
            )
            if cursor is not None:
                page_query = page_query.where(tuple_(
                    candidate_rows.c.oldest_created_at,
                    candidate_rows.c.conversation_id,
                ) > tuple_(literal(cursor[0]), literal(cursor[1])))
            page = list((await session.execute(
                page_query.order_by(candidate_rows.c.oldest_created_at,
                                    candidate_rows.c.conversation_id)
                .limit(CANDIDATE_PAGE_SIZE)
            )).all())
        if not page:
            return None

        for conversation_id, _oldest_created_at in page:
            async with database.session_factory() as session, session.begin():
                conversation = await session.scalar(select(Conversation).where(
                    Conversation.id == conversation_id, scope_filter(principal)
                ).with_for_update(skip_locked=True).execution_options(populate_existing=True))
                if conversation is None or conversation.control_mode != "AGENT":
                    continue
                active = await session.scalar(select(AgentTurn).where(
                    AgentTurn.conversation_id == conversation.id,
                    AgentTurn.status.in_(("PENDING", "CLAIMED")),
                ).with_for_update())
                if active is not None:
                    continue
                rows = await _pending_messages(session, conversation.id)
                if not rows or rows[0].settling_policy_version is None:
                    continue
                channel = Channel(conversation.channel)
                channel_bindings = _bindings(settings, channel)
                binding = next((candidate for candidate in channel_bindings
                                if candidate.owner_id == conversation.owner_id
                                and candidate.integration_id == conversation.integration_id), None)
                if binding is None:
                    continue
                if channel == Channel.TELEGRAM:
                    admission = await session.scalar(select(TelegramAdmission).where(
                        TelegramAdmission.canonical_message_id == rows[0].id,
                        TelegramAdmission.conversation_id == conversation.id,
                        TelegramAdmission.route == "MANAGED",
                    ))
                    if admission is None or admission.agent_id != binding.agent_id:
                        continue
                elif channel == Channel.WEBSITE_WIDGET:
                    admission = await session.scalar(select(WebsiteAdmission).where(
                        WebsiteAdmission.canonical_message_id == rows[0].id,
                        WebsiteAdmission.conversation_id == conversation.id,
                        WebsiteAdmission.route == "MANAGED",
                    ))
                    if admission is None or admission.agent_id != binding.agent_id:
                        continue
                else:
                    continue
                clock = await db_clock(session)
                choice = _burst(rows, clock)
                if choice is None:
                    continue
                due_messages, _deadline = choice
                selected = await _maximal_projectable_prefix(
                    session, conversation, due_messages, binding
                )
                if not selected:
                    # A candidate-specific invalid projection remains durable and
                    # cannot prevent later Conversations from making progress.
                    continue
                return await _create_turn(session, conversation, selected, binding, channel, clock)

        if len(page) < CANDIDATE_PAGE_SIZE:
            return None
        last_conversation_id, last_created_at = page[-1]
        cursor = (last_created_at, last_conversation_id)

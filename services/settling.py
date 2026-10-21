"""PostgreSQL-authoritative bounded burst selection shared by managed workers."""

from __future__ import annotations

from datetime import datetime, timedelta
from uuid import UUID, uuid4, uuid5

from sqlalchemy import and_, func, literal, or_, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from core.config import ManagedBinding, Settings, SettlingPolicy
from core.errors import DomainError
from db.database import Database
from models import (
    AgentTurn,
    Conversation,
    InboxGrantScope,
    Message,
    SettlingBurst,
    TelegramAdmission,
    TelegramImmediateTurn,
    TelegramProviderAccount,
    TurnInputMessage,
    WebsiteAdmission,
    WebsiteImmediateTurn,
)
from schemas.contracts import Channel, ErrorCode, Principal
from schemas.managed import MAX_MESSAGES
from services import catch_up
from services.common import db_clock, fingerprint, message_is_consumed, scope_filter
from services.history_projection import freeze_website_projection

MAX_BURST_MESSAGES = MAX_MESSAGES
CANDIDATE_PAGE_SIZE = 100
_SELECTION_NAMESPACE = UUID("d05d07b0-70a5-4f27-9628-801b852bd49e")


async def freeze_admission_membership(
    session: AsyncSession,
    conversation: Conversation,
    policy: SettlingPolicy | None,
    sequence: int,
    *,
    separate_authority_epoch: bool = False,
) -> tuple[datetime, UUID | None, str | None, int | None, int | None]:
    """Freeze one accepted input into the open burst or create a new burst."""
    burst = await session.scalar(
        select(SettlingBurst)
        .where(
            SettlingBurst.conversation_id == conversation.id,
            SettlingBurst.closed_at.is_(None),
        )
        .with_for_update()
    )
    clock = await db_clock(session)
    if (
        burst is not None
        and separate_authority_epoch
        and burst.tail_sequence <= conversation.authority_input_floor_sequence
    ):
        # Seal the old deadline without moving it or admitting post-Return input
        # to a burst from a previous authority epoch.
        burst.closed_at = burst.ready_at
        await session.flush()
        burst = None
    if burst is not None and clock < burst.ready_at:
        if sequence <= burst.tail_sequence:
            raise RuntimeError("Settling burst tail is not monotonic")
        burst.last_input_at = clock
        burst.tail_sequence = sequence
        burst.ready_at = min(
            clock + timedelta(milliseconds=burst.quiet_window_ms), burst.max_cutoff_at
        )
        await session.flush()
        return (clock, burst.id, burst.policy_version, burst.quiet_window_ms, burst.max_wait_ms)
    if burst is not None:
        burst.closed_at = burst.ready_at
    if policy is None:
        return clock, None, None, None, None
    version = policy.version
    quiet_ms = policy.quiet_window_ms
    max_ms = policy.max_wait_ms
    cutoff = clock + timedelta(milliseconds=max_ms)
    burst = SettlingBurst(
        id=uuid4(),
        conversation_id=conversation.id,
        policy_version=version,
        quiet_window_ms=quiet_ms,
        max_wait_ms=max_ms,
        opened_at=clock,
        max_cutoff_at=cutoff,
        last_input_at=clock,
        tail_sequence=sequence,
        ready_at=min(clock + timedelta(milliseconds=quiet_ms), cutoff),
        closed_at=None,
    )
    session.add(burst)
    await session.flush()
    return clock, burst.id, version, quiet_ms, max_ms


def _bindings(settings: Settings, channel: Channel) -> tuple[ManagedBinding, ...]:
    bindings = tuple(
        binding for binding in settings.managed_ai_bindings if binding.channel == channel
    )
    if channel == Channel.WEBSITE_WIDGET:
        website = {
            (entry.owner_id, entry.integration_id, entry.agent_id)
            for entry in settings.website_bindings
        }
        bindings = tuple(
            binding
            for binding in bindings
            if (binding.owner_id, binding.integration_id, binding.agent_id) in website
        )
    return bindings


def _binding_clause(bindings: tuple[ManagedBinding, ...]) -> ColumnElement[bool] | None:
    if not bindings:
        return None
    return or_(
        *(
            and_(
                Conversation.owner_id == binding.owner_id,
                Conversation.integration_id == binding.integration_id,
                Conversation.channel == binding.channel.value,
            )
            for binding in bindings
        )
    )


async def _record_selection(
    session: AsyncSession,
    turn: AgentTurn,
    selected: list[Message],
    channel: Channel,
) -> None:
    session.add_all(
        TurnInputMessage(
            message_id=message.id,
            conversation_id=turn.conversation_id,
            turn_id=turn.id,
            sequence=message.sequence,
            ordinal=ordinal,
        )
        for ordinal, message in enumerate(selected)
    )
    if channel == Channel.TELEGRAM:
        session.add_all(
            TelegramImmediateTurn(
                message_id=message.id, conversation_id=turn.conversation_id, turn_id=turn.id
            )
            for message in selected
        )
    elif channel == Channel.WEBSITE_WIDGET:
        session.add_all(
            WebsiteImmediateTurn(
                message_id=message.id, conversation_id=turn.conversation_id, turn_id=turn.id
            )
            for message in selected
        )


async def _pending_messages(
    session: AsyncSession, conversation_id: UUID, input_floor: int = 0
) -> list[Message]:
    statement = select(Message).where(
        Message.conversation_id == conversation_id,
        Message.actor == "CUSTOMER",
        Message.direction == "INBOUND",
        Message.sequence > input_floor,
        ~message_is_consumed(),
    )
    return list(
        (
            await session.scalars(
                statement.order_by(Message.sequence).limit(MAX_BURST_MESSAGES + 1)
            )
        ).all()
    )


def _turn_for_selection(
    conversation: Conversation,
    selected: list[Message],
    clock: datetime,
    catch_up_digest: str | None = None,
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
        # All eligible members are known before selection; residual chunks must
        # share the same selection-time input freshness snapshot.
        "input_revision": conversation.input_revision,
        "settling_revision": conversation.settling_revision,
        "settling_burst_id": (
            str(first.settling_burst_id) if first.settling_burst_id is not None else None
        ),
        "authority_revision": conversation.authority_revision,
        "message_ids": [str(message.id) for message in selected],
        "policy_version": first.settling_policy_version,
    }
    if catch_up_digest is not None:
        boundaries.update(catch_up_version=1, catch_up_digest=catch_up_digest)
    return AgentTurn(
        id=uuid4(),
        conversation_id=conversation.id,
        selection_key=selection_key,
        creation_fingerprint=fingerprint(boundaries),
        input_first_sequence=first.sequence,
        input_last_sequence=last.sequence,
        context_last_sequence=last.sequence,
        input_revision=conversation.input_revision,
        settling_revision=conversation.settling_revision,
        settling_burst_id=first.settling_burst_id,
        catch_up_version=1 if catch_up_digest is not None else 0,
        authority_revision=conversation.authority_revision,
        status="PENDING",
        claim_epoch=0,
        operation_refs=[],
        created_at=clock,
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


def _validate_settled_members(messages: list[Message], *, multiple_bursts: bool = False) -> None:
    """Reject malformed persisted snapshots before projection or ownership writes."""
    if not messages:
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    first = messages[0]
    for index, message in enumerate(messages):
        if (
            message.input_revision is None
            or message.input_revision < 1
            or message.settling_revision is None
            or message.settling_revision < 0
            or (not multiple_bursts and message.settling_burst_id != first.settling_burst_id)
            or message.settling_policy_version != first.settling_policy_version
            or message.settling_quiet_window_ms != first.settling_quiet_window_ms
            or message.settling_max_wait_ms != first.settling_max_wait_ms
            or message.sequence != first.sequence + index
        ):
            raise DomainError(ErrorCode.VALIDATION_ERROR)


async def _create_turn(
    session: AsyncSession,
    conversation: Conversation,
    selected: list[Message],
    binding: ManagedBinding,
    channel: Channel,
    clock: datetime,
    catch_up_enabled: bool = False,
    handoff_enrolled: bool = False,
) -> UUID:
    _validate_settled_members(selected, multiple_bursts=catch_up_enabled)
    evidence = (
        await catch_up.prepare(session, conversation, selected, binding.agent_id)
        if catch_up_enabled
        else None
    )
    turn = _turn_for_selection(
        conversation, selected, clock, evidence.digest if evidence is not None else None
    )
    if channel == Channel.WEBSITE_WIDGET or channel == Channel.TELEGRAM:
        await freeze_website_projection(session, conversation, turn, binding.agent_id)
    if handoff_enrolled:
        turn.managed_contract_version = 3
    session.add(turn)
    await session.flush()
    await _record_selection(session, turn, selected, channel)
    await session.flush()
    if evidence is not None:
        await catch_up.persist(session, turn, evidence, clock)
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
    pending = ~message_is_consumed()
    async with database.session_factory() as session:
        discovery_clock = await db_clock(session)
    candidate_rows = (
        select(
            Conversation.id.label("conversation_id"),
            func.min(SettlingBurst.ready_at).label("oldest_ready_at"),
        )
        .join(Message, Message.conversation_id == Conversation.id)
        .join(
            SettlingBurst,
            and_(
                SettlingBurst.id == Message.settling_burst_id,
                SettlingBurst.conversation_id == Message.conversation_id,
            ),
        )
        .where(
            Message.actor == "CUSTOMER",
            Message.direction == "INBOUND",
            Message.settling_burst_id.is_not(None),
            pending,
            SettlingBurst.ready_at <= discovery_clock,
            scope_filter(principal),
            scope,
        )
        .group_by(Conversation.id)
        .subquery()
    )
    cursor: tuple[datetime, UUID] | None = None
    while True:
        async with database.session_factory() as session, session.begin():
            page_query = select(
                candidate_rows.c.conversation_id,
                candidate_rows.c.oldest_ready_at,
            )
            if cursor is not None:
                page_query = page_query.where(
                    tuple_(
                        candidate_rows.c.oldest_ready_at,
                        candidate_rows.c.conversation_id,
                    )
                    > tuple_(literal(cursor[0]), literal(cursor[1]))
                )
            page = list(
                (
                    await session.execute(
                        page_query.order_by(
                            candidate_rows.c.oldest_ready_at, candidate_rows.c.conversation_id
                        ).limit(CANDIDATE_PAGE_SIZE)
                    )
                ).all()
            )
        if not page:
            return None

        for conversation_id, _oldest_ready_at in page:
            try:
                async with database.session_factory() as session, session.begin():
                    candidate = await session.scalar(
                        select(Conversation).where(
                            Conversation.id == conversation_id, scope_filter(principal)
                        )
                    )
                    if candidate is None:
                        continue
                    channel = Channel(candidate.channel)
                    binding = next(
                        (
                            candidate_binding
                            for candidate_binding in _bindings(settings, channel)
                            if candidate_binding.owner_id == candidate.owner_id
                            and candidate_binding.integration_id == candidate.integration_id
                        ),
                        None,
                    )
                    if binding is None:
                        continue
                    lifecycle_scope = None
                    if channel == Channel.TELEGRAM:
                        values = dict(
                            owner_id=candidate.owner_id,
                            integration_id=candidate.integration_id,
                            channel=Channel.TELEGRAM.value,
                            agent_id=binding.agent_id,
                        )
                        await session.execute(
                            insert(InboxGrantScope)
                            .values(**values)
                            .on_conflict_do_nothing(
                                index_elements=["owner_id", "integration_id", "channel", "agent_id"]
                            )
                        )
                        lifecycle_scope = await session.scalar(
                            select(InboxGrantScope)
                            .where(
                                *[
                                    getattr(InboxGrantScope, key) == value
                                    for key, value in values.items()
                                ]
                            )
                            .with_for_update()
                            .execution_options(populate_existing=True)
                        )
                        if lifecycle_scope is None:
                            raise RuntimeError("Telegram grant scope unavailable")
                        ever_enrolled = bool(
                            lifecycle_scope.grant_generation > 0
                            or lifecycle_scope.cp_binding_high_water is not None
                            or lifecycle_scope.agent_lifecycle_high_water is not None
                        )
                        if ever_enrolled and not lifecycle_scope.active:
                            continue
                    conversation = await session.scalar(
                        select(Conversation)
                        .where(Conversation.id == conversation_id, scope_filter(principal))
                        .with_for_update(skip_locked=True)
                        .execution_options(populate_existing=True)
                    )
                    if conversation is None or conversation.control_mode != "AGENT":
                        continue
                    active = await session.scalar(
                        select(AgentTurn)
                        .where(
                            AgentTurn.conversation_id == conversation.id,
                            AgentTurn.status.in_(("PENDING", "CLAIMED")),
                            AgentTurn.retired_at.is_(None),
                        )
                        .with_for_update()
                    )
                    if active is not None:
                        continue
                    catch_up_enabled = catch_up.enabled(settings, binding)
                    rows = await _pending_messages(
                        session,
                        conversation.id,
                        max(
                            conversation.authority_input_floor_sequence if catch_up_enabled else 0,
                            conversation.handoff_resume_floor_sequence or 0,
                        ),
                    )
                    if not rows or rows[0].settling_burst_id is None:
                        continue
                    burst = await session.scalar(
                        select(SettlingBurst)
                        .where(
                            SettlingBurst.id == rows[0].settling_burst_id,
                            SettlingBurst.conversation_id == conversation.id,
                        )
                        .with_for_update()
                        .execution_options(populate_existing=True)
                    )
                    if burst is None:
                        raise DomainError(ErrorCode.VALIDATION_ERROR)
                    clock = await db_clock(session)
                    if burst.closed_at is None:
                        if clock < burst.ready_at:
                            continue
                        burst.closed_at = burst.ready_at
                    if clock < burst.closed_at:
                        continue
                    due_messages: list[Message] = []
                    if catch_up_enabled:
                        due_messages = await catch_up.due_prefix(
                            session,
                            conversation,
                            rows,
                            binding,
                            clock,
                        )
                    for message in () if catch_up_enabled else rows:
                        if message.settling_burst_id != burst.id:
                            break
                        if (
                            message.settling_policy_version != burst.policy_version
                            or message.settling_quiet_window_ms != burst.quiet_window_ms
                            or message.settling_max_wait_ms != burst.max_wait_ms
                        ):
                            raise DomainError(ErrorCode.VALIDATION_ERROR)
                        if due_messages and message.sequence != due_messages[-1].sequence + 1:
                            break
                        due_messages.append(message)
                        if len(due_messages) == MAX_BURST_MESSAGES:
                            break
                    if not due_messages:
                        continue
                    _validate_settled_members(due_messages, multiple_bursts=catch_up_enabled)
                    if channel == Channel.TELEGRAM:
                        account = await session.scalar(
                            select(TelegramProviderAccount).where(
                                TelegramProviderAccount.owner_id == conversation.owner_id,
                                TelegramProviderAccount.integration_id
                                == conversation.integration_id,
                            )
                        )
                        admissions: list[TelegramAdmission] = []
                        for message in due_messages:
                            admission = await session.scalar(
                                select(TelegramAdmission).where(
                                    TelegramAdmission.canonical_message_id == message.id,
                                    TelegramAdmission.conversation_id == conversation.id,
                                )
                            )
                            if (
                                admission is None
                                or admission.route != "MANAGED"
                                or admission.owner_id != conversation.owner_id
                                or admission.integration_id != conversation.integration_id
                                or admission.agent_id != binding.agent_id
                                or admission.provider_thread_id != conversation.provider_thread_id
                                or admission.chat_id != conversation.provider_thread_id
                                or admission.sender_id != admission.chat_id
                                or admission.provider_event_id
                                != (
                                    f"b{admission.bot_id}c{admission.chat_id}m{admission.message_id}"
                                )
                            ):
                                raise DomainError(ErrorCode.VALIDATION_ERROR)
                            admissions.append(admission)
                        if (
                            account is None
                            or account.state != "BOUND"
                            or admissions[0].bot_id != account.bot_id
                        ):
                            continue
                        if any(item.bot_id != admissions[0].bot_id for item in admissions):
                            raise DomainError(ErrorCode.VALIDATION_ERROR)
                    elif channel == Channel.WEBSITE_WIDGET:
                        for message in due_messages:
                            admission = await session.scalar(
                                select(WebsiteAdmission).where(
                                    WebsiteAdmission.canonical_message_id == message.id,
                                    WebsiteAdmission.conversation_id == conversation.id,
                                )
                            )
                            try:
                                session_id_matches = (
                                    admission is not None
                                    and admission.session_id
                                    == UUID(conversation.provider_thread_id)
                                )
                            except ValueError:
                                session_id_matches = False
                            if (
                                admission is None
                                or admission.route != "MANAGED"
                                or admission.agent_id != binding.agent_id
                                or admission.owner_id != conversation.owner_id
                                or admission.integration_id != conversation.integration_id
                                or not session_id_matches
                            ):
                                raise DomainError(ErrorCode.VALIDATION_ERROR)
                    else:
                        continue
                    selected = await _maximal_projectable_prefix(
                        session, conversation, due_messages, binding
                    )
                    if not selected:
                        # A candidate-specific invalid projection remains durable
                        # and cannot prevent later Conversations from making progress.
                        continue
                    return await _create_turn(
                        session,
                        conversation,
                        selected,
                        binding,
                        channel,
                        clock,
                        catch_up_enabled=catch_up_enabled,
                        handoff_enrolled=bool(
                            lifecycle_scope is not None and lifecycle_scope.active
                        ),
                    )
            except DomainError as exc:
                if exc.code == ErrorCode.VALIDATION_ERROR:
                    # This candidate transaction has rolled back before advancing.
                    continue
                raise

        if len(page) < CANDIDATE_PAGE_SIZE:
            return None
        last_conversation_id, last_created_at = page[-1]
        cursor = (last_created_at, last_conversation_id)

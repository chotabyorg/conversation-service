"""Bounded text grouping and independently verifiable, immutable input evidence."""

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import and_, exists, func, select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import ManagedBinding, Settings
from core.errors import DomainError
from models import (
    AgentTurn,
    CatchUpMember,
    CatchUpSelection,
    Conversation,
    Message,
    SettlingBurst,
    TelegramAdmission,
    TelegramProviderAccount,
    TurnInputMessage,
    WebsiteAdmission,
)
from schemas.contracts import ErrorCode
from services.common import fingerprint

Proof = dict[str, Any]


def enabled(settings: Settings, binding: ManagedBinding) -> bool:
    return any(
        (entry.owner_id, entry.integration_id, entry.channel, entry.agent_id)
        == (binding.owner_id, binding.integration_id, binding.channel, binding.agent_id)
        for entry in settings.catch_up_bindings
    )


def policy(message: Message) -> tuple[str | None, int | None, int | None]:
    return (
        message.settling_policy_version,
        message.settling_quiet_window_ms,
        message.settling_max_wait_ms,
    )


def invalid() -> DomainError:
    return DomainError(ErrorCode.VALIDATION_ERROR)


async def identity(
    session: AsyncSession, conversation: Conversation, messages: list[Message], agent_id: UUID
) -> list[Proof]:
    """Exact canonical-admission joins; no mutable admission row stands alone as proof."""
    result: list[Proof] = []
    account = None
    if conversation.channel == "TELEGRAM":
        account = await session.scalar(
            select(TelegramProviderAccount).where(
                TelegramProviderAccount.owner_id == conversation.owner_id,
                TelegramProviderAccount.integration_id == conversation.integration_id,
            )
        )
        if account is None or account.state != "BOUND":
            raise invalid()
    for ordinal, message in enumerate(messages):
        item: Proof = {
            "message_id": str(message.id),
            "sequence": message.sequence,
            "ordinal": ordinal,
            "owner_id": conversation.owner_id,
            "input_revision": message.input_revision,
            "settling_revision": message.settling_revision,
            "integration_id": str(conversation.integration_id),
            "agent_id": str(agent_id),
            "channel": conversation.channel,
        }
        if conversation.channel == "TELEGRAM":
            telegram = await session.scalar(
                select(TelegramAdmission).where(
                    TelegramAdmission.canonical_message_id == message.id,
                    TelegramAdmission.conversation_id == conversation.id,
                )
            )
            if (
                telegram is None
                or account is None
                or telegram.route != "MANAGED"
                or telegram.owner_id != conversation.owner_id
                or telegram.integration_id != conversation.integration_id
                or telegram.channel != conversation.channel
                or telegram.agent_id != agent_id
                or telegram.provider_namespace != "main-chota.telegram.v1"
                or telegram.bot_id != account.bot_id
                or telegram.provider_thread_id != conversation.provider_thread_id
                or telegram.chat_id != conversation.provider_thread_id
                or telegram.sender_id != telegram.chat_id
                or telegram.provider_event_id
                != f"b{telegram.bot_id}c{telegram.chat_id}m{telegram.message_id}"
            ):
                raise invalid()
            item.update(
                admission_id=str(telegram.id),
                fingerprint=telegram.fingerprint,
                route=telegram.route,
                provider_namespace=telegram.provider_namespace,
                bot_id=telegram.bot_id,
                chat_id=telegram.chat_id,
                sender_id=telegram.sender_id,
                provider_message_id=telegram.message_id,
                update_id=telegram.update_id,
                provider_event_id=telegram.provider_event_id,
                provider_thread_id=telegram.provider_thread_id,
                account_state="BOUND",
            )
        elif conversation.channel == "WEBSITE_WIDGET":
            website = await session.scalar(
                select(WebsiteAdmission).where(
                    WebsiteAdmission.canonical_message_id == message.id,
                    WebsiteAdmission.conversation_id == conversation.id,
                )
            )
            if (
                website is None
                or website.route != "MANAGED"
                or website.owner_id != conversation.owner_id
                or website.integration_id != conversation.integration_id
                or website.channel != conversation.channel
                or website.agent_id != agent_id
                or website.provider_namespace != "main-chota.website-widget.v1"
                or str(website.session_id) != conversation.provider_thread_id
            ):
                raise invalid()
            item.update(
                admission_id=str(website.id),
                fingerprint=website.fingerprint,
                route=website.route,
                provider_namespace=website.provider_namespace,
                provider_message_id=str(website.message_id),
                session_id=str(website.session_id),
            )
        else:
            raise invalid()
        result.append(item)
    return result


async def due_prefix(
    session: AsyncSession,
    conversation: Conversation,
    rows: list[Message],
    binding: ManagedBinding,
    clock: datetime,
) -> list[Message]:
    """Stop at the first original boundary; never wait for a later burst."""
    result: list[Message] = []
    first_policy = policy(rows[0])
    for message in rows[:256]:
        if (
            message.sequence <= conversation.authority_input_floor_sequence
            or message.settling_burst_id is None
            or (result and message.sequence != result[-1].sequence + 1)
        ):
            break
        burst = await session.scalar(
            select(SettlingBurst)
            .where(
                SettlingBurst.id == message.settling_burst_id,
                SettlingBurst.conversation_id == conversation.id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if (
            burst is None
            or message.input_revision is None
            or message.input_revision < 1
            or message.settling_revision is None
            or message.settling_revision < 0
            or policy(message) != (burst.policy_version, burst.quiet_window_ms, burst.max_wait_ms)
        ):
            raise invalid()
        if policy(message) != first_policy or clock < burst.ready_at:
            break
        try:
            await identity(session, conversation, [message], binding.agent_id)
        except DomainError:
            if not result:
                raise
            break
        if burst.closed_at is None:
            burst.closed_at = burst.ready_at
        if clock < burst.closed_at:
            break
        result.append(message)
    return result


async def members(
    session: AsyncSession, conversation: Conversation, messages: list[Message]
) -> list[Proof]:
    result: list[Proof] = []
    seen: set[UUID] = set()
    for index, message in enumerate(messages):
        if (
            message.actor != "CUSTOMER"
            or message.direction != "INBOUND"
            or message.sequence != messages[0].sequence + index
            or message.settling_burst_id is None
            or policy(message) != policy(messages[0])
        ):
            raise invalid()
        if not result or result[-1]["burst_id"] != str(message.settling_burst_id):
            if message.settling_burst_id in seen:
                raise invalid()
            seen.add(message.settling_burst_id)
            burst = await session.scalar(
                select(SettlingBurst).where(
                    SettlingBurst.id == message.settling_burst_id,
                    SettlingBurst.conversation_id == conversation.id,
                )
            )
            if (
                burst is None
                or burst.closed_at is None
                or policy(message)
                != (burst.policy_version, burst.quiet_window_ms, burst.max_wait_ms)
            ):
                raise invalid()
            first = await session.scalar(
                select(func.min(Message.sequence)).where(
                    Message.conversation_id == conversation.id,
                    Message.settling_burst_id == burst.id,
                    Message.actor == "CUSTOMER",
                    Message.direction == "INBOUND",
                )
            )
            if first is None or first > message.sequence or burst.tail_sequence < message.sequence:
                raise invalid()
            result.append(
                {
                    "ordinal": len(result),
                    "burst_id": str(burst.id),
                    "first_sequence": message.sequence,
                    "last_sequence": message.sequence,
                    "burst_first_sequence": first,
                    "burst_tail_sequence": burst.tail_sequence,
                    "policy_version": burst.policy_version,
                    "quiet_window_ms": burst.quiet_window_ms,
                    "max_wait_ms": burst.max_wait_ms,
                    "closed_at": burst.closed_at.isoformat(),
                }
            )
        else:
            if message.sequence > result[-1]["burst_tail_sequence"]:
                raise invalid()
            result[-1]["last_sequence"] = message.sequence
    return result


@dataclass(frozen=True)
class Evidence:
    fields: Proof
    members: list[Proof]

    @property
    def digest(self) -> str:
        return fingerprint({**self.fields, "members": self.members})


async def prepare(
    session: AsyncSession, conversation: Conversation, selected: list[Message], agent_id: UUID
) -> Evidence:
    latest = await session.scalar(
        select(func.max(Message.sequence)).where(
            Message.conversation_id == conversation.id,
            Message.actor == "CUSTOMER",
            Message.direction == "INBOUND",
        )
    )
    if not selected or len(selected) > 256 or latest is None:
        raise invalid()
    values = await members(session, conversation, selected)
    return Evidence(
        dict(
            selection_version=1,
            conversation_id=str(conversation.id),
            authority_revision=conversation.authority_revision,
            authority_input_floor_sequence=conversation.authority_input_floor_sequence,
            input_revision=conversation.input_revision,
            settling_revision=conversation.settling_revision,
            first_sequence=selected[0].sequence,
            last_sequence=selected[-1].sequence,
            latest_customer_sequence=latest,
            agent_id=str(agent_id),
            channel=conversation.channel,
            member_count=len(values),
            identity_proof=await identity(session, conversation, selected, agent_id),
        ),
        values,
    )


async def persist(
    session: AsyncSession, turn: AgentTurn, evidence: Evidence, clock: datetime
) -> None:
    values = {
        **evidence.fields,
        "conversation_id": turn.conversation_id,
        "agent_id": UUID(evidence.fields["agent_id"]),
    }
    session.add(
        CatchUpSelection(
            turn_id=turn.id, **values, membership_fingerprint=evidence.digest, created_at=clock
        )
    )
    await session.flush()
    for member in evidence.members:
        session.add(
            CatchUpMember(
                turn_id=turn.id,
                conversation_id=turn.conversation_id,
                **{
                    **member,
                    "burst_id": UUID(member["burst_id"]),
                    "closed_at": datetime.fromisoformat(member["closed_at"]),
                },
            )
        )


async def validate(
    session: AsyncSession, conversation: Conversation, turn: AgentTurn, agent_id: UUID | None = None
) -> bool:
    """Rebuild the frozen proof before new work; retained outcomes take precedence elsewhere."""
    if turn.catch_up_version == 0:
        return True
    if turn.catch_up_version != 1:
        return False
    header = await session.get(CatchUpSelection, turn.id, populate_existing=True)
    if (
        header is None
        or header.conversation_id != conversation.id
        or header.selection_version != 1
        or header.channel != conversation.channel
        or header.authority_revision != turn.authority_revision
        or header.input_revision != turn.input_revision
        or header.settling_revision != turn.settling_revision
        or header.first_sequence != turn.input_first_sequence
        or header.last_sequence != turn.input_last_sequence
        or header.agent_id != turn.managed_projection_agent_id
        or (agent_id is not None and header.agent_id != agent_id)
        or not 1 <= header.last_sequence - header.first_sequence + 1 <= 256
        or header.first_sequence <= header.authority_input_floor_sequence
        or header.latest_customer_sequence < header.last_sequence
    ):
        return False
    rows = (
        await session.execute(
            select(TurnInputMessage, Message)
            .join(
                Message,
                and_(
                    Message.id == TurnInputMessage.message_id,
                    Message.conversation_id == TurnInputMessage.conversation_id,
                    Message.sequence == TurnInputMessage.sequence,
                ),
            )
            .where(
                TurnInputMessage.turn_id == turn.id,
                TurnInputMessage.conversation_id == conversation.id,
            )
            .order_by(TurnInputMessage.ordinal)
            .limit(257)
        )
    ).all()
    if len(rows) != header.last_sequence - header.first_sequence + 1 or any(
        owned.ordinal != ordinal
        or owned.sequence != header.first_sequence + ordinal
        or message.input_revision is None
        or message.input_revision < 1
        or message.settling_revision is None
        or message.settling_revision < 0
        for ordinal, (owned, message) in enumerate(rows)
    ):
        return False
    try:
        selected = [message for _, message in rows]
        values = await members(session, conversation, selected)
        proof = await identity(session, conversation, selected, header.agent_id)
    except DomainError:
        return False
    retained = list(
        (
            await session.scalars(
                select(CatchUpMember)
                .where(
                    CatchUpMember.turn_id == turn.id,
                    CatchUpMember.conversation_id == conversation.id,
                )
                .order_by(CatchUpMember.ordinal)
                .limit(257)
            )
        ).all()
    )
    if (
        len(retained) != header.member_count
        or len(values) != len(retained)
        or proof != header.identity_proof
        or not retained
        or retained[0].burst_id != turn.settling_burst_id
    ):
        return False
    for member, value in zip(retained, values, strict=True):
        if any(
            (
                str(getattr(member, key))
                if key == "burst_id"
                else getattr(member, key).isoformat()
                if key == "closed_at"
                else getattr(member, key)
            )
            != expected
            for key, expected in value.items()
        ):
            return False
    fields: Proof = {
        key: getattr(header, key)
        for key in (
            "selection_version",
            "authority_revision",
            "authority_input_floor_sequence",
            "input_revision",
            "settling_revision",
            "first_sequence",
            "last_sequence",
            "latest_customer_sequence",
            "channel",
            "member_count",
        )
    }
    fields.update(
        conversation_id=str(conversation.id), agent_id=str(header.agent_id), identity_proof=proof
    )
    return Evidence(fields, values).digest == header.membership_fingerprint


async def communication_current(
    session: AsyncSession, conversation: Conversation, turn: AgentTurn
) -> bool:
    if turn.catch_up_version == 0:
        return True
    if not await validate(session, conversation, turn):
        return False
    header = await session.get(CatchUpSelection, turn.id)
    latest = await session.scalar(
        select(func.max(Message.sequence)).where(
            Message.conversation_id == conversation.id,
            Message.actor == "CUSTOMER",
            Message.direction == "INBOUND",
        )
    )
    return bool(
        header is not None
        and header.latest_customer_sequence == turn.input_last_sequence
        and latest == turn.input_last_sequence
        and conversation.input_revision == turn.input_revision
        and conversation.settling_revision == turn.settling_revision
        and turn.input_first_sequence > conversation.authority_input_floor_sequence
    )


async def effect_complete(
    session: AsyncSession, conversation: Conversation, turn: AgentTurn
) -> bool:
    if not await validate(session, conversation, turn):
        return False
    header = await session.get(CatchUpSelection, turn.id)
    if header is None or header.latest_customer_sequence != turn.input_last_sequence:
        return False
    selected = list(
        (
            await session.scalars(
                select(CatchUpMember).where(
                    CatchUpMember.turn_id == turn.id,
                )
            )
        ).all()
    )
    if not selected or any(
        member.first_sequence != member.burst_first_sequence
        or member.last_sequence != member.burst_tail_sequence
        for member in selected
    ):
        return False
    # Confirm every canonical original burst member is owned by this exact Turn.
    return not bool(
        await session.scalar(
            select(
                exists(
                    select(1)
                    .select_from(Message)
                    .where(
                        Message.conversation_id == conversation.id,
                        Message.settling_burst_id.in_([member.burst_id for member in selected]),
                        Message.actor == "CUSTOMER",
                        Message.direction == "INBOUND",
                        ~exists(
                            select(1)
                            .select_from(TurnInputMessage)
                            .where(
                                TurnInputMessage.message_id == Message.id,
                                TurnInputMessage.conversation_id == conversation.id,
                                TurnInputMessage.turn_id == turn.id,
                                TurnInputMessage.sequence == Message.sequence,
                            )
                        ),
                    )
                )
            )
        )
    )

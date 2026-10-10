"""Canonical whole-message Website windows, frozen before claim or dispatch."""
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.errors import DomainError
from models import AgentTurn, Conversation, Message
from schemas.contracts import Channel, ErrorCode
from schemas.managed import (
    MAX_MESSAGES,
    ManagedMessage,
    ManagedTurnRequest,
    PreparedManagedRequest,
    prepare_request,
)


def projected_request(turn: AgentTurn, conversation: Conversation, agent_id: UUID,
                      messages: tuple[ManagedMessage, ...]) -> PreparedManagedRequest:
    return prepare_request(ManagedTurnRequest(
        contract_version=turn.managed_contract_version,
        conversation_id=conversation.id, turn_id=turn.id,
        owner_id=conversation.owner_id, integration_id=conversation.integration_id,
        agent_id=agent_id, channel=Channel(conversation.channel),
        input_revision=turn.input_revision, authority_revision=turn.authority_revision,
        input_first_sequence=turn.input_first_sequence,
        input_last_sequence=turn.input_last_sequence,
        context_last_sequence=turn.context_last_sequence, messages=messages,
        selected_input_message_ids=tuple(m.id for m in messages
            if turn.input_first_sequence <= m.sequence <= turn.input_last_sequence),
    ))


async def canonical_window(session: AsyncSession, conversation_id: UUID,
                           first: int, last: int) -> tuple[ManagedMessage, ...]:
    if not 1 <= first <= last or last - first + 1 > MAX_MESSAGES:
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    rows = (await session.scalars(select(Message).where(
        Message.conversation_id == conversation_id,
        Message.sequence >= first, Message.sequence <= last,
    ).order_by(Message.sequence).limit(MAX_MESSAGES + 1))).all()
    try:
        messages = tuple(ManagedMessage.model_validate(row) for row in rows)
    except ValueError:
        raise DomainError(ErrorCode.VALIDATION_ERROR) from None
    if tuple(m.sequence for m in messages) != tuple(range(first, last + 1)):
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    return messages


def oldest_unit_end(messages: tuple[ManagedMessage, ...], selected_first: int) -> int:
    """Drop a CUSTOMER and its following AGENT rows; boundary singletons fallback."""
    end = 1
    if messages[0].actor == "CUSTOMER":
        while (end < len(messages) and messages[end].sequence < selected_first
               and messages[end].actor == "AGENT"):
            end += 1
    return end


async def freeze_website_projection(session: AsyncSession, conversation: Conversation,
                                    turn: AgentTurn, agent_id: UUID) -> None:
    # A single bounded descending read supplies at most257 whole canonical rows.
    # The first loaded row is a boundary singleton if its earlier unit is outside
    # this candidate. No giant prefix or serialized request is retained.
    if turn.context_last_sequence - turn.input_first_sequence + 1 > MAX_MESSAGES:
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    rows = (await session.scalars(select(Message).where(
        Message.conversation_id == conversation.id,
        Message.sequence <= turn.context_last_sequence,
    ).order_by(Message.sequence.desc()).limit(MAX_MESSAGES + 1))).all()
    try:
        messages = tuple(ManagedMessage.model_validate(row) for row in reversed(rows))
    except ValueError:
        raise DomainError(ErrorCode.VALIDATION_ERROR) from None
    if (not messages or messages[-1].sequence != turn.context_last_sequence
            or tuple(m.sequence for m in messages) != tuple(range(
                messages[0].sequence, turn.context_last_sequence + 1))):
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    turn.managed_contract_version = 2
    while messages:
        try:
            prepared = projected_request(turn, conversation, agent_id, messages)
        except ValueError:
            if messages[0].sequence >= turn.input_first_sequence:
                raise DomainError(ErrorCode.VALIDATION_ERROR) from None
            messages = messages[oldest_unit_end(messages, turn.input_first_sequence):]
            continue
        turn.managed_context_first_sequence = messages[0].sequence
        turn.managed_projection_agent_id = agent_id
        turn.managed_projection_fingerprint = prepared.fingerprint
        return
    raise DomainError(ErrorCode.VALIDATION_ERROR)

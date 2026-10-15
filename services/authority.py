"""Conversation-owned authority CAS, including deliberate same-mode fencing."""

from uuid import UUID

from sqlalchemy import select

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import Conversation, InboxGrantScope
from schemas.contracts import Channel, ControlTransition, ConversationRead, ErrorCode, Principal
from services.common import (
    authorized_conversation,
    db_clock,
    telegram_handoff_was_ever_enrolled,
)


def advance_authority(conversation: Conversation) -> None:
    """Advance authority and fence all input accepted in the previous epoch."""
    # The locked Conversation row is deliberately the single authority/freshness lock.
    conversation.authority_revision += 1
    conversation.authority_input_floor_sequence = conversation.last_message_sequence


async def transition_control(
    database: Database,
    principal: Principal,
    conversation_id: UUID,
    request: ControlTransition,
    settings: Settings,
) -> ConversationRead:
    async with database.session_factory() as session, session.begin():
        # Discover immutable scope without taking the parent lock first. Handoff
        # lifecycle operations use the integration-scope -> Conversation order.
        probe = await authorized_conversation(session, principal, conversation_id)
        if probe.channel == Channel.TELEGRAM.value:
            from services import telegram

            existing_agent_ids = set(
                (
                    await session.scalars(
                        select(InboxGrantScope.agent_id).where(
                            InboxGrantScope.owner_id == probe.owner_id,
                            InboxGrantScope.integration_id == probe.integration_id,
                            InboxGrantScope.channel == Channel.TELEGRAM.value,
                        )
                    )
                ).all()
            )
            configured_agent_ids = {
                binding.agent_id
                for binding in settings.managed_ai_bindings
                if binding.channel == Channel.TELEGRAM
                and binding.owner_id == probe.owner_id
                and binding.integration_id == probe.integration_id
            }
            scopes = [
                await telegram._locked_handoff_scope_values(
                    session, probe.owner_id, probe.integration_id, agent_id
                )
                for agent_id in sorted(existing_agent_ids | configured_agent_ids, key=str)
            ]
            if any(telegram._ever_enrolled(scope) for scope in scopes):
                raise DomainError(ErrorCode.HANDOFF_CONFLICT)

        conversation = await authorized_conversation(session, principal, conversation_id, lock=True)
        if await telegram_handoff_was_ever_enrolled(session, conversation):
            raise DomainError(ErrorCode.HANDOFF_CONFLICT)
        if request.expected_authority_revision != conversation.authority_revision:
            raise DomainError(ErrorCode.AUTHORITY_REVISION_CONFLICT)
        advance_authority(conversation)
        conversation.control_mode = request.control_mode.value
        conversation.updated_at = await db_clock(session)
        await session.flush()
        result = ConversationRead.model_validate(conversation)
    return result

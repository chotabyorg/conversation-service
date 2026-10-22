"""Conversation-owned authority CAS, including deliberate same-mode fencing."""

from uuid import UUID

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import Conversation
from schemas.contracts import ControlTransition, ConversationRead, ErrorCode, Principal
from services.common import (
    authorized_conversation,
    db_clock,
    handoff_was_ever_enrolled,
)
from services.handoff_scope import lock_legacy_scopes


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
        if await lock_legacy_scopes(session, owner_id=probe.owner_id,
                integration_id=probe.integration_id, channel=probe.channel, settings=settings):
            raise DomainError(ErrorCode.HANDOFF_CONFLICT)

        conversation = await authorized_conversation(session, principal, conversation_id, lock=True)
        if await handoff_was_ever_enrolled(session, conversation):
            raise DomainError(ErrorCode.HANDOFF_CONFLICT)
        if request.expected_authority_revision != conversation.authority_revision:
            raise DomainError(ErrorCode.AUTHORITY_REVISION_CONFLICT)
        advance_authority(conversation)
        conversation.control_mode = request.control_mode.value
        conversation.updated_at = await db_clock(session)
        await session.flush()
        result = ConversationRead.model_validate(conversation)
    return result

"""Conversation-owned authority CAS, including deliberate same-mode fencing."""

from uuid import UUID

from core.errors import DomainError
from db.database import Database
from models import Conversation
from schemas.contracts import ControlTransition, ConversationRead, ErrorCode, Principal
from services.common import authorized_conversation, db_clock


def advance_authority(conversation: Conversation) -> None:
    """Advance authority and fence all input accepted in the previous epoch."""
    # The locked Conversation row is deliberately the single authority/freshness lock.
    conversation.authority_revision += 1
    conversation.authority_input_floor_sequence = conversation.last_message_sequence


async def transition_control(
    database: Database, principal: Principal, conversation_id: UUID, request: ControlTransition
) -> ConversationRead:
    async with database.session_factory() as session, session.begin():
        conversation = await authorized_conversation(session, principal, conversation_id, lock=True)
        if request.expected_authority_revision != conversation.authority_revision:
            raise DomainError(ErrorCode.AUTHORITY_REVISION_CONFLICT)
        advance_authority(conversation)
        conversation.control_mode = request.control_mode.value
        conversation.updated_at = await db_clock(session)
        await session.flush()
        result = ConversationRead.model_validate(conversation)
    return result

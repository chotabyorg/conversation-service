"""Atomic normalized event receipt, identity resolution and ordered Message append."""

from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import Conversation, InboundEvent, Message
from schemas.contracts import (
    ConversationRead,
    ErrorCode,
    InboundEventCreate,
    InboundEventRead,
    InboundEventResult,
    MessagePage,
    MessageRead,
    Principal,
)
from services.common import authorized_conversation, db_clock, fingerprint, scope_filter


def event_filter(request: InboundEventCreate) -> tuple[ColumnElement[bool], ...]:
    return (
        InboundEvent.owner_id == request.owner_id,
        InboundEvent.integration_id == request.integration_id,
        InboundEvent.channel == request.channel.value,
        InboundEvent.provider_namespace == request.provider_namespace,
        InboundEvent.provider_event_id == request.provider_event_id,
    )


async def replay_result(
    session: AsyncSession, principal: Principal, event: InboundEvent, digest: str
) -> InboundEventResult:
    if event.fingerprint != digest:
        raise DomainError(ErrorCode.EVENT_CONFLICT)
    conversation = await authorized_conversation(session, principal, event.conversation_id)
    message = await session.scalar(
        select(Message).where(
            Message.source_inbound_event_id == event.id,
            Message.conversation_id == conversation.id,
        )
    )
    if message is None:
        raise RuntimeError("Accepted event has no canonical message")
    return InboundEventResult(
        conversation=ConversationRead.model_validate(conversation),
        event=InboundEventRead.model_validate(event),
        message=MessageRead.model_validate(message),
        replayed=True,
    )


async def ingest_event(
    database: Database, principal: Principal, request: InboundEventCreate, settings: Settings
) -> InboundEventResult:
    if not any(
        grant.owner_id == request.owner_id
        and grant.integration_id == request.integration_id
        and grant.channel == request.channel
        and grant.provider_namespace == request.provider_namespace
        for grant in principal.grants
    ):
        raise DomainError(ErrorCode.NOT_FOUND)
    canonical = request.model_dump(mode="json")
    canonical.update(event_kind="TEXT", actor="CUSTOMER", direction="INBOUND")
    digest = fingerprint(canonical)
    async with database.session_factory() as session, session.begin():
        existing = await session.scalar(select(InboundEvent).where(*event_filter(request)))
        if existing is not None:
            result = await replay_result(session, principal, existing, digest)
        else:
            # One separate statement after ON CONFLICT sees an uncommitted winner's
            # now-committed row at READ COMMITTED. A same-statement CTE cannot do so.
            await session.execute(
                insert(Conversation)
                .values(
                    id=uuid4(),
                    owner_id=request.owner_id,
                    integration_id=request.integration_id,
                    channel=request.channel.value,
                    provider_thread_id=request.provider_thread_id,
                )
                .on_conflict_do_nothing(constraint="uq_conversation_identity")
            )
            conversation = await session.scalar(
                select(Conversation)
                .where(
                    Conversation.owner_id == request.owner_id,
                    Conversation.integration_id == request.integration_id,
                    Conversation.channel == request.channel.value,
                    Conversation.provider_thread_id == request.provider_thread_id,
                    scope_filter(principal),
                )
                .with_for_update()
                .execution_options(populate_existing=True)
            )
            if conversation is None:
                raise DomainError(ErrorCode.NOT_FOUND)
            accepted_at = await db_clock(session)
            new_event_id = uuid4()
            inserted_id = await session.scalar(
                insert(InboundEvent)
                .values(
                    id=new_event_id,
                    conversation_id=conversation.id,
                    owner_id=request.owner_id,
                    integration_id=request.integration_id,
                    channel=request.channel.value,
                    provider_namespace=request.provider_namespace,
                    provider_event_id=request.provider_event_id,
                    contract_version=1,
                    fingerprint=digest,
                    provider_occurred_at=request.provider_occurred_at,
                    accepted_at=accepted_at,
                )
                .on_conflict_do_nothing(constraint="uq_inbound_event_scope")
                .returning(InboundEvent.id)
            )
            if inserted_id is None:
                winner = await session.scalar(select(InboundEvent).where(*event_filter(request)))
                if winner is None:
                    raise RuntimeError("Event conflict winner unavailable")
                # A changed-thread conflict raises through session.begin: the whole
                # transaction rolls back, including any candidate Conversation.
                result = await replay_result(session, principal, winner, digest)
            else:
                conversation.last_message_sequence += 1
                conversation.input_revision += 1
                # Every accepted customer input fences an older draft, including
                # input admitted while settling policy is disabled.
                conversation.settling_revision += 1
                conversation.updated_at = accepted_at
                message = Message(
                    id=uuid4(),
                    conversation_id=conversation.id,
                    sequence=conversation.last_message_sequence,
                    input_revision=conversation.input_revision,
                    settling_revision=conversation.settling_revision,
                    # Generic journal ingress has no guaranteed managed selector.
                    # Policy snapshots belong to channel-specific managed admission.
                    settling_policy_version=None,
                    settling_quiet_window_ms=None,
                    settling_max_wait_ms=None,
                    actor="CUSTOMER",
                    direction="INBOUND",
                    content_type="TEXT",
                    text=request.text,
                    source_inbound_event_id=new_event_id,
                    created_at=accepted_at,
                )
                session.add(message)
                await session.flush()
                event = await session.scalar(
                    select(InboundEvent).where(InboundEvent.id == new_event_id)
                )
                if event is None:
                    raise RuntimeError("Inserted event unavailable")
                result = InboundEventResult(
                    conversation=ConversationRead.model_validate(conversation),
                    event=InboundEventRead.model_validate(event),
                    message=MessageRead.model_validate(message),
                    replayed=False,
                )
    return result


async def get_conversation(
    database: Database, principal: Principal, conversation_id: UUID
) -> ConversationRead:
    async with database.session_factory() as session, session.begin():
        return ConversationRead.model_validate(
            await authorized_conversation(session, principal, conversation_id)
        )


async def list_messages(
    database: Database,
    principal: Principal,
    conversation_id: UUID,
    *,
    after_sequence: int = 0,
    limit: int = 50,
) -> MessagePage:
    async with database.session_factory() as session, session.begin():
        await authorized_conversation(session, principal, conversation_id)
        rows = (
            await session.scalars(
                select(Message)
                .where(
                    Message.conversation_id == conversation_id, Message.sequence > after_sequence
                )
                .order_by(Message.sequence)
                .limit(limit + 1)
            )
        ).all()
        return MessagePage(
            items=[MessageRead.model_validate(row) for row in rows[:limit]],
            next_after_sequence=rows[limit - 1].sequence if len(rows) > limit else None,
        )


async def get_message(database: Database, principal: Principal, message_id: UUID) -> MessageRead:
    async with database.session_factory() as session, session.begin():
        message = await session.scalar(
            select(Message)
            .join(Conversation, Conversation.id == Message.conversation_id)
            .where(Message.id == message_id, scope_filter(principal))
        )
        if message is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        return MessageRead.model_validate(message)

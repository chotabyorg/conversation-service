"""Shared scope, row-lock and canonical equality primitives."""

import hashlib
import json
from contextvars import ContextVar, Token
from datetime import datetime
from typing import Any
from uuid import UUID

from sqlalchemy import and_, exists, false, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from core.errors import DomainError
from models import (
    AgentTurn,
    Conversation,
    DeliveryAttempt,
    InboxGrantScope,
    Message,
    TurnInputMessage,
)
from schemas.contracts import ErrorCode, Principal

_TEST_DATABASE_CLOCK: ContextVar[datetime | None] = ContextVar(
    "conversation_test_database_clock", default=None
)


def set_test_database_clock(value: datetime) -> Token[datetime | None]:
    """Install a deterministic clock for the current test/process context."""
    return _TEST_DATABASE_CLOCK.set(value)


def reset_test_database_clock(token: Token[datetime | None]) -> None:
    _TEST_DATABASE_CLOCK.reset(token)


def advance_test_database_clock(value: datetime) -> None:
    """Advance the explicit test clock; production contexts keep using PostgreSQL."""
    _TEST_DATABASE_CLOCK.set(value)


def current_test_database_clock() -> datetime | None:
    return _TEST_DATABASE_CLOCK.get()


def scope_filter(principal: Principal) -> ColumnElement[bool]:
    # Each OR arm is an actual complete grant triple, never scalar allowlists.
    return or_(
        false(),
        *(
            and_(
                Conversation.owner_id == grant.owner_id,
                Conversation.integration_id == grant.integration_id,
                Conversation.channel == grant.channel.value,
            )
            for grant in principal.grants
        )
    )


def message_is_consumed(message: Any = Message) -> ColumnElement[bool]:
    """Alias-safe global ownership across explicit mappings and retained Turn ranges."""
    input_owner = aliased(TurnInputMessage)
    frozen_turn = aliased(AgentTurn)
    return exists(select(1).select_from(input_owner).where(
        input_owner.message_id == message.id
    )) | exists(select(1).select_from(frozen_turn).where(
        frozen_turn.conversation_id == message.conversation_id,
        message.sequence >= frozen_turn.input_first_sequence,
        message.sequence <= frozen_turn.input_last_sequence,
    ))


async def authorized_conversation(
    session: AsyncSession, principal: Principal, conversation_id: UUID, *, lock: bool = False
) -> Conversation:
    statement = select(Conversation).where(
        Conversation.id == conversation_id, scope_filter(principal)
    )
    if lock:
        # A lock waiter must evaluate the row it acquires, not an old identity-map copy.
        statement = statement.with_for_update().execution_options(populate_existing=True)
    conversation = await session.scalar(statement)
    if conversation is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    return conversation


async def handoff_was_ever_enrolled(
    session: AsyncSession, conversation: Conversation
) -> bool:
    """Recheck the durable text enrollment fence after locking its parent.

    Generic legacy writers normally lock an existing/configured grant scope
    before the Conversation. A stale settings instance can miss a scope that
    enrollment creates while the writer waits for the Conversation lock. This
    read-only post-lock check closes that gap without taking the scope lock in
    reverse order.
    """
    if conversation.channel not in {"TELEGRAM", "WEBSITE_WIDGET"}:
        return False
    return bool(
        await session.scalar(
            select(InboxGrantScope.owner_id)
            .where(
                InboxGrantScope.owner_id == conversation.owner_id,
                InboxGrantScope.integration_id == conversation.integration_id,
                InboxGrantScope.channel == conversation.channel,
                or_(
                    InboxGrantScope.grant_generation > 0,
                    InboxGrantScope.cp_binding_high_water.is_not(None),
                    InboxGrantScope.agent_lifecycle_high_water.is_not(None),
                ),
            )
            .limit(1)
        )
    )


async def locked_turn(
    session: AsyncSession, principal: Principal, turn_id: UUID
) -> tuple[Conversation, AgentTurn]:
    # ID-only lookup discovers the parent without loading stale mutable ORM fields.
    parent_id = await session.scalar(
        select(AgentTurn.conversation_id)
        .join(Conversation, Conversation.id == AgentTurn.conversation_id)
        .where(AgentTurn.id == turn_id, scope_filter(principal))
    )
    if parent_id is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    conversation = await authorized_conversation(session, principal, parent_id, lock=True)
    turn = await session.scalar(
        select(AgentTurn)
        .where(AgentTurn.id == turn_id, AgentTurn.conversation_id == conversation.id)
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if turn is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    return conversation, turn


async def locked_delivery(
    session: AsyncSession, principal: Principal, delivery_id: UUID
) -> tuple[Conversation, DeliveryAttempt]:
    parent_id = await session.scalar(
        select(DeliveryAttempt.conversation_id)
        .join(Conversation, Conversation.id == DeliveryAttempt.conversation_id)
        .where(DeliveryAttempt.id == delivery_id, scope_filter(principal))
    )
    if parent_id is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    conversation = await authorized_conversation(session, principal, parent_id, lock=True)
    attempt = await session.scalar(
        select(DeliveryAttempt)
        .where(
            DeliveryAttempt.id == delivery_id, DeliveryAttempt.conversation_id == conversation.id
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if attempt is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    return conversation, attempt


async def db_clock(session: AsyncSession) -> datetime:
    # Callers sample only after their final relevant lock has been acquired.
    test_time = _TEST_DATABASE_CLOCK.get()
    if test_time is not None:
        return test_time
    value = await session.scalar(select(func.clock_timestamp()))
    if not isinstance(value, datetime):
        raise RuntimeError("Database clock unavailable")
    return value


def fingerprint(fields: dict[str, object]) -> str:
    encoded = json.dumps(
        fields, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def unique_violation(exc: IntegrityError, constraint_name: str) -> bool:
    original = exc.orig
    cause = getattr(original, "__cause__", None)
    return (
        getattr(original, "sqlstate", None) == "23505"
        and getattr(cause, "constraint_name", None) == constraint_name
    )

"""Shared scope, row-lock and canonical equality primitives."""

import hashlib
import json
from datetime import datetime
from uuid import UUID

from sqlalchemy import and_, func, or_, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from core.errors import DomainError
from models import AgentTurn, Conversation, DeliveryAttempt
from schemas.contracts import ErrorCode, Principal


def scope_filter(principal: Principal) -> ColumnElement[bool]:
    # Each OR arm is an actual complete grant triple, never scalar allowlists.
    return or_(
        *(
            and_(
                Conversation.owner_id == grant.owner_id,
                Conversation.integration_id == grant.integration_id,
                Conversation.channel == grant.channel.value,
            )
            for grant in principal.grants
        )
    )


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

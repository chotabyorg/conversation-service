"""Conversation-owned authority checks and immutable opaque effect permits."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import and_, exists, func, select, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import aliased
from sqlalchemy.sql.elements import ColumnElement

from core.errors import DomainError
from db.database import Database
from models import (
    AgentTurn,
    Conversation,
    ExternalEffectPermit,
    Message,
    SettlingBurst,
    TurnInputMessage,
)
from schemas.contracts import ErrorCode, Principal
from schemas.effects import (
    EffectReason,
    ExternalEffectContext,
    ExternalEffectEligibilityRequest,
    ExternalEffectEligibilityResult,
    ExternalEffectPermitRequest,
    ExternalEffectPermitResult,
    request_fingerprint,
)
from services import catch_up
from services.common import authorized_conversation, db_clock, unique_violation


class EffectServiceConflict(Exception):
    def __init__(self, code: str, reason: EffectReason | None = None) -> None:
        super().__init__(code)
        self.code = code
        self.reason = reason


def _customer_input(message: Any = Message) -> ColumnElement[bool]:
    return and_(message.actor == "CUSTOMER", message.direction == "INBOUND")


async def _exists(session: AsyncSession, statement: Any) -> bool:
    return bool(await session.scalar(select(exists(statement))))


async def _input_complete(
    session: AsyncSession, conversation: Conversation, turn: AgentTurn
) -> bool:
    customer = _customer_input()
    # Every canonical customer message in the frozen range must be selected by this Turn.
    missing_range_member = await _exists(session, select(1).select_from(Message).where(
        Message.conversation_id == conversation.id,
        Message.sequence.between(turn.input_first_sequence, turn.input_last_sequence),
        customer,
        ~exists(select(1).select_from(TurnInputMessage).where(
            TurnInputMessage.message_id == Message.id,
            TurnInputMessage.conversation_id == conversation.id,
            TurnInputMessage.turn_id == turn.id,
            TurnInputMessage.sequence == Message.sequence,
        )),
    ))
    if missing_range_member:
        return False

    # A forged or damaged ownership row outside the immutable range cannot expand inputs.
    invalid_owned_member = await _exists(session, select(1).select_from(TurnInputMessage).join(
        Message, and_(Message.id == TurnInputMessage.message_id,
                      Message.conversation_id == TurnInputMessage.conversation_id,
                      Message.sequence == TurnInputMessage.sequence),
    ).where(
        TurnInputMessage.turn_id == turn.id,
        TurnInputMessage.conversation_id == conversation.id,
        (Message.sequence < turn.input_first_sequence)
        | (Message.sequence > turn.input_last_sequence)
        | ~customer,
    ))
    if invalid_owned_member:
        return False

    selected_count = await session.scalar(select(func.count()).select_from(
        TurnInputMessage
    ).join(Message, and_(Message.id == TurnInputMessage.message_id,
                         Message.conversation_id == TurnInputMessage.conversation_id,
                         Message.sequence == TurnInputMessage.sequence)).where(
        TurnInputMessage.turn_id == turn.id,
        TurnInputMessage.conversation_id == conversation.id,
        Message.sequence.between(turn.input_first_sequence, turn.input_last_sequence),
        customer,
    ))
    if not selected_count:
        return False

    if turn.catch_up_version:
        return await catch_up.effect_complete(session, conversation, turn)

    if turn.settling_burst_id is None:
        return not await _exists(session, select(1).select_from(TurnInputMessage).join(
            Message, and_(Message.id == TurnInputMessage.message_id,
                          Message.conversation_id == TurnInputMessage.conversation_id,
                          Message.sequence == TurnInputMessage.sequence),
        ).where(
            TurnInputMessage.turn_id == turn.id,
            TurnInputMessage.conversation_id == conversation.id,
            Message.sequence.between(turn.input_first_sequence, turn.input_last_sequence),
            customer,
            Message.settling_burst_id.is_not(None),
        ))

    burst = await session.scalar(select(SettlingBurst).where(
        SettlingBurst.id == turn.settling_burst_id,
        SettlingBurst.conversation_id == conversation.id,
    ))
    if burst is None or burst.closed_at is None:
        return False

    # No selected customer may come from another burst, and every canonical member
    # of this immutable burst must belong to this Turn (without a bounded prefix).
    selected_other_burst = await _exists(session, select(1).select_from(
        TurnInputMessage
    ).join(Message, and_(Message.id == TurnInputMessage.message_id,
                         Message.conversation_id == TurnInputMessage.conversation_id,
                         Message.sequence == TurnInputMessage.sequence)).where(
        TurnInputMessage.turn_id == turn.id,
        TurnInputMessage.conversation_id == conversation.id,
        Message.sequence.between(turn.input_first_sequence, turn.input_last_sequence),
        customer,
        Message.settling_burst_id.is_(None)
        | (Message.settling_burst_id != turn.settling_burst_id),
    ))
    if selected_other_burst:
        return False

    burst_member = aliased(Message)
    missing_burst_member = await _exists(session, select(1).select_from(burst_member).where(
        burst_member.conversation_id == conversation.id,
        burst_member.settling_burst_id == turn.settling_burst_id,
        _customer_input(burst_member),
        ~exists(select(1).select_from(TurnInputMessage).where(
            TurnInputMessage.message_id == burst_member.id,
            TurnInputMessage.conversation_id == conversation.id,
            TurnInputMessage.turn_id == turn.id,
            TurnInputMessage.sequence == burst_member.sequence,
        )),
    ))
    return not missing_burst_member


async def _split_floor(
    session: AsyncSession, conversation_id: UUID, origin_tail: int, current_tail: int
) -> int:
    # Keep the correlated proof explicit: the burst contains a canonical
    # customer message owned by the candidate Turn and at least one other
    # canonical member not owned by that same Turn. Reusing a SQLAlchemy
    # aliased Message in both nested EXISTS clauses obscures the correlation
    # in generated SQL, so use one bounded raw query here.
    value = await session.scalar(text(
        "SELECT burst.tail_sequence "
        "FROM settling_bursts AS burst "
        "WHERE burst.conversation_id=:conversation_id "
        "AND burst.tail_sequence > :origin_tail "
        "AND burst.tail_sequence <= :current_tail "
        "AND (EXISTS ("
        " SELECT 1 FROM catch_up_members AS member "
        " JOIN agent_turns AS selected ON selected.id=member.turn_id "
        " AND selected.conversation_id=member.conversation_id "
        " WHERE member.conversation_id=burst.conversation_id AND member.burst_id=burst.id "
        " AND selected.catch_up_version=1 "
        " AND (member.first_sequence>member.burst_first_sequence "
        " OR member.last_sequence<member.burst_tail_sequence)"
        ") OR EXISTS ("
        "  SELECT 1 FROM agent_turns AS candidate "
        "  WHERE candidate.conversation_id=burst.conversation_id "
        "    AND candidate.settling_burst_id=burst.id "
        "    AND EXISTS ("
        "      SELECT 1 FROM turn_input_messages AS owned "
        "      JOIN messages AS owned_message "
        "        ON owned_message.id=owned.message_id "
        "       AND owned_message.conversation_id=owned.conversation_id "
        "       AND owned_message.sequence=owned.sequence "
        "      WHERE owned.turn_id=candidate.id "
        "        AND owned.conversation_id=burst.conversation_id "
        "        AND owned_message.settling_burst_id=burst.id "
        "        AND owned_message.actor='CUSTOMER' "
        "        AND owned_message.direction='INBOUND'"
        "    ) "
        "    AND EXISTS ("
        "      SELECT 1 FROM messages AS missing_message "
        "      WHERE missing_message.conversation_id=burst.conversation_id "
        "        AND missing_message.settling_burst_id=burst.id "
        "        AND missing_message.actor='CUSTOMER' "
        "        AND missing_message.direction='INBOUND' "
        "        AND NOT EXISTS ("
        "          SELECT 1 FROM turn_input_messages AS assignment "
        "          WHERE assignment.turn_id=candidate.id "
        "            AND assignment.conversation_id=burst.conversation_id "
        "            AND assignment.message_id=missing_message.id "
        "            AND assignment.sequence=missing_message.sequence"
        "        )"
        "    )"
        ")) "
        "ORDER BY burst.tail_sequence DESC LIMIT 1"
    ), {
        "conversation_id": conversation_id,
        "origin_tail": origin_tail,
        "current_tail": current_tail,
    })
    return int(value or 0)


async def _current_input_reason(
    session: AsyncSession, conversation: Conversation, turn: AgentTurn
) -> EffectReason | None:
    if await _exists(session, select(1).select_from(TurnInputMessage).join(
        Message, and_(Message.id == TurnInputMessage.message_id,
                      Message.conversation_id == TurnInputMessage.conversation_id,
                      Message.sequence == TurnInputMessage.sequence),
    ).where(
        TurnInputMessage.turn_id == turn.id,
        TurnInputMessage.conversation_id == conversation.id,
        Message.sequence.between(turn.input_first_sequence, turn.input_last_sequence),
        _customer_input(),
        Message.sequence <= conversation.authority_input_floor_sequence,
    )):
        return EffectReason.INPUT_BEFORE_AUTHORITY

    if turn.input_revision != conversation.input_revision:
        return EffectReason.INPUT_SUPERSEDED
    newer_input = await session.scalar(select(Message.sequence).where(
        Message.conversation_id == conversation.id,
        _customer_input(),
        Message.sequence > turn.input_last_sequence,
    ).order_by(Message.sequence.desc()).limit(1))
    last_selected_revision = await session.scalar(select(Message.input_revision).join(
        TurnInputMessage,
        and_(TurnInputMessage.message_id == Message.id,
             TurnInputMessage.conversation_id == Message.conversation_id,
             TurnInputMessage.sequence == Message.sequence),
    ).where(
        TurnInputMessage.turn_id == turn.id,
        TurnInputMessage.conversation_id == conversation.id,
        Message.sequence.between(turn.input_first_sequence, turn.input_last_sequence),
        _customer_input(),
    ).order_by(Message.sequence.desc()).limit(1))
    if newer_input is not None or last_selected_revision != conversation.input_revision:
        return EffectReason.INPUT_SUPERSEDED
    if (turn.settling_burst_id is not None
            and turn.settling_revision != conversation.settling_revision):
        return EffectReason.INPUT_SUPERSEDED
    return None


async def _context_rows(
    session: AsyncSession, principal: Principal, request: ExternalEffectContext
) -> tuple[Conversation, AgentTurn, AgentTurn]:
    conversation = await authorized_conversation(
        session, principal, request.conversation_id, lock=True
    )
    if (conversation.owner_id != request.owner_id
            or conversation.integration_id != request.integration_id
            or conversation.channel != request.channel.value):
        raise DomainError(ErrorCode.NOT_FOUND)

    ids = sorted({request.turn_id, request.origin_turn_id}, key=str)
    turns = (await session.scalars(select(AgentTurn).where(
        AgentTurn.id.in_(ids), AgentTurn.conversation_id == conversation.id,
    ).order_by(AgentTurn.id).with_for_update().execution_options(populate_existing=True))).all()
    by_id = {turn.id: turn for turn in turns}
    current = by_id.get(request.turn_id)
    origin = by_id.get(request.origin_turn_id)
    if current is None or origin is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    # A not-yet-marked execution is an ordinary inactive Turn. A present but
    # contradictory immutable marker is a context conflict and must fail closed.
    if ((current.managed_agent_id is not None
         and current.managed_agent_id != request.agent_id)
            or (current.managed_request_fingerprint is not None
                and current.managed_request_fingerprint
                != request.managed_request_fingerprint)
            or ((current.managed_agent_id is None)
                != (current.managed_request_fingerprint is None))):
        raise EffectServiceConflict("EXTERNAL_EFFECT_CONTEXT_CONFLICT")
    return conversation, current, origin


@dataclass(frozen=True)
class Eligibility:
    reason: EffectReason
    split_floor: int

    @property
    def eligible(self) -> bool:
        return self.reason is EffectReason.ELIGIBLE


async def _eligibility_locked(
    session: AsyncSession,
    conversation: Conversation,
    current: AgentTurn,
    origin: AgentTurn,
    clock: datetime,
    requested_authority_revision: int,
) -> Eligibility:
    split_floor = await _split_floor(
        session, conversation.id, origin.input_last_sequence, current.input_last_sequence
    )
    if (conversation.control_mode != "AGENT"
            or current.authority_revision != conversation.authority_revision
            or requested_authority_revision != conversation.authority_revision):
        return Eligibility(EffectReason.AUTHORITY_CHANGED, split_floor)
    if (current.status != "CLAIMED" or current.lease_expires_at is None
            or current.lease_expires_at <= clock
            or current.managed_dispatch_epoch is None
            or current.managed_dispatch_epoch != current.claim_epoch
            or current.managed_dispatch_started_at is None
            or current.managed_request_fingerprint is None):
        return Eligibility(EffectReason.EXECUTION_INACTIVE, split_floor)
    if origin.authority_revision != conversation.authority_revision:
        return Eligibility(EffectReason.ORIGIN_STALE, split_floor)
    if current.id != origin.id and (
        origin.managed_agent_id is None
        or origin.managed_agent_id != current.managed_agent_id
        or origin.managed_dispatch_epoch is None
        or origin.managed_dispatch_started_at is None
        or origin.managed_request_fingerprint is None
    ):
        return Eligibility(EffectReason.ORIGIN_STALE, split_floor)
    if await _exists(session, select(1).select_from(TurnInputMessage).join(
        Message, and_(Message.id == TurnInputMessage.message_id,
                      Message.conversation_id == TurnInputMessage.conversation_id,
                      Message.sequence == TurnInputMessage.sequence),
    ).where(
        TurnInputMessage.turn_id == origin.id,
        TurnInputMessage.conversation_id == conversation.id,
        _customer_input(),
        Message.sequence <= conversation.authority_input_floor_sequence,
    )):
        return Eligibility(EffectReason.ORIGIN_STALE, split_floor)
    if not await _input_complete(session, conversation, origin):
        return Eligibility(EffectReason.ORIGIN_INCOMPLETE, split_floor)
    if not await _input_complete(session, conversation, current):
        return Eligibility(EffectReason.INPUT_INCOMPLETE, split_floor)
    # The initial-intent WRITE_SAFE case explicitly uses the same Turn as
    # origin and current. Strictly-later membership applies only when a prior
    # proposal is being confirmed by a distinct, fresh Turn.
    if current.id != origin.id and await _exists(
        session, select(1).select_from(TurnInputMessage).join(
            Message, and_(Message.id == TurnInputMessage.message_id,
                          Message.conversation_id == TurnInputMessage.conversation_id,
                          Message.sequence == TurnInputMessage.sequence),
        ).where(
            TurnInputMessage.turn_id == current.id,
            TurnInputMessage.conversation_id == conversation.id,
            _customer_input(),
            Message.sequence <= origin.input_last_sequence,
        )
    ):
        return Eligibility(EffectReason.ORIGIN_STALE, split_floor)
    input_reason = await _current_input_reason(session, conversation, current)
    if input_reason is not None:
        return Eligibility(input_reason, split_floor)
    if split_floor:
        return Eligibility(EffectReason.INTERVENING_SPLIT, split_floor)
    return Eligibility(EffectReason.ELIGIBLE, 0)


async def eligibility(
    database: Database,
    principal: Principal,
    request: ExternalEffectEligibilityRequest,
) -> ExternalEffectEligibilityResult:
    async with database.session_factory() as session, session.begin():
        await session.execute(text("SET LOCAL statement_timeout = '3000ms'"))
        conversation, current, origin = await _context_rows(session, principal, request)
        clock = await db_clock(session)
        decision = await _eligibility_locked(
            session, conversation, current, origin, clock, request.authority_revision
        )
        result = ExternalEffectEligibilityResult(
            contract_version=1,
            request_fingerprint=request_fingerprint(request),
            eligible=decision.eligible,
            reason=decision.reason,
            authority_revision=conversation.authority_revision,
            authority_input_floor_sequence=conversation.authority_input_floor_sequence,
            split_input_floor_sequence=decision.split_floor,
        )
    return result


async def permit(
    database: Database,
    principal: Principal,
    request: ExternalEffectPermitRequest,
) -> ExternalEffectPermitResult:
    request_hash = request_fingerprint(request)
    try:
        async with database.session_factory() as session, session.begin():
            await session.execute(text("SET LOCAL statement_timeout = '3000ms'"))
            conversation, current, origin = await _context_rows(session, principal, request)
            prior = await session.scalar(select(ExternalEffectPermit).where(
                ExternalEffectPermit.operation_namespace == request.operation_namespace,
                ExternalEffectPermit.operation_id == request.operation_id,
            ))
            if prior is not None:
                if prior.conversation_id != conversation.id:
                    raise DomainError(ErrorCode.NOT_FOUND)
                if (prior.request_fingerprint != request_hash
                        or prior.operation_fingerprint != request.operation_fingerprint):
                    raise EffectServiceConflict("PERMIT_CONFLICT")
                result = ExternalEffectPermitResult(
                    contract_version=1,
                    request_fingerprint=request_hash,
                    permit_id=prior.id,
                    replayed=True,
                )
            else:
                clock = await db_clock(session)
                decision = await _eligibility_locked(
                    session, conversation, current, origin, clock,
                    request.authority_revision,
                )
                if not decision.eligible:
                    raise EffectServiceConflict("EXTERNAL_EFFECT_DENIED", decision.reason)
                count = await session.scalar(select(func.count()).select_from(
                    ExternalEffectPermit
                ).where(
                    ExternalEffectPermit.conversation_id == conversation.id,
                    ExternalEffectPermit.turn_id == current.id,
                ))
                if count is not None and count >= 32:
                    raise EffectServiceConflict("PERMIT_CONFLICT")
                permit_id = uuid4()
                session.add(ExternalEffectPermit(
                    id=permit_id,
                    conversation_id=conversation.id,
                    turn_id=current.id,
                    origin_turn_id=origin.id,
                    operation_namespace=request.operation_namespace,
                    operation_id=request.operation_id,
                    operation_fingerprint=request.operation_fingerprint,
                    request_fingerprint=request_hash,
                    agent_id=request.agent_id,
                    authority_revision=conversation.authority_revision,
                    authority_input_floor_sequence=conversation.authority_input_floor_sequence,
                    split_input_floor_sequence=decision.split_floor,
                    issued_at=clock,
                ))
                await session.flush()
                result = ExternalEffectPermitResult(
                    contract_version=1,
                    request_fingerprint=request_hash,
                    permit_id=permit_id,
                    replayed=False,
                )
        # Leaving session.begin positively commits before the result is returned.
        return result
    except IntegrityError as error:
        if unique_violation(error, "uq_effect_permit_operation"):
            async with database.session_factory() as session:
                owner = await session.scalar(select(ExternalEffectPermit.conversation_id).where(
                    ExternalEffectPermit.operation_namespace == request.operation_namespace,
                    ExternalEffectPermit.operation_id == request.operation_id,
                ))
            if owner is not None and owner != request.conversation_id:
                raise DomainError(ErrorCode.NOT_FOUND) from None
            raise EffectServiceConflict("PERMIT_CONFLICT") from None
        raise

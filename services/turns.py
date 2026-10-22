"""Frozen selections and same-identity lease recovery, fenced by locked DB state."""

import hashlib
import hmac
import secrets
from datetime import timedelta
from uuid import UUID, uuid4

from sqlalchemy import or_, select
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import AgentTurn, Conversation, Message, TurnInputMessage
from schemas.contracts import (
    ErrorCode,
    Principal,
    TurnClaim,
    TurnClaimResult,
    TurnComplete,
    TurnCreate,
    TurnFail,
    TurnRead,
    TurnRenew,
    TurnResult,
)
from services.common import (
    authorized_conversation,
    db_clock,
    fingerprint,
    handoff_was_ever_enrolled,
    locked_turn,
    message_is_consumed,
    scope_filter,
    unique_violation,
)
from services.handoff_scope import lock_legacy_scopes


async def create_turn(
    database: Database,
    principal: Principal,
    conversation_id: UUID,
    request: TurnCreate,
    *,
    settings: Settings | None = None,
) -> TurnResult:
    digest = fingerprint(
        {
            "contract_version": 1,
            "conversation_id": str(conversation_id),
            **request.model_dump(mode="json"),
        }
    )
    try:
        async with database.session_factory() as session, session.begin():
            # Exact historical selection replay remains available after a
            # handoff/floor transition. New Telegram selections first acquire
            # the same integration-scope lock as enrollment and supplier work.
            probe = await authorized_conversation(session, principal, conversation_id)
            turn = await session.scalar(
                select(AgentTurn).where(
                    AgentTurn.conversation_id == probe.id,
                    AgentTurn.selection_key == request.selection_key,
                )
            )
            if turn is not None:
                if turn.creation_fingerprint != digest:
                    raise DomainError(ErrorCode.TURN_SELECTION_CONFLICT)
                result = TurnResult(turn=TurnRead.model_validate(turn), replayed=True)
            else:
                handoff_enrolled = await lock_legacy_scopes(session, owner_id=probe.owner_id,
                    integration_id=probe.integration_id, channel=probe.channel, settings=settings)
                conversation = await authorized_conversation(
                    session, principal, conversation_id, lock=True
                )
                # Another request can have inserted the exact selection while
                # this transaction waited for the scope or parent lock.
                turn = await session.scalar(
                    select(AgentTurn).where(
                        AgentTurn.conversation_id == conversation.id,
                        AgentTurn.selection_key == request.selection_key,
                    )
                )
                if turn is not None:
                    if turn.creation_fingerprint != digest:
                        raise DomainError(ErrorCode.TURN_SELECTION_CONFLICT)
                    result = TurnResult(turn=TurnRead.model_validate(turn), replayed=True)
                    return result
                if handoff_enrolled or await handoff_was_ever_enrolled(
                    session, conversation
                ):
                    raise DomainError(ErrorCode.HANDOFF_CONFLICT)
                if conversation.control_mode != "AGENT":
                    raise DomainError(ErrorCode.CONTROL_MODE_CONFLICT)
                if request.expected_authority_revision != conversation.authority_revision:
                    raise DomainError(ErrorCode.AUTHORITY_REVISION_CONFLICT)
                if request.expected_input_revision != conversation.input_revision:
                    raise DomainError(ErrorCode.INPUT_REVISION_CONFLICT)
                if not (
                    1
                    <= request.input_first_sequence
                    <= request.input_last_sequence
                    <= conversation.last_message_sequence
                ):
                    raise DomainError(ErrorCode.INVALID_INPUT_BOUNDARY)
                settling_input = await session.scalar(
                    select(Message.id)
                    .where(
                    Message.conversation_id == conversation.id,
                    Message.sequence >= request.input_first_sequence,
                    Message.sequence <= request.input_last_sequence,
                    Message.actor == "CUSTOMER",
                    Message.direction == "INBOUND",
                        or_(
                            Message.settling_policy_version.is_not(None),
                            Message.settling_burst_id.is_not(None),
                        ),
                    )
                    .limit(1)
                )
                if settling_input is not None:
                    raise DomainError(ErrorCode.SETTLING_REQUIRED)
                endpoints = set(
                    (
                        await session.scalars(
                            select(Message.sequence).where(
                                Message.conversation_id == conversation.id,
                                Message.sequence.in_(
                                    {
                                        request.input_first_sequence,
                                        request.input_last_sequence,
                                        conversation.last_message_sequence,
                                    }
                                ),
                            )
                        )
                    ).all()
                )
                if endpoints != {
                    request.input_first_sequence,
                    request.input_last_sequence,
                    conversation.last_message_sequence,
                }:
                    raise DomainError(ErrorCode.INVALID_INPUT_BOUNDARY)
                active_id = await session.scalar(
                    select(AgentTurn.id).where(
                        AgentTurn.conversation_id == conversation.id,
                        AgentTurn.status.in_(("PENDING", "CLAIMED")),
                        AgentTurn.retired_at.is_(None),
                    )
                )
                if active_id is not None:
                    raise DomainError(ErrorCode.ACTIVE_TURN_EXISTS)
                covered_by_retained_turn = await session.scalar(
                    select(Message.id)
                    .where(
                    Message.conversation_id == conversation.id,
                    Message.sequence >= request.input_first_sequence,
                    Message.sequence <= request.input_last_sequence,
                    Message.actor == "CUSTOMER",
                    Message.direction == "INBOUND",
                    message_is_consumed(),
                    )
                    .limit(1)
                )
                if covered_by_retained_turn is not None:
                    raise DomainError(ErrorCode.TURN_SELECTION_CONFLICT)
                turn = AgentTurn(
                    id=uuid4(),
                    conversation_id=conversation.id,
                    selection_key=request.selection_key,
                    creation_fingerprint=digest,
                    input_first_sequence=request.input_first_sequence,
                    input_last_sequence=request.input_last_sequence,
                    context_last_sequence=conversation.last_message_sequence,
                    input_revision=conversation.input_revision,
                    authority_revision=conversation.authority_revision,
                    settling_revision=conversation.settling_revision,
                    status="PENDING",
                    claim_epoch=0,
                    operation_refs=[],
                    created_at=await db_clock(session),
                )
                session.add(turn)
                await session.flush()
                selected_messages = (
                    await session.execute(
                        select(
                            Message.id,
                            Message.sequence,
                        )
                        .where(
                    Message.conversation_id == conversation.id,
                    Message.sequence >= request.input_first_sequence,
                    Message.sequence <= request.input_last_sequence,
                    Message.actor == "CUSTOMER",
                    Message.direction == "INBOUND",
                        )
                        .order_by(Message.sequence)
                    )
                ).all()
                for ordinal, (message_id, sequence) in enumerate(selected_messages):
                    await session.execute(
                        insert(TurnInputMessage).values(
                            message_id=message_id,
                            conversation_id=conversation.id,
                            turn_id=turn.id,
                            sequence=sequence,
                            ordinal=ordinal,
                        )
                    )
                result = TurnResult(turn=TurnRead.model_validate(turn), replayed=False)
    except IntegrityError as exc:
        # session.begin has rolled back before translating a recognized durable fence.
        if unique_violation(exc, "uq_turn_one_active"):
            raise DomainError(ErrorCode.ACTIVE_TURN_EXISTS) from None
        if unique_violation(exc, "pk_turn_input_messages"):
            raise DomainError(ErrorCode.TURN_SELECTION_CONFLICT) from None
        raise
    return result


async def get_turn(database: Database, principal: Principal, turn_id: UUID) -> TurnRead:
    async with database.session_factory() as session, session.begin():
        turn = await session.scalar(
            select(AgentTurn)
            .join(Conversation, Conversation.id == AgentTurn.conversation_id)
            .where(AgentTurn.id == turn_id, scope_filter(principal))
        )
        if turn is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        return TurnRead.model_validate(turn)


async def claim_turn(
    database: Database, principal: Principal, turn_id: UUID, request: TurnClaim
) -> TurnClaimResult:
    async with database.session_factory() as session, session.begin():
        probe = (
            await session.execute(
                select(
                    Conversation.owner_id,
                    Conversation.integration_id,
                    Conversation.channel,
                    AgentTurn.managed_contract_version,
                    AgentTurn.managed_projection_agent_id,
                    AgentTurn.managed_dispatch_epoch,
                )
                .join(AgentTurn, AgentTurn.conversation_id == Conversation.id)
                .where(
                    AgentTurn.id == turn_id,
                    scope_filter(principal),
                )
            )
        ).first()
        handoff_scope = None
        if probe is not None and probe.managed_contract_version == 3:
            if probe.channel != "TELEGRAM" or probe.managed_projection_agent_id is None:
                raise DomainError(ErrorCode.NOT_FOUND)
            from services.telegram import _locked_handoff_scope_values

            handoff_scope = await _locked_handoff_scope_values(
                session,
                probe.owner_id,
                probe.integration_id,
                probe.managed_projection_agent_id,
            )
            if handoff_scope is None:
                raise DomainError(ErrorCode.GRANT_CONFLICT)
            if not handoff_scope.active and probe.managed_dispatch_epoch is None:
                raise DomainError(ErrorCode.GRANT_CONFLICT)
        conversation, turn = await locked_turn(session, principal, turn_id)
        if turn.retired_at is not None and turn.managed_dispatch_epoch is None:
            raise DomainError(ErrorCode.CONTROL_MODE_CONFLICT)
        if (
            handoff_scope is not None
            and not handoff_scope.active
            and turn.managed_dispatch_epoch is None
        ):
            raise DomainError(ErrorCode.GRANT_CONFLICT)
        clock = await db_clock(session)
        if turn.status in ("COMPLETED", "FAILED"):
            raise DomainError(ErrorCode.TURN_TERMINAL)
        # Once a managed request has been durably marked, recovery is about
        # resolving that exact uncertainty, not authorizing a new AI dispatch.
        if conversation.control_mode != "AGENT" and turn.managed_dispatch_epoch is None:
            raise DomainError(ErrorCode.CONTROL_MODE_CONFLICT)
        if turn.status == "CLAIMED" and (
            turn.lease_expires_at is None or turn.lease_expires_at > clock
        ):
            raise DomainError(ErrorCode.LEASE_HELD)
        raw_token = secrets.token_urlsafe(32)
        turn.claim_epoch += 1
        turn.claim_token_digest = hashlib.sha256(raw_token.encode("ascii")).hexdigest()
        turn.worker_ref = request.worker_ref
        turn.lease_expires_at = clock + timedelta(seconds=request.lease_seconds)
        turn.started_at = turn.started_at or clock
        turn.status = "CLAIMED"
        await session.flush()
        result = TurnClaimResult(turn=TurnRead.model_validate(turn), claim_token=raw_token)
    return result


def winning_claim(turn: AgentTurn, epoch: int, raw_token: str) -> None:
    supplied_digest = hashlib.sha256(raw_token.encode("ascii")).hexdigest()
    if (
        turn.claim_epoch != epoch
        or turn.claim_token_digest is None
        or not hmac.compare_digest(turn.claim_token_digest, supplied_digest)
    ):
        raise DomainError(ErrorCode.STALE_CLAIM)


async def renew_turn(
    database: Database, principal: Principal, turn_id: UUID, request: TurnRenew
) -> TurnRead:
    async with database.session_factory() as session, session.begin():
        _, turn = await locked_turn(session, principal, turn_id)
        if turn.status in ("COMPLETED", "FAILED"):
            raise DomainError(ErrorCode.TURN_TERMINAL)
        winning_claim(turn, request.claim_epoch, request.claim_token.get_secret_value())
        clock = await db_clock(session)
        if turn.status != "CLAIMED" or turn.lease_expires_at is None:
            raise DomainError(ErrorCode.STALE_CLAIM)
        if clock >= turn.lease_expires_at:
            raise DomainError(ErrorCode.LEASE_EXPIRED)
        turn.lease_expires_at = max(
            turn.lease_expires_at, clock + timedelta(seconds=request.lease_seconds)
        )
        await session.flush()
        result = TurnRead.model_validate(turn)
    return result


async def finish_turn(
    database: Database,
    principal: Principal,
    turn_id: UUID,
    request: TurnComplete | TurnFail,
) -> TurnResult:
    completed = isinstance(request, TurnComplete)
    terminal_status = "COMPLETED" if completed else "FAILED"
    refs = [ref.model_dump(mode="json") for ref in request.operation_refs]
    digest = fingerprint(
        {
            "contract_version": 1,
            "turn_id": str(turn_id),
            "terminal_status": terminal_status,
            "outcome": request.outcome.value if isinstance(request, TurnComplete) else None,
            "draft_text": request.draft_text if isinstance(request, TurnComplete) else None,
            "failure_code": request.failure_code.value if isinstance(request, TurnFail) else None,
            "operation_refs": refs,
        }
    )
    async with database.session_factory() as session, session.begin():
        _, turn = await locked_turn(session, principal, turn_id)
        winning_claim(turn, request.claim_epoch, request.claim_token.get_secret_value())
        if turn.status in ("COMPLETED", "FAILED"):
            if turn.terminal_fingerprint != digest:
                raise DomainError(ErrorCode.TERMINAL_RESULT_CONFLICT)
            # This proof acknowledges an already committed result, even after expiry.
            result = TurnResult(turn=TurnRead.model_validate(turn), replayed=True)
        else:
            clock = await db_clock(session)
            if turn.status != "CLAIMED" or turn.lease_expires_at is None:
                raise DomainError(ErrorCode.STALE_CLAIM)
            if clock >= turn.lease_expires_at:
                raise DomainError(ErrorCode.LEASE_EXPIRED)
            turn.status = terminal_status
            turn.completed_at = clock
            turn.terminal_fingerprint = digest
            turn.operation_refs = refs
            if isinstance(request, TurnComplete):
                turn.outcome = request.outcome.value
                turn.draft_text = request.draft_text
                turn.failure_code = None
            else:
                turn.failure_code = request.failure_code.value
                turn.outcome = None
                turn.draft_text = None
            await session.flush()
            result = TurnResult(turn=TurnRead.model_validate(turn), replayed=False)
    return result


async def complete_turn(
    database: Database, principal: Principal, turn_id: UUID, request: TurnComplete
) -> TurnResult:
    return await finish_turn(database, principal, turn_id, request)


async def fail_turn(
    database: Database, principal: Principal, turn_id: UUID, request: TurnFail
) -> TurnResult:
    return await finish_turn(database, principal, turn_id, request)

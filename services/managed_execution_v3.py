"""Exact-replay managed v3 execution and atomic HUMAN_HANDOFF control."""

from __future__ import annotations

import asyncio
from typing import Literal, cast
from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import ManagedBinding, Settings
from core.errors import DomainError
from db.database import Database
from models import (
    AgentTurn,
    Conversation,
    DeliveryAttempt,
    HandoffCase,
    InboxCommandReceipt,
    InboxGrantScope,
    SettlingBurst,
)
from schemas.contracts import (
    ErrorCode,
    OperationReference,
    Principal,
    TurnComplete,
    TurnExecute,
    TurnFail,
    TurnFailureCode,
    TurnOutcome,
    TurnRead,
    TurnResult,
)
from schemas.managed_v3 import (
    AuthorityEventFact,
    ControlEvidenceRequest,
    DeliveryObservationFact,
    HandoffControlRequest,
    ManagedTurnV3Request,
    ManagedTurnV3Result,
    PriorOperationReference,
    V3Message,
    V3OperationReference,
)
from services import catch_up, telegram, turns
from services.common import authorized_conversation, db_clock, fingerprint, scope_filter
from services.history_projection import canonical_window
from services.managed_client import (
    ManagedAIPort,
    ManagedPortFailure,
    PreparedManagedV3Request,
    handoff_request_id,
    prepare_v3_request,
)


async def _authority_and_delivery_facts(
    session: AsyncSession,
    conversation: Conversation,
    messages: list[V3Message],
    through_revision: int,
) -> list[AuthorityEventFact | DeliveryObservationFact]:
    cases = (
        await session.scalars(
            select(HandoffCase)
            .where(
                HandoffCase.conversation_id == conversation.id,
                HandoffCase.opened_authority_revision <= through_revision,
            )
            .order_by(HandoffCase.opened_authority_revision, HandoffCase.id)
        )
    ).all()
    receipts = (
        await session.scalars(
            select(InboxCommandReceipt)
            .where(
                InboxCommandReceipt.conversation_id == conversation.id,
                InboxCommandReceipt.kind == "RETURN_TO_AGENT",
                InboxCommandReceipt.applied_authority_revision <= through_revision,
            )
            .order_by(
                InboxCommandReceipt.applied_authority_revision, InboxCommandReceipt.command_id
            )
        )
    ).all()
    facts: list[AuthorityEventFact | DeliveryObservationFact] = []
    facts.extend(
        AuthorityEventFact(
            kind="AUTHORITY_EVENT",
            event_id=case.id,
            authority_revision=case.opened_authority_revision,
            from_mode="AGENT",
            to_mode="HUMAN",
            case_id=case.id,
        )
        for case in cases
    )
    facts.extend(
        AuthorityEventFact(
            kind="AUTHORITY_EVENT",
            event_id=receipt.command_id,
            authority_revision=receipt.applied_authority_revision,
            from_mode="HUMAN",
            to_mode="AGENT",
            case_id=receipt.case_id,
        )
        for receipt in receipts
    )

    outbound = [
        item
        for item in messages
        if item.actor in {"HUMAN", "SYSTEM"} and item.direction == "OUTBOUND"
    ]
    attempts: dict[UUID, DeliveryAttempt] = {}
    if outbound:
        rows = (
            await session.scalars(
                select(DeliveryAttempt).where(
                    DeliveryAttempt.conversation_id == conversation.id,
                    DeliveryAttempt.source_message_id.in_([item.id for item in outbound]),
                )
            )
        ).all()
        attempts = {row.source_message_id: row for row in rows if row.source_message_id is not None}
    for item in outbound:
        attempt = attempts.get(item.id)
        facts.append(
            DeliveryObservationFact(
                kind="DELIVERY_OBSERVATION",
                message_id=item.id,
                delivery_attempt_id=attempt.id if attempt is not None else None,
                source_authority_revision=(
                    attempt.authority_revision if attempt is not None else through_revision
                ),
                status=(
                    cast(
                        Literal[
                            "NOT_ACQUIRED",
                            "GENERATED",
                            "DISPATCH_REQUESTED",
                            "PROVIDER_ACCEPTED",
                            "DELIVERED",
                            "READ",
                            "FAILED",
                            "SUPERSEDED",
                            "UNKNOWN",
                            "PARTIAL",
                            "INTERRUPTED",
                        ],
                        attempt.status,
                    )
                    if attempt is not None
                    else "NOT_ACQUIRED"
                ),
            )
        )
    facts.sort(
        key=lambda fact: (
            0 if isinstance(fact, AuthorityEventFact) else 1,
            fact.authority_revision
            if isinstance(fact, AuthorityEventFact)
            else next((m.sequence for m in messages if m.id == fact.message_id), 0),
            str(fact.event_id) if isinstance(fact, AuthorityEventFact) else str(fact.message_id),
        )
    )
    if len(facts) > 64:
        delivery_facts = [f for f in facts if isinstance(f, DeliveryObservationFact)]
        authority_facts = [f for f in facts if isinstance(f, AuthorityEventFact)]
        if len(delivery_facts) > 64:
            # Every projected HUMAN/SYSTEM outbound has a delivery fact. Never
            # truncate one while retaining its message in the AI context.
            raise DomainError(ErrorCode.PAYLOAD_TOO_LARGE)
        authority_budget = 64 - len(delivery_facts)
        authority_facts = authority_facts[-authority_budget:] if authority_budget else []
        facts = authority_facts + delivery_facts
        facts.sort(
            key=lambda fact: (
                0 if isinstance(fact, AuthorityEventFact) else 1,
                fact.authority_revision
                if isinstance(fact, AuthorityEventFact)
                else next((m.sequence for m in messages if m.id == fact.message_id), 0),
                str(fact.event_id)
                if isinstance(fact, AuthorityEventFact)
                else str(fact.message_id),
            )
        )
    return facts


async def _prior_operation_refs(
    session: AsyncSession,
    conversation_id: UUID,
    turn: AgentTurn,
) -> list[PriorOperationReference]:
    previous = (
        await session.scalars(
            select(AgentTurn)
            .where(
                AgentTurn.conversation_id == conversation_id,
                AgentTurn.id != turn.id,
                AgentTurn.created_at <= turn.created_at,
                AgentTurn.status.in_(("COMPLETED", "FAILED")),
            )
            .order_by(AgentTurn.created_at.desc(), AgentTurn.id.desc())
            .limit(32)
        )
    ).all()
    values: list[PriorOperationReference] = []
    for prior in previous:
        for item in prior.operation_refs:
            try:
                operation = V3OperationReference.model_validate(
                    {
                        "origin": item["origin"],
                        "operation_id": item["operation_id"],
                        "outcome": item.get("outcome"),
                    }
                )
                values.append(
                    PriorOperationReference(
                        origin_turn_id=prior.id,
                        operation_ref=operation,
                    )
                )
            except (KeyError, TypeError, ValueError):
                raise DomainError(ErrorCode.VALIDATION_ERROR) from None
    values.sort(
        key=lambda item: (
            str(item.origin_turn_id),
            item.operation_ref.origin,
            item.operation_ref.operation_id,
        )
    )
    return values[:32]


async def _build_request(
    session: AsyncSession,
    conversation: Conversation,
    turn: AgentTurn,
    binding: ManagedBinding,
) -> PreparedManagedV3Request:
    messages = await canonical_window(
        session,
        conversation.id,
        turn.managed_context_first_sequence,
        turn.context_last_sequence,
    )
    v3_messages = [V3Message.model_validate(item.model_dump(mode="python")) for item in messages]
    facts = await _authority_and_delivery_facts(
        session,
        conversation,
        v3_messages,
        turn.authority_revision,
    )
    prior_refs = await _prior_operation_refs(session, conversation.id, turn)
    try:
        request = ManagedTurnV3Request(
            contract_version=3,
            conversation_id=conversation.id,
            turn_id=turn.id,
            owner_id=conversation.owner_id,
            integration_id=conversation.integration_id,
            agent_id=binding.agent_id,
            channel="TELEGRAM",
            input_revision=turn.input_revision,
            authority_revision=turn.authority_revision,
            input_first_sequence=turn.input_first_sequence,
            input_last_sequence=turn.input_last_sequence,
            context_last_sequence=turn.context_last_sequence,
            messages=v3_messages,
            selected_input_message_ids=[
                message.id
                for message in v3_messages
                if turn.input_first_sequence <= message.sequence <= turn.input_last_sequence
            ],
            communication_facts=facts,
            prior_operation_refs=prior_refs,
        )
        return prepare_v3_request(request)
    except (ValueError, TypeError):
        raise DomainError(ErrorCode.VALIDATION_ERROR) from None


async def _locked_handoff_turn(
    session: AsyncSession,
    principal: Principal,
    turn_id: UUID,
) -> tuple[Conversation, AgentTurn, InboxGrantScope]:
    discovered = (
        await session.execute(
            select(
                Conversation.id.label("conversation_id"),
                Conversation.owner_id,
                Conversation.integration_id,
                AgentTurn.managed_projection_agent_id,
            )
            .join(AgentTurn, AgentTurn.conversation_id == Conversation.id)
            .where(
                AgentTurn.id == turn_id,
                AgentTurn.managed_contract_version == 3,
                scope_filter(principal),
            )
        )
    ).first()
    if discovered is None or discovered.managed_projection_agent_id is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    scope = await telegram._locked_handoff_scope_values(
        session,
        discovered.owner_id,
        discovered.integration_id,
        discovered.managed_projection_agent_id,
    )
    if scope is None:
        raise DomainError(ErrorCode.GRANT_CONFLICT)
    conversation = await authorized_conversation(
        session,
        principal,
        discovered.conversation_id,
        lock=True,
    )
    turn = await session.scalar(
        select(AgentTurn)
        .where(
            AgentTurn.id == turn_id,
            AgentTurn.conversation_id == conversation.id,
        )
        .with_for_update()
        .execution_options(populate_existing=True)
    )
    if turn is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    return conversation, turn, scope


async def _prepare_dispatch(
    database: Database,
    principal: Principal,
    turn_id: UUID,
    proof: TurnExecute,
    settings: Settings,
) -> PreparedManagedV3Request | TurnResult:
    async with database.session_factory() as session, session.begin():
        conversation, turn, scope = await _locked_handoff_turn(session, principal, turn_id)
        turns.winning_claim(turn, proof.claim_epoch, proof.claim_token.get_secret_value())
        if turn.status in {"COMPLETED", "FAILED"}:
            return TurnResult(turn=TurnRead.model_validate(turn), replayed=True)
        clock = await db_clock(session)
        if (
            turn.status != "CLAIMED"
            or turn.lease_expires_at is None
            or clock >= turn.lease_expires_at
        ):
            raise DomainError(ErrorCode.LEASE_EXPIRED)
        binding = next(
            (
                item
                for item in settings.managed_ai_bindings
                if item.channel.value == "TELEGRAM"
                and item.owner_id == conversation.owner_id
                and item.integration_id == conversation.integration_id
                and item.agent_id == turn.managed_projection_agent_id
            ),
            None,
        )
        if binding is None or settings.managed_ai_base_url is None:
            raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
        if turn.managed_v3_snapshot is None:
            if not await catch_up.validate(session, conversation, turn, binding.agent_id):
                raise DomainError(ErrorCode.VALIDATION_ERROR)
            if (
                turn.retired_at is not None
                or not scope.active
                or scope.agent_id != binding.agent_id
                or conversation.control_mode != "AGENT"
                or turn.authority_revision != conversation.authority_revision
                or scope.agent_lifecycle_generation is None
                or (
                    scope.agent_lifecycle_high_water is not None
                    and scope.agent_lifecycle_generation <= scope.agent_lifecycle_high_water
                )
                or scope.cp_binding_generation is None
                or (
                    scope.cp_binding_high_water is not None
                    and scope.cp_binding_generation <= scope.cp_binding_high_water
                )
            ):
                raise DomainError(ErrorCode.GRANT_CONFLICT)
            prepared = await _build_request(session, conversation, turn, binding)
            turn.managed_v3_snapshot = prepared.request.model_dump(mode="json")
            turn.managed_v3_request_fingerprint = prepared.request_fingerprint
            turn.managed_v3_dispatch_id = prepared.original_dispatch_id
            turn.managed_request_fingerprint = prepared.sidecar_fingerprint
            turn.managed_agent_id = binding.agent_id
            turn.managed_dispatch_started_at = clock
        else:
            try:
                request = ManagedTurnV3Request.model_validate(turn.managed_v3_snapshot)
                prepared = prepare_v3_request(request)
            except (ValueError, TypeError):
                raise DomainError(ErrorCode.VALIDATION_ERROR) from None
            if (
                prepared.request_fingerprint != turn.managed_v3_request_fingerprint
                or prepared.original_dispatch_id != turn.managed_v3_dispatch_id
                or prepared.sidecar_fingerprint != turn.managed_request_fingerprint
                or request.turn_id != turn.id
                or request.conversation_id != conversation.id
                or request.agent_id != binding.agent_id
            ):
                raise DomainError(ErrorCode.VALIDATION_ERROR)
        # Updating this claim epoch authorizes same-request recovery only; the
        # immutable v3 body and AI receipt ID never change.
        turn.managed_dispatch_epoch = proof.claim_epoch
        turn.managed_dispatch_started_at = clock
        await session.flush()
        return prepared


def _operation_refs(values: list[V3OperationReference]) -> list[OperationReference]:
    return [OperationReference.model_validate(item.model_dump(mode="python")) for item in values]


def _terminal_request(
    proof: TurnExecute,
    outcome: str,
    draft_text: str | None,
    operation_refs: list[V3OperationReference],
    error_code: str | None,
) -> TurnComplete | TurnFail:
    refs = _operation_refs(operation_refs)
    if outcome == "DRAFT_READY":
        return TurnComplete(
            claim_epoch=proof.claim_epoch,
            claim_token=proof.claim_token,
            operation_refs=refs,
            outcome=TurnOutcome.DRAFT_READY,
            draft_text=draft_text,
        )
    if outcome == "NO_RESPONSE":
        return TurnComplete(
            claim_epoch=proof.claim_epoch,
            claim_token=proof.claim_token,
            operation_refs=refs,
            outcome=TurnOutcome.NO_RESPONSE,
            draft_text=None,
        )
    if outcome == "FAILED":
        failure = (
            TurnFailureCode.INVALID_RESULT
            if error_code in {"INVALID_RESULT", "CONTEXT_LIMIT"}
            else TurnFailureCode.PROCESSING_ERROR
        )
    elif error_code in {"EXECUTION_TIMEOUT", "TOOL_EXECUTION_UNKNOWN", "TOOL_CLEANUP_FAILED"}:
        failure = TurnFailureCode.EXECUTION_UNKNOWN
    elif error_code == "MODEL_TIMEOUT":
        failure = TurnFailureCode.TIMEOUT
    elif error_code in {"INVALID_RESULT", "CONTEXT_LIMIT"}:
        failure = TurnFailureCode.INVALID_RESULT
    else:
        failure = TurnFailureCode.PROCESSING_ERROR
    return TurnFail(
        claim_epoch=proof.claim_epoch,
        claim_token=proof.claim_token,
        operation_refs=refs,
        failure_code=failure,
    )


def _terminal_fingerprint(turn_id: UUID, request: TurnComplete) -> str:
    refs = [ref.model_dump(mode="json") for ref in request.operation_refs]
    return fingerprint(
        {
            "contract_version": 1,
            "turn_id": str(turn_id),
            "terminal_status": "COMPLETED",
            "outcome": request.outcome.value,
            "draft_text": request.draft_text,
            "failure_code": None,
            "operation_refs": refs,
        }
    )


async def _finish_handoff(
    database: Database,
    principal: Principal,
    turn_id: UUID,
    proof: TurnExecute,
    control: HandoffControlRequest,
    operation_refs: list[V3OperationReference],
) -> TurnResult:
    from services.authority import advance_authority
    from services.telegram import _reconciliation_key, _record_reconciliation

    async with database.session_factory() as session, session.begin():
        conversation, turn, scope = await _locked_handoff_turn(session, principal, turn_id)
        turns.winning_claim(turn, proof.claim_epoch, proof.claim_token.get_secret_value())
        expected_control_id = handoff_request_id(
            ManagedTurnV3Request.model_validate(turn.managed_v3_snapshot)
        )
        if control.request_id != expected_control_id:
            raise DomainError(ErrorCode.HANDOFF_CONFLICT)
        control_fingerprint = fingerprint(control.model_dump(mode="json"))
        existing = await session.scalar(
            select(HandoffCase)
            .where(
                HandoffCase.conversation_id == conversation.id,
                HandoffCase.owner_id == conversation.owner_id,
                HandoffCase.integration_id == conversation.integration_id,
                HandoffCase.channel == "TELEGRAM",
                HandoffCase.control_request_id == control.request_id,
            )
            .with_for_update()
            .execution_options(populate_existing=True)
        )
        if turn.status in {"COMPLETED", "FAILED"}:
            if (
                turn.status == "COMPLETED"
                and existing is not None
                and existing.source_turn_id == turn.id
                and existing.control_request_fingerprint == control_fingerprint
            ):
                return TurnResult(turn=TurnRead.model_validate(turn), replayed=True)
            raise DomainError(ErrorCode.HANDOFF_CONFLICT)
        clock = await db_clock(session)
        if (
            turn.status != "CLAIMED"
            or turn.lease_expires_at is None
            or clock >= turn.lease_expires_at
        ):
            raise DomainError(ErrorCode.LEASE_EXPIRED)
        if existing is not None:
            if (
                existing.source_turn_id != turn.id
                or existing.control_request_fingerprint != control_fingerprint
            ):
                raise DomainError(ErrorCode.HANDOFF_CONFLICT)
            # The case transition and source-turn terminalization are atomic.
            # Finding a case here means only exact recovery, never reopen it.
            terminal = TurnComplete(
                claim_epoch=proof.claim_epoch,
                claim_token=proof.claim_token,
                operation_refs=_operation_refs(operation_refs),
                outcome=TurnOutcome.NO_RESPONSE,
                draft_text=None,
            )
            turn.status = "COMPLETED"
            turn.completed_at = clock
            turn.terminal_fingerprint = _terminal_fingerprint(turn.id, terminal)
            turn.operation_refs = [ref.model_dump(mode="json") for ref in terminal.operation_refs]
            turn.outcome = "NO_RESPONSE"
            turn.draft_text = None
            turn.failure_code = None
            await session.flush()
            return TurnResult(turn=TurnRead.model_validate(turn), replayed=False)

        current = (
            scope.active
            and scope.grant_generation >= 1
            and scope.agent_id == turn.managed_projection_agent_id
            and conversation.control_mode == "AGENT"
            and conversation.authority_revision == turn.authority_revision
            and conversation.input_revision == turn.input_revision
            and conversation.settling_revision == turn.settling_revision
            and turn.retired_at is None
        )
        if current and turn.catch_up_version:
            current = await catch_up.communication_current(session, conversation, turn)
        terminal = TurnComplete(
            claim_epoch=proof.claim_epoch,
            claim_token=proof.claim_token,
            operation_refs=_operation_refs(operation_refs),
            outcome=TurnOutcome.NO_RESPONSE,
            draft_text=None,
        )
        if current:
            duplicate = await session.scalar(
                select(HandoffCase.id).where(
                    HandoffCase.conversation_id == conversation.id,
                    HandoffCase.status.in_(("OPEN", "CLAIMED")),
                )
            )
            if duplicate is not None:
                raise DomainError(ErrorCode.HANDOFF_CONFLICT)
            advance_authority(conversation)
            conversation.control_mode = "HUMAN"
            conversation.updated_at = clock
            case = HandoffCase(
                id=uuid4(),
                conversation_id=conversation.id,
                owner_id=conversation.owner_id,
                integration_id=conversation.integration_id,
                channel="TELEGRAM",
                agent_id=turn.managed_projection_agent_id,
                control_request_id=control.request_id,
                control_request_fingerprint=control_fingerprint,
                source_turn_id=turn.id,
                reason=control.reason,
                note=control.note,
                status="OPEN",
                case_revision=1,
                authority_revision=conversation.authority_revision,
                opened_authority_revision=conversation.authority_revision,
                assignment_revision=0,
                assigned_account_id=None,
                created_at=clock,
                updated_at=clock,
            )
            session.add(case)
            bursts = (
                await session.scalars(
                    select(SettlingBurst)
                    .where(
                        SettlingBurst.conversation_id == conversation.id,
                        SettlingBurst.closed_at.is_(None),
                    )
                    .with_for_update()
                )
            ).all()
            for burst in bursts:
                burst.closed_at = burst.ready_at
            active_turns = (
                await session.scalars(
                    select(AgentTurn)
                    .where(
                        AgentTurn.conversation_id == conversation.id,
                        AgentTurn.id != turn.id,
                        AgentTurn.status.in_(("PENDING", "CLAIMED")),
                        AgentTurn.retired_at.is_(None),
                    )
                    .with_for_update()
                )
            ).all()
            for active_turn in active_turns:
                active_turn.retired_at = clock
            pending_intents = (
                await session.scalars(
                    select(DeliveryAttempt)
                    .where(
                        DeliveryAttempt.conversation_id == conversation.id,
                        DeliveryAttempt.source_turn_id.is_not(None),
                        DeliveryAttempt.writer_state == "NONE",
                        DeliveryAttempt.status == "GENERATED",
                    )
                    .with_for_update()
                )
            ).all()
            for intent in pending_intents:
                from services.telegram_delivery_v2 import response_attempt_id

                _record_reconciliation(
                    session,
                    intent,
                    conversation.id,
                    "SUPERSEDED",
                    key=_reconciliation_key(response_attempt_id(intent.id), "SUPERSEDED"),
                    expected_revision=intent.revision,
                    clock=clock,
                )
        turn.status = "COMPLETED"
        turn.completed_at = clock
        turn.terminal_fingerprint = _terminal_fingerprint(turn.id, terminal)
        turn.operation_refs = [ref.model_dump(mode="json") for ref in terminal.operation_refs]
        turn.outcome = "NO_RESPONSE"
        turn.draft_text = None
        turn.failure_code = None
        await session.flush()
        return TurnResult(turn=TurnRead.model_validate(turn), replayed=False)


async def _finish_result(
    database: Database,
    principal: Principal,
    turn_id: UUID,
    proof: TurnExecute,
    result: ManagedTurnV3Result,
) -> TurnResult:
    if result.result_kind == "PLATFORM_CONTROL_REQUEST":
        assert result.control_request is not None
        return await _finish_handoff(
            database,
            principal,
            turn_id,
            proof,
            result.control_request,
            result.operation_refs,
        )
    terminal = _terminal_request(
        proof,
        result.processing_outcome,
        result.draft_text,
        result.operation_refs,
        result.error_code,
    )
    return await turns.finish_turn(database, principal, turn_id, terminal)


async def _evidence_request(prepared: PreparedManagedV3Request) -> ControlEvidenceRequest:
    value = prepared.request
    return ControlEvidenceRequest(
        contract_version=3,
        owner_id=value.owner_id,
        integration_id=value.integration_id,
        channel="TELEGRAM",
        agent_id=value.agent_id,
        conversation_id=value.conversation_id,
        turn_id=value.turn_id,
        input_revision=value.input_revision,
        authority_revision=value.authority_revision,
        input_first_sequence=value.input_first_sequence,
        input_last_sequence=value.input_last_sequence,
        context_last_sequence=value.context_last_sequence,
        request_fingerprint=prepared.request_fingerprint,
        original_dispatch_id=prepared.original_dispatch_id,
    )


async def _current_result(
    database: Database,
    principal: Principal,
    turn_id: UUID,
) -> TurnResult:
    async with database.session_factory() as session, session.begin():
        conversation_id = await session.scalar(
            select(AgentTurn.conversation_id).where(
                AgentTurn.id == turn_id,
            )
        )
        if conversation_id is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        conversation = await authorized_conversation(
            session,
            principal,
            conversation_id,
            lock=False,
        )
        turn = await session.scalar(
            select(AgentTurn).where(
                AgentTurn.id == turn_id,
                AgentTurn.conversation_id == conversation.id,
            )
        )
        if turn is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        return TurnResult(turn=TurnRead.model_validate(turn), replayed=True)


async def execute_claimed_turn_v3(
    database: Database,
    principal: Principal,
    turn_id: UUID,
    proof: TurnExecute,
    settings: Settings,
    port: ManagedAIPort,
    request_id: str = "",
) -> TurnResult:
    try:
        prepared = await _prepare_dispatch(database, principal, turn_id, proof, settings)
    except DomainError as error:
        if error.code != ErrorCode.PAYLOAD_TOO_LARGE:
            raise
        # A canonical window that cannot fit its required communication facts
        # is a terminal input limit, not a recoverable transport uncertainty.
        return await turns.finish_turn(
            database,
            principal,
            turn_id,
            TurnFail(
                claim_epoch=proof.claim_epoch,
                claim_token=proof.claim_token,
                failure_code=TurnFailureCode.INVALID_RESULT,
                operation_refs=[],
            ),
        )
    if isinstance(prepared, TurnResult):
        return prepared
    binding = next(
        (
            item
            for item in settings.managed_ai_bindings
            if item.channel.value == "TELEGRAM"
            and item.owner_id == prepared.request.owner_id
            and item.integration_id == prepared.request.integration_id
            and item.agent_id == prepared.request.agent_id
        ),
        None,
    )
    if binding is None:
        raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
    direct_result = None
    try:
        direct_result = await port.execute_v3(prepared, binding, request_id)
    except asyncio.CancelledError:
        # The exact request, dispatch identity, and AI receipt are durable.
        # A later claim recovers the same call without invoking a second model run.
        raise
    except ManagedPortFailure as failure:
        if failure.failure_code != TurnFailureCode.EXECUTION_UNKNOWN:
            return await turns.finish_turn(
                database,
                principal,
                turn_id,
                TurnFail(
                    claim_epoch=proof.claim_epoch,
                    claim_token=proof.claim_token,
                    failure_code=failure.failure_code,
                    operation_refs=[],
                ),
            )
    except Exception:
        pass
    if direct_result is not None and direct_result.result_kind == "PLATFORM_CONTROL_REQUEST":
        return await _finish_result(database, principal, turn_id, proof, direct_result)
    if direct_result is not None and direct_result.processing_outcome not in {"UNKNOWN"}:
        return await _finish_result(database, principal, turn_id, proof, direct_result)
    try:
        evidence = await port.control_evidence_v3(
            await _evidence_request(prepared),
            binding,
        )
    except Exception:
        return await _current_result(database, principal, turn_id)
    if evidence.evidence_kind == "CONTROL_REQUEST" and evidence.control_request is not None:
        return await _finish_handoff(
            database,
            principal,
            turn_id,
            proof,
            evidence.control_request,
            evidence.operation_refs,
        )
    if evidence.evidence_kind == "SEALED_NO_CONTROL":
        # Evidence proves that the immutable dispatch is sealed, but it omits
        # draft content. Replay the same idempotent dispatch to retrieve the
        # retained typed result; this cannot start a second model execution.
        try:
            replayed_result = await port.execute_v3(prepared, binding, request_id)
        except asyncio.CancelledError:
            raise
        except Exception:
            return await _current_result(database, principal, turn_id)
        # SEALED_NO_CONTROL is positive evidence that this dispatch has a
        # terminal result. UNKNOWN is terminal uncertainty, not an unsealed
        # transport failure: retain its typed refs and release the Turn so a
        # later customer input can proceed. Only UNSEALED/UNAVAILABLE stays
        # recoverable below.
        return await _finish_result(database, principal, turn_id, proof, replayed_result)
    # UNSEALED and UNAVAILABLE are not evidence that no control was requested.
    # Leave the claim recoverable; a later same-dispatch request reads the same
    # AI receipt and cannot invoke another model execution.
    return await _current_result(database, principal, turn_id)

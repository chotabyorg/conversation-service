"""Claimed same-Turn execution with a durable one-attempt fence and no scheduler."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from uuid import UUID

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from core.config import ManagedBinding, Settings
from core.errors import DomainError
from core.logging import logger
from db.database import Database
from models import (
    AgentTurn,
    Conversation,
    TelegramAdmission,
    TelegramImmediateTurn,
    WebsiteAdmission,
    WebsiteImmediateTurn,
)
from schemas.contracts import (
    Channel,
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
from schemas.managed import (
    MAX_RESULT_BYTES,
    ManagedMessage,
    ManagedTurnRequest,
    ManagedTurnResult,
    PreparedManagedRequest,
    prepare_request,
)
from services import telegram, turns
from services.common import db_clock, locked_turn
from services.history_projection import canonical_window, projected_request
from services.managed_client import ManagedAIPort, ManagedPortFailure
from services.website_evidence import recovery_refs

FINALIZATION_SECONDS = 5.0


@dataclass(frozen=True)
class Snapshot:
    turn: TurnRead
    owner_id: str
    integration_id: UUID
    channel: Channel
    binding: ManagedBinding
    messages: tuple[ManagedMessage, ...]
    contract_version: int
    projection_fingerprint: str | None


@dataclass(frozen=True)
class RecoverMarked:
    """No request preparation/binding/network is needed for durable uncertainty."""


async def validate_website_agent(
    session: AsyncSession, turn: AgentTurn, agent_id: UUID, conversation_channel: str
) -> None:
    if conversation_channel == Channel.VOICE.value:
        from services.voice import validate_dispatch
        conversation = await session.get(Conversation, turn.conversation_id)
        if conversation is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        await validate_dispatch(session, turn, conversation, agent_id)
        return
    if conversation_channel == Channel.TELEGRAM.value:
        # Telegram v1 turns predate the Telegram admission model. Preserve their
        # established path while binding every new immediate v2 turn to its
        # durable Telegram admission before dispatch and again before commit.
        if turn.managed_contract_version != 2:
            return
        admission_agent = await session.scalar(select(TelegramAdmission.agent_id).join(
            TelegramImmediateTurn,
            TelegramImmediateTurn.message_id == TelegramAdmission.canonical_message_id,
        ).where(TelegramImmediateTurn.turn_id == turn.id,
                TelegramImmediateTurn.conversation_id == turn.conversation_id,
                TelegramAdmission.conversation_id == turn.conversation_id,
                TelegramAdmission.route == "MANAGED"))
        if (admission_agent != agent_id or turn.managed_projection_agent_id != agent_id):
            raise DomainError(ErrorCode.VALIDATION_ERROR)
        return
    admission_agent = await session.scalar(select(WebsiteAdmission.agent_id).join(
        WebsiteImmediateTurn,
        WebsiteImmediateTurn.message_id == WebsiteAdmission.canonical_message_id,
    ).where(WebsiteImmediateTurn.turn_id == turn.id,
            WebsiteImmediateTurn.conversation_id == turn.conversation_id,
            WebsiteAdmission.conversation_id == turn.conversation_id))
    if ((admission_agent is not None and admission_agent != agent_id)
            or (turn.managed_contract_version == 2 and (
                admission_agent is None or turn.managed_projection_agent_id != agent_id))):
        raise DomainError(ErrorCode.VALIDATION_ERROR)


async def validate_fresh_telegram_dispatch(
    session: AsyncSession, turn: AgentTurn, conversation: Conversation, agent_id: UUID
) -> None:
    """Fence new Telegram v2 AI execution on the pinned account and full input range."""
    if (conversation.channel != Channel.TELEGRAM.value
            or turn.managed_contract_version != 2):
        return
    account = await telegram._telegram_account(
        session, conversation.owner_id, conversation.integration_id
    )
    if (account is None or account.state != "BOUND" or account.bot_id is None
            or turn.managed_projection_agent_id != agent_id):
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    members = await telegram._validated_turn_admissions(
        session, conversation, turn, agent_id
    )
    if not members or any(member.bot_id != account.bot_id for member in members):
        raise DomainError(ErrorCode.VALIDATION_ERROR)


async def claimed_state(
    session: AsyncSession, principal: Principal, turn_id: UUID, proof: TurnExecute
) -> tuple[AgentTurn, TurnResult | RecoverMarked | None]:
    _, turn = await locked_turn(session, principal, turn_id)
    turns.winning_claim(turn, proof.claim_epoch, proof.claim_token.get_secret_value())
    if turn.status in {"COMPLETED", "FAILED"}:
        return turn, TurnResult(turn=TurnRead.model_validate(turn), replayed=True)
    clock = await db_clock(session)
    if turn.status != "CLAIMED" or turn.lease_expires_at is None:
        raise DomainError(ErrorCode.STALE_CLAIM)
    if clock >= turn.lease_expires_at:
        raise DomainError(ErrorCode.LEASE_EXPIRED)
    if turn.managed_dispatch_epoch is not None:
        if turn.managed_dispatch_epoch == proof.claim_epoch:
            raise DomainError(ErrorCode.MANAGED_EXECUTION_STARTED)
        return turn, RecoverMarked()
    return turn, None


async def take_snapshot(
    database: Database, principal: Principal, turn_id: UUID, proof: TurnExecute, settings: Settings
) -> Snapshot | TurnResult | RecoverMarked:
    async with database.session_factory() as session, session.begin():
        turn, retained = await claimed_state(session, principal, turn_id, proof)
        if retained is not None:
            return retained
        # The already locked parent is immutable in scope; fetch its current fields.
        conversation = await session.get(Conversation, turn.conversation_id)
        if conversation is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        scope = (conversation.owner_id, conversation.integration_id, Channel(conversation.channel))
        if (conversation.channel == Channel.VOICE.value
                and conversation.integration_id not in settings.voice_cohort_integration_ids):
            raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
        binding = next(
            (entry for entry in settings.managed_ai_bindings if entry.scope() == scope), None
        )
        if binding is None or settings.managed_ai_base_url is None:
            raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
        await validate_website_agent(session, turn, binding.agent_id, conversation.channel)
        await validate_fresh_telegram_dispatch(
            session, turn, conversation, binding.agent_id
        )
        messages = await canonical_window(session, conversation.id,
            turn.managed_context_first_sequence, turn.context_last_sequence)
        snapshot = Snapshot(TurnRead.model_validate(turn), *scope, binding, messages,
                            turn.managed_contract_version, turn.managed_projection_fingerprint)
        prepare_snapshot(snapshot)
        return snapshot


def prepare_snapshot(snapshot: Snapshot) -> PreparedManagedRequest:
    turn = snapshot.turn
    try:
        request = ManagedTurnRequest(
            contract_version=snapshot.contract_version,
            conversation_id=turn.conversation_id,
            turn_id=turn.id,
            owner_id=snapshot.owner_id,
            integration_id=snapshot.integration_id,
            agent_id=snapshot.binding.agent_id,
            channel=snapshot.channel,
            input_revision=turn.input_revision,
            authority_revision=turn.authority_revision,
            input_first_sequence=turn.input_first_sequence,
            input_last_sequence=turn.input_last_sequence,
            context_last_sequence=turn.context_last_sequence,
            messages=snapshot.messages,
            selected_input_message_ids=tuple(
                message.id
                for message in snapshot.messages
                if turn.input_first_sequence <= message.sequence <= turn.input_last_sequence
            ),
        )
        prepared = prepare_request(request)
        if ((snapshot.projection_fingerprint is not None
                and prepared.fingerprint != snapshot.projection_fingerprint)
                or (snapshot.contract_version == 2 and snapshot.projection_fingerprint is None)):
            raise ValueError("frozen projection changed")
        return prepared
    except (ValueError, TypeError):
        raise DomainError(ErrorCode.VALIDATION_ERROR) from None


async def mark_dispatch(
    database: Database,
    principal: Principal,
    turn_id: UUID,
    proof: TurnExecute,
    prepared: PreparedManagedRequest,
) -> TurnResult | RecoverMarked | None:
    async with database.session_factory() as session, session.begin():
        turn, retained = await claimed_state(session, principal, turn_id, proof)
        if retained is not None:
            return retained
        # Reconstruct again under the final Conversation lock. Neither a stale
        # caller snapshot nor concurrent row/identity drift may become the marker.
        conversation = await session.get(Conversation, turn.conversation_id)
        if conversation is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        await validate_website_agent(
            session, turn, prepared.request.agent_id, conversation.channel
        )
        await validate_fresh_telegram_dispatch(
            session, turn, conversation, prepared.request.agent_id
        )
        messages = await canonical_window(session, conversation.id,
            turn.managed_context_first_sequence, turn.context_last_sequence)
        try:
            current = projected_request(turn, conversation, prepared.request.agent_id, messages)
        except ValueError:
            raise DomainError(ErrorCode.VALIDATION_ERROR) from None
        if (current.fingerprint != prepared.fingerprint or current.body != prepared.body
                or (turn.managed_contract_version == 2
                    and current.fingerprint != turn.managed_projection_fingerprint)):
            raise DomainError(ErrorCode.VALIDATION_ERROR)
        turn.managed_agent_id = prepared.request.agent_id
        turn.managed_dispatch_epoch = proof.claim_epoch
        turn.managed_dispatch_started_at = await db_clock(session)
        turn.managed_request_fingerprint = prepared.fingerprint
        await session.flush()
    # The transaction commit must positively return before any possible HTTP.
    return None


def fail_request(proof: TurnExecute, failure: TurnFailureCode) -> TurnFail:
    return TurnFail(
        claim_epoch=proof.claim_epoch,
        claim_token=proof.claim_token,
        failure_code=failure,
        operation_refs=[],
    )


def terminal_request(
    proof: TurnExecute, result: ManagedTurnResult, prepared: PreparedManagedRequest
) -> TurnComplete | TurnFail:
    result = ManagedTurnResult.model_validate(result.model_dump(mode="json"))
    result.correlate(prepared.request)
    if len(result.model_dump_json().encode("utf-8")) > MAX_RESULT_BYTES:
        raise ValueError("invalid result bound")
    refs = [
        OperationReference.model_validate(ref.model_dump(mode="json"))
        for ref in result.operation_refs
    ]
    if result.status in {"FAILED", "UNKNOWN"}:
        # Only the validated, correlated closed result crosses the logging boundary.
        logger.warning("", extra={"stage": "turn", "turn_id": result.turn_id,
                                  "code": f"MANAGED_{result.status}_{result.error_code}"})
    if result.status in {"DRAFT_READY", "NO_RESPONSE"}:
        return TurnComplete(
            claim_epoch=proof.claim_epoch,
            claim_token=proof.claim_token,
            operation_refs=refs,
            outcome=TurnOutcome(result.status),
            draft_text=result.draft_text,
        )
    if result.status == "UNKNOWN":
        failure = TurnFailureCode.EXECUTION_UNKNOWN
    elif result.error_code in {"CONTEXT_LIMIT", "INVALID_RESULT"}:
        failure = TurnFailureCode.INVALID_RESULT
    elif result.error_code in {"MODEL_TIMEOUT", "EXECUTION_TIMEOUT"}:
        failure = TurnFailureCode.TIMEOUT
    else:
        failure = TurnFailureCode.PROCESSING_ERROR
    return TurnFail(
        claim_epoch=proof.claim_epoch,
        claim_token=proof.claim_token,
        operation_refs=refs,
        failure_code=failure,
    )


async def finalize_cancelled(
    database: Database, principal: Principal, turn_id: UUID, proof: TurnExecute
) -> None:
    async def finish() -> None:
        try:
            async with asyncio.timeout(FINALIZATION_SECONDS):
                await turns.finish_turn(
                    database,
                    principal,
                    turn_id,
                    fail_request(proof, TurnFailureCode.EXECUTION_UNKNOWN),
                )
        except Exception:
            # Stale/expired/uncertain DB outcome leaves the durable marker intact.
            pass

    owner = asyncio.create_task(finish(), name="managed-turn-cancel-finalization")
    while not owner.done():
        try:
            await asyncio.shield(owner)
        except asyncio.CancelledError:
            # Repeated caller cancellation cannot abandon owned DB cleanup.
            continue
    owner.result()


async def execute_claimed_turn(
    database: Database,
    principal: Principal,
    turn_id: UUID,
    proof: TurnExecute,
    settings: Settings,
    port: ManagedAIPort,
    request_id: str = "",
) -> TurnResult:
    snapshot = await take_snapshot(database, principal, turn_id, proof, settings)
    if isinstance(snapshot, TurnResult):
        return snapshot
    if isinstance(snapshot, RecoverMarked):
        terminal_unknown = fail_request(proof, TurnFailureCode.EXECUTION_UNKNOWN)
        terminal_unknown.operation_refs = await recovery_refs(
            database, principal, turn_id, proof, settings, port)
        return await turns.finish_turn(database, principal, turn_id, terminal_unknown)
    prepared = prepare_snapshot(snapshot)
    retained = await mark_dispatch(database, principal, turn_id, proof, prepared)
    if isinstance(retained, TurnResult):
        return retained
    if isinstance(retained, RecoverMarked):
        return await turns.finish_turn(
            database, principal, turn_id, fail_request(proof, TurnFailureCode.EXECUTION_UNKNOWN)
        )
    try:
        result = await port.execute(prepared, snapshot.binding, request_id)
        terminal = terminal_request(proof, result, prepared)
    except asyncio.CancelledError:
        await finalize_cancelled(database, principal, turn_id, proof)
        raise
    except ManagedPortFailure as failure:
        terminal = fail_request(proof, failure.failure_code)
    except Exception:
        terminal = fail_request(proof, TurnFailureCode.EXECUTION_UNKNOWN)
    if (isinstance(terminal, TurnFail)
            and terminal.failure_code == TurnFailureCode.EXECUTION_UNKNOWN):
        refs = await recovery_refs(database, principal, turn_id, proof, settings, port)
        if refs:
            terminal.operation_refs = refs
    # A finish/commit failure must never re-enter HTTP or use a different fingerprint.
    return await turns.finish_turn(database, principal, turn_id, terminal)

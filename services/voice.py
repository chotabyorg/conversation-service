"""Conversation-owned Voice admission, immediate turns and transport evidence."""

from datetime import datetime, timedelta
from typing import Literal
from uuid import UUID, uuid4, uuid5

from sqlalchemy import and_, exists, false, func, or_, select, text
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import (
    AgentTurn,
    Conversation,
    DeliveryAttempt,
    DeliveryReconciliation,
    InboundEvent,
    Message,
    TurnInputMessage,
    VoiceCall,
    VoiceInput,
    VoicePlaybackEvent,
    VoiceResponse,
)
from schemas.contracts import Channel, ErrorCode, Principal
from schemas.voice import (
    VOICE_NAMESPACE,
    CallEnd,
    CallIdentity,
    CallResult,
    CallStart,
    Playback,
    PlaybackResult,
    ResponseBegin,
    ResponseCandidate,
    ResponsePermission,
    Utterance,
    UtteranceResult,
)
from services.common import (
    authorized_conversation,
    db_clock,
    fingerprint,
    message_is_consumed,
)
from services.history_projection import freeze_website_projection

RESPONSE_NAMESPACE = UUID("a70af1c6-2345-438e-9460-dfda809c5660")


def voice_scope(principal: Principal) -> ColumnElement[bool]:
    return or_(false(), *(and_(
        Conversation.owner_id == g.owner_id,
        Conversation.integration_id == g.integration_id,
        Conversation.channel == Channel.VOICE.value,
    ) for g in principal.grants
        if g.channel == Channel.VOICE and g.provider_namespace == VOICE_NAMESPACE))


def grant(principal: Principal, owner: str, integration: UUID) -> None:
    if not any(g.owner_id == owner and g.integration_id == integration
               and g.channel == Channel.VOICE and g.provider_namespace == VOICE_NAMESPACE
               for g in principal.grants):
        raise DomainError(ErrorCode.NOT_FOUND)


def configured(settings: Settings, conversation: Conversation, call: VoiceCall) -> bool:
    return conversation.integration_id in settings.voice_cohort_integration_ids and any(
        b.owner_id == conversation.owner_id and b.integration_id == conversation.integration_id
        and b.channel == Channel.VOICE and b.agent_id == call.agent_id
        for b in settings.managed_ai_bindings
    )


async def locked_call(session: AsyncSession, principal: Principal,
                      request: CallIdentity) -> tuple[Conversation, VoiceCall]:
    parent = await session.scalar(select(VoiceCall.conversation_id).where(
        VoiceCall.id == request.call_id))
    if parent is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    conversation = await authorized_conversation(session, principal, parent, lock=True)
    grant(principal, conversation.owner_id, conversation.integration_id)
    call = await session.scalar(select(VoiceCall).where(VoiceCall.id == request.call_id)
                                .with_for_update().execution_options(populate_existing=True))
    if (call is None or call.media_session_id != request.media_session_id
            or conversation.channel != "VOICE"):
        raise DomainError(ErrorCode.NOT_FOUND)
    return conversation, call


def call_state(call: VoiceCall, clock: datetime) -> Literal["ACTIVE", "ENDED", "EXPIRED"]:
    return "ENDED" if call.ended_at is not None else (
        "EXPIRED" if clock >= call.expires_at else "ACTIVE")


def live(settings: Settings, conversation: Conversation, call: VoiceCall,
         clock: datetime) -> None:
    if call_state(call, clock) != "ACTIVE" or not configured(settings, conversation, call):
        raise DomainError(ErrorCode.CONTROL_MODE_CONFLICT)


async def start(database: Database, principal: Principal, request: CallStart,
                settings: Settings) -> CallResult:
    grant(principal, request.owner_id, request.integration_id)
    async with database.session_factory() as session, session.begin():
        # Global call identity cannot be reassigned by a different owner/session.
        await session.execute(text("SELECT pg_advisory_xact_lock(:key)"),
                              {"key": request.call_id.int % (1 << 63)})
        existing = await session.get(VoiceCall, request.call_id)
        if existing is not None:
            conversation, call = await locked_call(session, principal, request)
            if (conversation.owner_id != request.owner_id
                    or conversation.integration_id != request.integration_id
                    or call.agent_id != request.agent_id):
                raise DomainError(ErrorCode.EVENT_CONFLICT)
            return CallResult(call_id=call.id, media_session_id=call.media_session_id,
                              conversation_id=conversation.id,
                              state=call_state(call, await db_clock(session)), replayed=True)
        conversation = Conversation(id=uuid4(), owner_id=request.owner_id,
                                    integration_id=request.integration_id, channel="VOICE",
                                    provider_thread_id=str(request.call_id))
        clock = await db_clock(session)
        call = VoiceCall(id=request.call_id, conversation_id=conversation.id,
                         media_session_id=request.media_session_id, agent_id=request.agent_id,
                         started_at=clock,
                         expires_at=clock + timedelta(seconds=settings.voice_call_lease_seconds))
        if not configured(settings, conversation, call):
            raise DomainError(ErrorCode.NOT_FOUND)
        session.add(conversation)
        await session.flush()
        session.add(call)
        await session.flush()
        result = CallResult(call_id=call.id, media_session_id=call.media_session_id,
                            conversation_id=conversation.id, state="ACTIVE")
    return result


async def heartbeat(database: Database, principal: Principal, request: CallIdentity,
                    settings: Settings) -> CallResult:
    async with database.session_factory() as session, session.begin():
        conversation, call = await locked_call(session, principal, request)
        clock = await db_clock(session)
        state = call_state(call, clock)
        if state == "ACTIVE":
            live(settings, conversation, call, clock)
            if conversation.control_mode != "AGENT":
                raise DomainError(ErrorCode.CONTROL_MODE_CONFLICT)
            call.expires_at = min(
                clock + timedelta(seconds=settings.voice_call_lease_seconds),
                call.started_at + timedelta(hours=1),
            )
        result = CallResult(**request.model_dump(), conversation_id=conversation.id, state=state)
    return result


async def admit(database: Database, principal: Principal, request: Utterance,
                settings: Settings) -> UtteranceResult:
    digest = fingerprint(request.model_dump(mode="json"))
    async with database.session_factory() as session, session.begin():
        conversation, call = await locked_call(session, principal, request)
        previous = await session.get(VoiceInput, (call.id, request.sequence))
        if previous is not None:
            if previous.fingerprint != digest:
                raise DomainError(ErrorCode.EVENT_CONFLICT)
            return UtteranceResult(call_id=call.id, media_session_id=call.media_session_id,
                                   sequence=request.sequence, message_id=previous.message_id,
                                   replayed=True)
        clock = await db_clock(session)
        live(settings, conversation, call, clock)
        if request.sequence != call.last_sequence + 1:
            raise DomainError(ErrorCode.INPUT_REVISION_CONFLICT)
        if call.last_sequence >= 10000:
            raise DomainError(ErrorCode.PAYLOAD_TOO_LARGE)
        conversation.last_message_sequence += 1
        conversation.input_revision += 1
        conversation.settling_revision += 1
        conversation.updated_at = clock
        event_id, message_id = uuid4(), uuid4()
        session.add(InboundEvent(
            id=event_id, conversation_id=conversation.id, owner_id=conversation.owner_id,
            integration_id=conversation.integration_id, channel="VOICE",
            provider_namespace=VOICE_NAMESPACE, provider_event_id=f"{call.id}:{request.sequence}",
            contract_version=1, fingerprint=digest, accepted_at=clock,
        ))
        await session.flush()
        session.add(Message(
            id=message_id, conversation_id=conversation.id,
            sequence=conversation.last_message_sequence, input_revision=conversation.input_revision,
            settling_revision=conversation.settling_revision, actor="CUSTOMER", direction="INBOUND",
            content_type="TEXT", text=request.text, source_inbound_event_id=event_id,
            created_at=clock,
        ))
        await session.flush()
        session.add(VoiceInput(call_id=call.id, sequence=request.sequence,
                               conversation_id=conversation.id, message_id=message_id,
                               fingerprint=digest))
        call.last_sequence = request.sequence
        await session.flush()
        result = UtteranceResult(call_id=call.id, media_session_id=call.media_session_id,
                                 sequence=request.sequence, message_id=message_id, replayed=False)
    return result


async def validate_dispatch(session: AsyncSession, turn: AgentTurn,
                            conversation: Conversation, agent_id: UUID) -> None:
    """Recheck the live call and exact canonical input under the Conversation lock."""
    call = await session.scalar(select(VoiceCall).where(
        VoiceCall.conversation_id == conversation.id))
    mapped = (await session.execute(select(TurnInputMessage, VoiceInput, Message).join(
        VoiceInput, VoiceInput.message_id == TurnInputMessage.message_id,
    ).join(Message, Message.id == VoiceInput.message_id).where(
        TurnInputMessage.turn_id == turn.id,
        TurnInputMessage.conversation_id == conversation.id,
    ))).all()
    if (call is None or call.agent_id != agent_id or call_state(call, await db_clock(session))
            != "ACTIVE" or conversation.control_mode != "AGENT"
            or conversation.authority_revision != turn.authority_revision
            or turn.managed_contract_version != 2 or turn.managed_projection_agent_id != agent_id
            or len(mapped) != 1 or turn.input_first_sequence != turn.input_last_sequence):
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    owner, admission, message = mapped[0]
    if (admission.call_id != call.id or admission.conversation_id != conversation.id
            or message.conversation_id != conversation.id or owner.ordinal != 0
            or owner.sequence != message.sequence or message.sequence != turn.input_first_sequence
            or message.actor != "CUSTOMER" or message.direction != "INBOUND"):
        raise DomainError(ErrorCode.VALIDATION_ERROR)


async def select_turn(database: Database, principal: Principal, settings: Settings) -> UUID | None:
    after: UUID | None = None
    while True:
        async with database.session_factory() as session:
            query = select(VoiceCall.conversation_id).join(
                Conversation, Conversation.id == VoiceCall.conversation_id,
            ).where(voice_scope(principal), or_(
                (VoiceCall.ended_at.is_(None) & (VoiceCall.expires_at > func.clock_timestamp())),
                exists(select(AgentTurn.id).where(
                    AgentTurn.conversation_id == VoiceCall.conversation_id,
                    AgentTurn.status.in_(("PENDING", "CLAIMED")),
                )),
            ))
            if after is not None:
                query = query.where(VoiceCall.conversation_id > after)
            ids = list((await session.scalars(query.order_by(VoiceCall.conversation_id)
                                             .limit(100))).all())
        if not ids:
            return None
        for parent in ids:
            try:
                async with database.session_factory() as session, session.begin():
                    conversation = await authorized_conversation(
                        session, principal, parent, lock=True)
                    grant(principal, conversation.owner_id, conversation.integration_id)
                    call = await session.scalar(select(VoiceCall).where(
                        VoiceCall.conversation_id == parent).with_for_update())
                    assert call is not None
                    clock = await db_clock(session)
                    active = await session.scalar(select(AgentTurn).where(
                        AgentTurn.conversation_id == parent,
                        AgentTurn.status.in_(("PENDING", "CLAIMED")),
                    ).with_for_update())
                    if active is not None and active.managed_dispatch_epoch is not None:
                        # Same-Turn UNKNOWN recovery must survive call expiry/revocation.
                        if active.lease_expires_at is not None and active.lease_expires_at <= clock:
                            return active.id
                        continue
                    if active is not None:
                        if active.status == "PENDING" or (
                            active.lease_expires_at is not None and active.lease_expires_at <= clock
                        ):
                            # A closed/revoked unmarked Turn is claimed only to fail
                            # validation in the executor, without sending to AI.
                            return active.id
                        continue
                    if (call_state(call, clock) != "ACTIVE"
                            or not configured(settings, conversation, call)
                            or conversation.control_mode != "AGENT"):
                        continue
                    message = await session.scalar(select(Message).where(
                        Message.conversation_id == parent, Message.actor == "CUSTOMER",
                        Message.direction == "INBOUND", ~message_is_consumed(),
                    ).order_by(Message.sequence).limit(1))
                    if message is None:
                        continue
                    admitted = await session.scalar(select(VoiceInput).where(
                        VoiceInput.message_id == message.id, VoiceInput.call_id == call.id))
                    if admitted is None:
                        continue  # Generic ingress cannot authorize managed Voice execution.
                    turn = AgentTurn(
                        id=uuid4(), conversation_id=parent, selection_key=message.id,
                        creation_fingerprint=fingerprint({"voice_message": str(message.id)}),
                        input_first_sequence=message.sequence, input_last_sequence=message.sequence,
                        context_last_sequence=message.sequence,
                        input_revision=message.input_revision,
                        authority_revision=conversation.authority_revision,
                        settling_revision=message.settling_revision,
                        status="PENDING", claim_epoch=0, operation_refs=[], created_at=clock,
                    )
                    await freeze_website_projection(session, conversation, turn, call.agent_id)
                    session.add(turn)
                    await session.flush()
                    session.add(TurnInputMessage(
                        message_id=message.id, conversation_id=parent,
                        turn_id=turn.id, sequence=message.sequence, ordinal=0))
                    await session.flush()
                    return turn.id
            except DomainError as exc:
                # Roll back one invalid candidate before considering later calls.
                # Unexpected failures still escape to worker health/observability.
                if exc.code not in {ErrorCode.VALIDATION_ERROR, ErrorCode.NOT_FOUND}:
                    raise
        after = ids[-1]


async def next_response(database: Database, principal: Principal, request: CallIdentity,
                        settings: Settings) -> ResponseCandidate:
    async with database.session_factory() as session, session.begin():
        conversation, call = await locked_call(session, principal, request)
        live(settings, conversation, call, await db_clock(session))
        if await session.scalar(select(VoiceResponse.id).where(
            VoiceResponse.call_id == call.id, VoiceResponse.send_authorized.is_(True),
            VoiceResponse.terminal.is_(False),
        ).limit(1)) is not None:
            return ResponseCandidate(**request.model_dump(), turn_id=None)
        turn_id = await session.scalar(select(AgentTurn.id).where(
            AgentTurn.conversation_id == conversation.id, AgentTurn.status == "COMPLETED",
            AgentTurn.outcome == "DRAFT_READY",
            ~exists(select(VoiceResponse.id).where(VoiceResponse.turn_id == AgentTurn.id)),
        ).order_by(AgentTurn.input_first_sequence).limit(1))
        return ResponseCandidate(**request.model_dump(), turn_id=turn_id)


def record_delivery(session: AsyncSession, delivery: DeliveryAttempt, state: str,
                    key: UUID, clock: datetime, *, expected_settling: int | None = None,
                    current_settling: int | None = None) -> None:
    previous = delivery.revision
    delivery.status, delivery.revision, delivery.updated_at = state, previous + 1, clock
    delivery.error_code = ("INTERRUPTED" if state == "INTERRUPTED" else "UNSPECIFIED") if (
        state in {"UNKNOWN", "FAILED", "PARTIAL", "INTERRUPTED"}) else None
    if state in {"DISPATCH_REQUESTED", "SUPERSEDED"}:
        delivery.dispatch_requested_at = clock
        delivery.expected_settling_revision = expected_settling
        delivery.fenced_settling_revision = current_settling
    if state == "PROVIDER_ACCEPTED":
        delivery.provider_accepted_at = delivery.provider_accepted_at or clock
    session.add(DeliveryReconciliation(
        id=uuid4(), conversation_id=delivery.conversation_id, delivery_attempt_id=delivery.id,
        reconciliation_key=key, fingerprint=fingerprint({"voice_state": state, "key": str(key)}),
        expected_revision=previous, result_revision=delivery.revision, status=state,
        error_code=delivery.error_code,
        expected_authority_revision=(delivery.authority_revision
                                     if state == "DISPATCH_REQUESTED" else None),
        expected_settling_revision=expected_settling, result_settling_revision=current_settling,
        accepted_at=clock,
    ))


async def begin(database: Database, principal: Principal, request: ResponseBegin,
                settings: Settings) -> ResponsePermission:
    response_id = uuid5(RESPONSE_NAMESPACE, str(request.turn_id))
    async with database.session_factory() as session, session.begin():
        conversation, call = await locked_call(session, principal, request)
        previous = await session.get(VoiceResponse, response_id)
        if previous is not None:
            if previous.call_id != call.id:
                raise DomainError(ErrorCode.NOT_FOUND)
            # A lost permission response never permits a second playback.
            return ResponsePermission(**request.model_dump(), response_id=response_id,
                                      send=False, replayed=True)
        if await session.scalar(select(VoiceResponse.id).where(
            VoiceResponse.call_id == call.id, VoiceResponse.send_authorized.is_(True),
            VoiceResponse.terminal.is_(False),
        ).limit(1)) is not None:
            raise DomainError(ErrorCode.LEASE_HELD)
        clock = await db_clock(session)
        turn = await session.scalar(select(AgentTurn).where(
            AgentTurn.id == request.turn_id, AgentTurn.conversation_id == conversation.id,
        ).with_for_update())
        if turn is None or turn.status != "COMPLETED" or turn.draft_text is None:
            raise DomainError(ErrorCode.NO_DRAFT)
        # Validate retained Voice input ownership even when the call is no longer live.
        count = await session.scalar(select(VoiceInput.message_id).join(
            TurnInputMessage, TurnInputMessage.message_id == VoiceInput.message_id,
        ).where(TurnInputMessage.turn_id == turn.id, VoiceInput.call_id == call.id))
        if count is None or turn.managed_projection_agent_id != call.agent_id:
            raise DomainError(ErrorCode.NOT_FOUND)
        allowed = (call_state(call, clock) == "ACTIVE" and configured(settings, conversation, call)
                   and conversation.control_mode == "AGENT"
                   and conversation.authority_revision == turn.authority_revision
                   and conversation.settling_revision == turn.settling_revision)
        if await session.scalar(select(DeliveryAttempt.id).where(
            DeliveryAttempt.source_turn_id == turn.id)) is not None:
            raise DomainError(ErrorCode.DELIVERY_INTENT_EXISTS)
        delivery = DeliveryAttempt(
            id=uuid4(), conversation_id=conversation.id, source_turn_id=turn.id,
            delivery_key=response_id, creation_fingerprint=fingerprint({"voice": str(response_id)}),
            authority_revision=turn.authority_revision, status="GENERATED", revision=0,
            created_at=clock, updated_at=clock,
        )
        session.add(delivery)
        await session.flush()
        record_delivery(session, delivery, "DISPATCH_REQUESTED" if allowed else "SUPERSEDED",
                        response_id, clock, expected_settling=turn.settling_revision,
                        current_settling=conversation.settling_revision)
        session.add(VoiceResponse(id=response_id, call_id=call.id,
                                  conversation_id=conversation.id, turn_id=turn.id,
                                  delivery_id=delivery.id, send_authorized=allowed,
                                  terminal=not allowed))
        await session.flush()
        result = ResponsePermission(**request.model_dump(), response_id=response_id,
                                    send=allowed, text=turn.draft_text if allowed else None,
                                    replayed=False)
    return result


async def playback(database: Database, principal: Principal, request: Playback,
                   settings: Settings) -> PlaybackResult:
    digest = fingerprint(request.model_dump(mode="json"))
    async with database.session_factory() as session, session.begin():
        conversation, call = await locked_call(session, principal, request)
        response = await session.scalar(select(VoiceResponse).where(
            VoiceResponse.id == request.response_id, VoiceResponse.call_id == call.id,
        ).with_for_update())
        if response is None or not response.send_authorized:
            raise DomainError(ErrorCode.NOT_FOUND)
        previous = await session.get(VoicePlaybackEvent, (response.id, request.event_sequence))
        if previous is not None:
            if previous.fingerprint != digest:
                raise DomainError(ErrorCode.DELIVERY_RECONCILIATION_CONFLICT)
            return PlaybackResult(call_id=call.id, media_session_id=call.media_session_id,
                                  response_id=response.id, event_sequence=request.event_sequence,
                                  replayed=True)
        if (response.terminal or request.event_sequence != response.last_event_sequence + 1
                or request.generated_bytes < response.generated_bytes
                or request.transport_accepted_bytes < response.accepted_bytes):
            raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
        clock = await db_clock(session)
        delivery = await session.get(DeliveryAttempt, response.delivery_id, with_for_update=True)
        assert delivery is not None
        # COMPLETED means PCM accepted by transport, never DELIVERED/READ/heard.
        if request.terminal:
            status = {"COMPLETED": "PROVIDER_ACCEPTED", "NOT_STARTED": "FAILED"}.get(
                request.state, request.state)
            if request.state == "FAILED" and request.transport_accepted_bytes:
                status = "PARTIAL"
            record_delivery(session, delivery, status,
                            uuid5(response.id, str(request.event_sequence)), clock)
        session.add(VoicePlaybackEvent(
            response_id=response.id, sequence=request.event_sequence, call_id=call.id,
            fingerprint=digest, state=request.state, generated_bytes=request.generated_bytes,
            accepted_bytes=request.transport_accepted_bytes, terminal=request.terminal,
            accepted_at=clock,
        ))
        response.last_event_sequence = request.event_sequence
        response.generated_bytes = request.generated_bytes
        response.accepted_bytes = request.transport_accepted_bytes
        response.terminal = request.terminal
        if request.state == "COMPLETED":
            turn = await session.get(AgentTurn, response.turn_id)
            assert turn is not None and turn.draft_text is not None
            conversation.last_message_sequence += 1
            conversation.updated_at = clock
            session.add(Message(
                id=uuid4(), conversation_id=conversation.id,
                sequence=conversation.last_message_sequence, actor="AGENT", direction="OUTBOUND",
                content_type="TEXT", text=turn.draft_text, source_agent_turn_id=turn.id,
                created_at=clock,
            ))
        await session.flush()
        result = PlaybackResult(call_id=call.id, media_session_id=call.media_session_id,
                                response_id=response.id, event_sequence=request.event_sequence,
                                replayed=False)
    return result


async def end(database: Database, principal: Principal, request: CallEnd,
              settings: Settings) -> CallResult:
    async with database.session_factory() as session, session.begin():
        conversation, call = await locked_call(session, principal, request)
        clock = await db_clock(session)
        replayed = call.ended_at is not None
        if replayed and call.end_reason != request.reason:
            raise DomainError(ErrorCode.EVENT_CONFLICT)
        if not replayed:
            await close_call(session, conversation, call, clock, request.reason)
        await session.flush()
        result = CallResult(call_id=call.id, media_session_id=call.media_session_id,
                            conversation_id=conversation.id, state="ENDED", replayed=replayed)
    return result


async def close_call(session: AsyncSession, conversation: Conversation, call: VoiceCall,
                     clock: datetime, reason: str) -> None:
    call.ended_at, call.end_reason = clock, reason
    conversation.authority_revision += 1
    conversation.updated_at = clock
    pending = (await session.scalars(select(VoiceResponse).where(
        VoiceResponse.call_id == call.id, VoiceResponse.send_authorized.is_(True),
        VoiceResponse.terminal.is_(False),
    ))).all()
    for response in pending:
        delivery = await session.get(DeliveryAttempt, response.delivery_id)
        assert delivery is not None
        record_delivery(session, delivery, "UNKNOWN", uuid5(response.id, "call-end"), clock)


async def expire_calls(database: Database, principal: Principal) -> None:
    # Bounded sweep. Admission/dispatch already reject expired calls without it;
    # this persists the missing end event and unresolved transport uncertainty.
    async with database.session_factory() as session:
        clock = await db_clock(session)
        calls = list((await session.scalars(select(VoiceCall.conversation_id).join(
            Conversation, Conversation.id == VoiceCall.conversation_id,
        ).where(voice_scope(principal), VoiceCall.ended_at.is_(None),
                VoiceCall.expires_at <= clock).order_by(VoiceCall.expires_at).limit(100))).all())
    for parent in calls:
        async with database.session_factory() as session, session.begin():
            conversation = await authorized_conversation(session, principal, parent, lock=True)
            grant(principal, conversation.owner_id, conversation.integration_id)
            call = await session.scalar(select(VoiceCall).where(
                VoiceCall.conversation_id == parent).with_for_update())
            clock = await db_clock(session)
            if call is not None and call.ended_at is None and clock >= call.expires_at:
                await close_call(session, conversation, call, clock, "TIMEOUT")
                await session.flush()

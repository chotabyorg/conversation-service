"""Telegram-only sticky routing and immediate execution over Foundation turns."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, cast
from uuid import UUID, uuid4, uuid5

from sqlalchemy import and_, exists, or_, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql.elements import ColumnElement
from sqlalchemy.sql.selectable import Select

from core.config import ManagedBinding, Settings
from core.errors import DomainError
from db.database import Database
from models import (
    AgentTurn,
    Conversation,
    DeliveryAttempt,
    DeliveryReconciliation,
    InboundEvent,
    Message,
    TelegramImmediateTurn,
    TelegramResponseAttempt,
)
from models import (
    TelegramAdmission as TelegramAdmissionRow,
)
from schemas.contracts import Channel, ErrorCode, Principal
from schemas.telegram import (
    TELEGRAM_NAMESPACE,
    TelegramAdmissionResult,
    TelegramIdentity,
    TelegramResponseBegin,
    TelegramResponseBegun,
    TelegramResponseComplete,
    TelegramResponseCompleted,
    TelegramResult,
)
from schemas.telegram import (
    TelegramAdmission as AdmissionRequest,
)
from services.common import authorized_conversation, db_clock, fingerprint

RESPONSE_NAMESPACE = UUID("6c5798bb-94e0-4e79-8bc9-f1cc7d8ca0ad")


def expected_response_attempt_id(admission_id: UUID) -> UUID:
    return uuid5(RESPONSE_NAMESPACE, str(admission_id))


def binding_for(settings: Settings, request: TelegramIdentity) -> ManagedBinding:
    binding = next((item for item in settings.managed_ai_bindings
                    if item.owner_id == request.owner_id
                    and item.integration_id == request.integration_id
                    and item.channel == Channel.TELEGRAM), None)
    if binding is None or binding.agent_id != request.agent_id:
        raise DomainError(ErrorCode.NOT_FOUND)
    return binding


def authorize(principal: Principal, request: TelegramIdentity, settings: Settings) -> None:
    if principal.role not in {"FULL", "TELEGRAM"}:
        raise DomainError(ErrorCode.NOT_FOUND)
    if not any(g.owner_id == request.owner_id
               and g.integration_id == request.integration_id
               and g.channel == Channel.TELEGRAM
               and g.provider_namespace == TELEGRAM_NAMESPACE
               for g in principal.grants):
        raise DomainError(ErrorCode.NOT_FOUND)
    binding_for(settings, request)


def identity_filter(request: TelegramIdentity) -> tuple[ColumnElement[bool], ...]:
    return (TelegramAdmissionRow.owner_id == request.owner_id,
            TelegramAdmissionRow.integration_id == request.integration_id,
            TelegramAdmissionRow.channel == "TELEGRAM",
            TelegramAdmissionRow.provider_namespace == TELEGRAM_NAMESPACE,
            TelegramAdmissionRow.provider_event_id == request.provider_event_id)


async def retained(session: AsyncSession, request: TelegramIdentity) -> TelegramAdmissionRow:
    admission = await session.scalar(select(TelegramAdmissionRow).where(*identity_filter(request)))
    if (admission is None or admission.agent_id != request.agent_id
            or admission.provider_thread_id != request.provider_thread_id):
        raise DomainError(ErrorCode.NOT_FOUND)
    return admission


async def admit(database: Database, principal: Principal, request: AdmissionRequest,
               settings: Settings) -> TelegramAdmissionResult:
    authorize(principal, request, settings)
    immutable = request.model_dump(mode="json", exclude={"desired_route"})
    digest = fingerprint({"contract_version": 1, "channel": "TELEGRAM",
                          "provider_namespace": TELEGRAM_NAMESPACE, **immutable})
    async with database.session_factory() as session, session.begin():
        # The advisory key is only a short-lived serialization aid; the durable
        # provider-identity unique constraint remains the authority.
        key = fingerprint({"owner": request.owner_id,
                           "integration": str(request.integration_id),
                           "namespace": TELEGRAM_NAMESPACE,
                           "provider_event_id": request.provider_event_id})
        await session.execute(text("SELECT pg_advisory_xact_lock(:key)"),
                              {"key": int(key[:16], 16) - (1 << 63)})
        existing = await session.scalar(
            select(TelegramAdmissionRow).where(*identity_filter(request))
        )
        if existing is not None:
            if existing.fingerprint != digest:
                raise DomainError(ErrorCode.EVENT_CONFLICT)
            result = TelegramAdmissionResult(
                route=cast(Literal["MANAGED", "LEGACY"], existing.route),
                admission_id=existing.id, replayed=True,
            )
        else:
            route: Literal["MANAGED", "LEGACY"] = (
                "MANAGED" if request.desired_route == "MANAGED"
                and request.integration_id in settings.telegram_cohort_integration_ids
                else "LEGACY"
            )
            conversation_id = canonical_message_id = None
            if route == "MANAGED":
                await session.execute(insert(Conversation).values(
                    id=uuid4(), owner_id=request.owner_id,
                    integration_id=request.integration_id, channel="TELEGRAM",
                    provider_thread_id=request.provider_thread_id,
                ).on_conflict_do_nothing(constraint="uq_conversation_identity"))
                conversation = await session.scalar(select(Conversation).where(
                    Conversation.owner_id == request.owner_id,
                    Conversation.integration_id == request.integration_id,
                    Conversation.channel == "TELEGRAM",
                    Conversation.provider_thread_id == request.provider_thread_id,
                ).with_for_update().execution_options(populate_existing=True))
                if conversation is None:
                    raise DomainError(ErrorCode.NOT_FOUND)
                clock = await db_clock(session)
                event_id, canonical_message_id = uuid4(), uuid4()
                conversation.last_message_sequence += 1
                conversation.input_revision += 1
                conversation.updated_at = clock
                session.add(InboundEvent(
                    id=event_id, conversation_id=conversation.id, owner_id=request.owner_id,
                    integration_id=request.integration_id, channel="TELEGRAM",
                    provider_namespace=TELEGRAM_NAMESPACE,
                    provider_event_id=request.provider_event_id, contract_version=1,
                    fingerprint=digest, accepted_at=clock,
                ))
                await session.flush()
                session.add(Message(
                    id=canonical_message_id, conversation_id=conversation.id,
                    sequence=conversation.last_message_sequence, actor="CUSTOMER",
                    direction="INBOUND", content_type="TEXT", text=request.text,
                    source_inbound_event_id=event_id, created_at=clock,
                ))
                await session.flush()
                conversation_id = conversation.id
            row = TelegramAdmissionRow(
                id=uuid4(), owner_id=request.owner_id, integration_id=request.integration_id,
                agent_id=request.agent_id, channel="TELEGRAM",
                provider_namespace=TELEGRAM_NAMESPACE,
                provider_event_id=request.provider_event_id,
                provider_thread_id=request.provider_thread_id,
                update_id=request.update_id, chat_id=str(request.chat_id),
                message_id=str(request.message_id), sender_id=str(request.sender_id),
                bot_id=str(request.bot_id), fingerprint=digest, route=route,
                conversation_id=conversation_id, canonical_message_id=canonical_message_id,
            )
            session.add(row)
            await session.flush()
            result = TelegramAdmissionResult(route=route, admission_id=row.id, replayed=False)
    return result


async def admission_turn(session: AsyncSession, admission: TelegramAdmissionRow) -> AgentTurn:
    if admission.route != "MANAGED" or admission.canonical_message_id is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    turn = await session.scalar(select(AgentTurn).join(
        TelegramImmediateTurn, TelegramImmediateTurn.turn_id == AgentTurn.id,
    ).where(TelegramImmediateTurn.message_id == admission.canonical_message_id))
    if turn is None:
        raise DomainError(ErrorCode.NOT_FOUND)
    return turn


async def result(database: Database, principal: Principal, request: TelegramIdentity,
                 settings: Settings) -> TelegramResult:
    authorize(principal, request, settings)
    async with database.session_factory() as session, session.begin():
        admission = await retained(session, request)
        if admission.route != "MANAGED":
            raise DomainError(ErrorCode.NOT_FOUND)
        turn = await session.scalar(select(AgentTurn).join(
            TelegramImmediateTurn, TelegramImmediateTurn.turn_id == AgentTurn.id,
        ).where(TelegramImmediateTurn.message_id == admission.canonical_message_id))
        if turn is None or turn.status in {"PENDING", "CLAIMED"}:
            return TelegramResult(**request.model_dump(), state="PENDING")
        if turn.status == "FAILED":
            state = "UNKNOWN" if turn.failure_code == "EXECUTION_UNKNOWN" else "FAILED"
            return TelegramResult(
                **request.model_dump(),
                state=cast(Literal["PENDING", "DRAFT_READY", "NO_RESPONSE", "FAILED", "UNKNOWN"],
                           state),
            )
        return TelegramResult(
            **request.model_dump(), state="DRAFT_READY" if turn.outcome == "DRAFT_READY"
            else "NO_RESPONSE", draft_text=turn.draft_text,
        )


def _reconciliation_key(response_attempt_id: UUID, status: str, reference: str = "") -> UUID:
    return uuid5(RESPONSE_NAMESPACE, f"{response_attempt_id}:{status}:{reference}")


def _record_reconciliation(session: AsyncSession, delivery: DeliveryAttempt, conversation_id: UUID,
                           status: str, *, provider_message_id: str | None = None,
                           error_code: str | None = None, key: UUID,
                           expected_revision: int, authority_revision: int | None = None,
                           clock: datetime) -> None:
    delivery.status = status
    delivery.revision += 1
    delivery.updated_at = clock
    delivery.provider_message_id = provider_message_id or delivery.provider_message_id
    delivery.error_code = error_code
    if status == "DISPATCH_REQUESTED" and delivery.dispatch_requested_at is None:
        delivery.dispatch_requested_at = clock
    if status == "PROVIDER_ACCEPTED" and delivery.provider_accepted_at is None:
        delivery.provider_accepted_at = clock
    payload = {"status": status, "provider_message_id": provider_message_id,
               "error_code": error_code, "expected_authority_revision": authority_revision}
    session.add(DeliveryReconciliation(
        id=uuid4(), conversation_id=conversation_id, delivery_attempt_id=delivery.id,
        reconciliation_key=key,
        fingerprint=fingerprint({"contract_version": 1, "delivery_attempt_id": str(delivery.id),
                                 **payload}),
        expected_revision=expected_revision, result_revision=delivery.revision,
        status=status, provider_message_id=provider_message_id, error_code=error_code,
        expected_authority_revision=authority_revision, accepted_at=clock,
    ))


async def begin_response(database: Database, principal: Principal,
                         request: TelegramResponseBegin,
                         settings: Settings) -> TelegramResponseBegun:
    authorize(principal, request, settings)
    async with database.session_factory() as session, session.begin():
        admission = await retained(session, request)
        if admission.route != "MANAGED" or admission.conversation_id is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        if request.response_attempt_id != expected_response_attempt_id(admission.id):
            raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
        conversation = await authorized_conversation(
            session, principal, admission.conversation_id, lock=True)
        turn = await admission_turn(session, admission)
        if turn.status != "COMPLETED" or turn.draft_text is None:
            raise DomainError(ErrorCode.NO_DRAFT)
        if (conversation.control_mode != "AGENT"
                or conversation.authority_revision != turn.authority_revision):
            raise DomainError(ErrorCode.AUTHORITY_REVISION_CONFLICT)
        attempt = await session.get(TelegramResponseAttempt, request.response_attempt_id)
        if attempt is not None:
            if attempt.admission_id != admission.id:
                raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
            delivery = await session.get(DeliveryAttempt, attempt.delivery_id)
            if delivery is None:
                raise DomainError(ErrorCode.NOT_FOUND)
            # An unresolved prior dispatch is never resent. Its outcome stays open
            # so a concurrently returning definitive Bot API response can upgrade it.
            if delivery.status == "DISPATCH_REQUESTED":
                clock = await db_clock(session)
                _record_reconciliation(
                    session, delivery, conversation.id, "UNKNOWN", error_code="INTERRUPTED",
                    key=_reconciliation_key(attempt.id, "UNKNOWN", "INTERRUPTED"),
                    expected_revision=delivery.revision, clock=clock,
                )
                await session.flush()
            return TelegramResponseBegun(response_attempt_id=attempt.id,
                                         draft_text=turn.draft_text, send=False, replayed=True)

        clock = await db_clock(session)
        delivery = await session.scalar(select(DeliveryAttempt).where(
            DeliveryAttempt.source_turn_id == turn.id,
            DeliveryAttempt.conversation_id == conversation.id,
        ).with_for_update())
        if delivery is not None:
            # A second identity cannot acquire another provider-send permission.
            raise DomainError(ErrorCode.DELIVERY_INTENT_EXISTS)
        delivery_id = uuid4()
        delivery = DeliveryAttempt(
            id=delivery_id, conversation_id=conversation.id, delivery_key=turn.id,
            source_turn_id=turn.id,
            creation_fingerprint=fingerprint({"contract_version": 1,
                "conversation_id": str(conversation.id), "delivery_key": str(turn.id),
                "source_turn_id": str(turn.id),
                "expected_authority_revision": conversation.authority_revision}),
            authority_revision=conversation.authority_revision, status="GENERATED", revision=0,
            created_at=clock, updated_at=clock,
        )
        session.add(delivery)
        await session.flush()
        _record_reconciliation(
            session, delivery, conversation.id, "DISPATCH_REQUESTED",
            key=turn.id, expected_revision=0,
            authority_revision=conversation.authority_revision, clock=clock,
        )
        session.add(TelegramResponseAttempt(
            id=request.response_attempt_id, admission_id=admission.id,
            delivery_id=delivery.id, conversation_id=conversation.id, created_at=clock,
        ))
        await session.flush()
        return TelegramResponseBegun(response_attempt_id=request.response_attempt_id,
                                     draft_text=turn.draft_text, send=True, replayed=False)


async def complete_response(database: Database, principal: Principal,
                            request: TelegramResponseComplete,
                            settings: Settings) -> TelegramResponseCompleted:
    authorize(principal, request, settings)
    async with database.session_factory() as session, session.begin():
        admission = await retained(session, request)
        if admission.route != "MANAGED" or admission.conversation_id is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        if request.response_attempt_id != expected_response_attempt_id(admission.id):
            raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
        conversation = await authorized_conversation(
            session, principal, admission.conversation_id, lock=True)
        attempt = await session.scalar(select(TelegramResponseAttempt).where(
            TelegramResponseAttempt.id == request.response_attempt_id,
            TelegramResponseAttempt.admission_id == admission.id,
            TelegramResponseAttempt.conversation_id == conversation.id,
        ).with_for_update())
        if attempt is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        delivery = await session.scalar(select(DeliveryAttempt).where(
            DeliveryAttempt.id == attempt.delivery_id,
            DeliveryAttempt.conversation_id == conversation.id,
        ).with_for_update())
        if delivery is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        outcome = request.outcome
        if attempt.outcome == outcome and delivery.status == outcome and (
                delivery.provider_message_id == request.provider_message_id
                and delivery.error_code == request.error_code):
            return TelegramResponseCompleted(response_attempt_id=attempt.id,
                                             outcome=outcome, replayed=True)
        if attempt.outcome not in {None, "UNKNOWN"} or (
                attempt.outcome == "UNKNOWN" and outcome == "UNKNOWN"):
            raise DomainError(ErrorCode.DELIVERY_RECONCILIATION_CONFLICT)
        if attempt.outcome is None and delivery.status not in {"DISPATCH_REQUESTED", "UNKNOWN"}:
            raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
        clock = await db_clock(session)
        reference = request.provider_message_id or (request.error_code or "")
        reconcile_key = _reconciliation_key(attempt.id, outcome, reference)
        _record_reconciliation(
            session, delivery, conversation.id, outcome,
            provider_message_id=request.provider_message_id,
            error_code=request.error_code, key=reconcile_key,
            expected_revision=delivery.revision, clock=clock,
        )
        attempt.outcome = outcome
        attempt.finished_at = clock
        if outcome == "PROVIDER_ACCEPTED":
            turn = await session.scalar(select(AgentTurn).where(
                AgentTurn.id == delivery.source_turn_id,
                AgentTurn.conversation_id == conversation.id,
            ))
            if turn is None or turn.status != "COMPLETED" or turn.draft_text is None:
                raise DomainError(ErrorCode.NO_DRAFT)
            existing = await session.scalar(select(Message.id).where(
                Message.source_agent_turn_id == turn.id,
                Message.conversation_id == conversation.id,
            ))
            if existing is None:
                conversation.last_message_sequence += 1
                conversation.updated_at = clock
                session.add(Message(
                    id=uuid4(), conversation_id=conversation.id,
                    sequence=conversation.last_message_sequence, actor="AGENT",
                    direction="OUTBOUND", content_type="TEXT", text=turn.draft_text,
                    source_agent_turn_id=turn.id, created_at=clock,
                ))
        await session.flush()
        return TelegramResponseCompleted(response_attempt_id=attempt.id,
                                         outcome=outcome, replayed=False)


async def select_immediate(database: Database, principal: Principal,
                           settings: Settings) -> UUID | None:
    bindings = [b for b in settings.managed_ai_bindings if b.channel == Channel.TELEGRAM]
    if not bindings:
        return None
    scopes = or_(*(and_(TelegramAdmissionRow.owner_id == b.owner_id,
                        TelegramAdmissionRow.integration_id == b.integration_id,
                        TelegramAdmissionRow.agent_id == b.agent_id)
                   for b in bindings))
    candidate = select(Conversation.id).join(
        TelegramAdmissionRow, TelegramAdmissionRow.conversation_id == Conversation.id,
    ).where(TelegramAdmissionRow.route == "MANAGED", scopes,
            or_(~exists(select(TelegramImmediateTurn.message_id).where(
                TelegramImmediateTurn.message_id == TelegramAdmissionRow.canonical_message_id)),
                exists(select(AgentTurn.id).where(
                    AgentTurn.conversation_id == Conversation.id,
                    AgentTurn.status.in_(("PENDING", "CLAIMED")))))).distinct()
    last_candidate_id: UUID | None = None
    while True:
        ids = await _candidate_ids(database, candidate, principal, last_candidate_id)
        if not ids:
            return None
        for conversation_id in ids:
            turn_id = await _select_candidate(database, principal, conversation_id, scopes)
            if turn_id is not None:
                return turn_id
        # Scan position is local to this poll. It advances past skipped rows and
        # resets on the next poll; it is not persistent fairness or scheduling state.
        last_candidate_id = ids[-1]


async def _select_candidate(database: Database, principal: Principal,
                            conversation_id: UUID,
                            scopes: ColumnElement[bool]) -> UUID | None:
    async with database.session_factory() as session, session.begin():
        # Candidate membership is only a hint. Revalidate every decision after
        # acquiring the Conversation lock.
        conversation = await authorized_conversation(session, principal, conversation_id,
                                                     lock=True)
        if conversation.control_mode != "AGENT":
            return None
        clock = await db_clock(session)
        active = await session.scalar(select(AgentTurn).where(
            AgentTurn.conversation_id == conversation_id,
            AgentTurn.status.in_(("PENDING", "CLAIMED")),
        ).with_for_update())
        if active is not None:
            is_telegram = await session.scalar(select(TelegramImmediateTurn.turn_id).where(
                TelegramImmediateTurn.turn_id == active.id))
            if is_telegram is None:
                return None
            if (active.status == "CLAIMED" and active.lease_expires_at is not None
                    and active.lease_expires_at > clock):
                return None
            return active.id
        message = await session.scalar(select(Message).join(
            TelegramAdmissionRow,
            TelegramAdmissionRow.canonical_message_id == Message.id,
        ).where(Message.conversation_id == conversation_id,
                TelegramAdmissionRow.route == "MANAGED", scopes,
                Message.actor == "CUSTOMER", Message.direction == "INBOUND",
                ~exists(select(TelegramImmediateTurn.message_id).where(
                    TelegramImmediateTurn.message_id == Message.id)))
            .order_by(Message.sequence).limit(1).with_for_update())
        if message is None:
            return None
        agent_id = await session.scalar(select(TelegramAdmissionRow.agent_id).where(
            TelegramAdmissionRow.canonical_message_id == message.id,
            TelegramAdmissionRow.conversation_id == conversation_id,
        ))
        if agent_id is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        turn = AgentTurn(
            id=uuid4(), conversation_id=conversation_id, selection_key=message.id,
            creation_fingerprint=fingerprint({
                "contract_version": 1, "conversation_id": str(conversation_id),
                "selection_key": str(message.id),
                "input_first_sequence": message.sequence,
                "input_last_sequence": message.sequence,
                "context_last_sequence": message.sequence,
                "expected_input_revision": conversation.input_revision,
                "expected_authority_revision": conversation.authority_revision,
            }),
            input_first_sequence=message.sequence, input_last_sequence=message.sequence,
            context_last_sequence=message.sequence,
            input_revision=conversation.input_revision,
            authority_revision=conversation.authority_revision,
            status="PENDING", claim_epoch=0, operation_refs=[], created_at=clock,
        )
        # This is the same bounded, immutable v2 projection used by the
        # existing immediate Website coordinator; Telegram adds no scheduler.
        from services.history_projection import freeze_website_projection
        await freeze_website_projection(session, conversation, turn, agent_id)
        session.add(turn)
        await session.flush()
        session.add(TelegramImmediateTurn(message_id=message.id,
                                           conversation_id=conversation_id, turn_id=turn.id))
        await session.flush()
        return turn.id


async def _candidate_ids(database: Database, candidate: Select[tuple[UUID]],
                         principal: Principal,
                         after_id: UUID | None = None) -> list[UUID]:
    # UUID cursor is bounded scan progress for one poll only. The row lock in
    # _select_candidate remains the authority for every selection and mutation.
    from services.common import scope_filter
    page = candidate.where(scope_filter(principal))
    if after_id is not None:
        page = page.where(Conversation.id > after_id)
    async with database.session_factory() as session, session.begin():
        return list((await session.scalars(
            page.order_by(Conversation.id).limit(100)
        )).all())

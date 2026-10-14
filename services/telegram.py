"""Telegram-only sticky routing and immediate execution over Foundation turns."""

from __future__ import annotations

from datetime import datetime
from typing import Literal, TypedDict, cast
from uuid import UUID, uuid4, uuid5

from sqlalchemy import and_, exists, false, literal, or_, select, text, tuple_
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
    TelegramProviderAccount,
    TelegramResponseAttempt,
    TurnInputMessage,
)
from models import (
    TelegramAdmission as TelegramAdmissionRow,
)
from schemas.contracts import Channel, ErrorCode, Principal
from schemas.telegram import (
    TELEGRAM_NAMESPACE,
    TelegramAdmissionResult,
    TelegramDeliveryWork,
    TelegramDeliveryWorkItem,
    TelegramDeliveryWorkRequest,
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
from services.common import (
    authorized_conversation,
    db_clock,
    fingerprint,
    message_is_consumed,
)
from services.settling import freeze_admission_membership

RESPONSE_NAMESPACE = UUID("6c5798bb-94e0-4e79-8bc9-f1cc7d8ca0ad")
CANDIDATE_PAGE_SIZE = 100


class _IdentityEcho(TypedDict):
    owner_id: str
    integration_id: UUID
    agent_id: UUID
    provider_thread_id: str
    provider_event_id: str


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
            or admission.provider_thread_id != request.provider_thread_id
            or (request.bot_id is not None and str(request.bot_id) != admission.bot_id)):
        raise DomainError(ErrorCode.NOT_FOUND)
    if (admission.chat_id != admission.provider_thread_id
            or admission.sender_id != admission.chat_id
            or admission.provider_event_id != (
                f"b{admission.bot_id}c{admission.chat_id}m{admission.message_id}"
            )):
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
            retained_burst = (
                await session.scalar(select(Message.settling_burst_id).where(
                    Message.id == existing.canonical_message_id
                )) if existing.canonical_message_id is not None else None
            )
            result = TelegramAdmissionResult(
                route=cast(Literal["MANAGED", "LEGACY"], existing.route),
                admission_id=existing.id, replayed=True, bot_id=int(existing.bot_id),
                coordination_mode="SETTLING" if retained_burst is not None else "IMMEDIATE",
            )
        else:
            route: Literal["MANAGED", "LEGACY"] = (
                "MANAGED" if request.desired_route == "MANAGED"
                and request.integration_id in settings.telegram_cohort_integration_ids
                else "LEGACY"
            )
            conversation_id = canonical_message_id = None
            burst_id = None
            if route == "MANAGED":
                # Pin the provider account before taking the Conversation lock.
                # The unique key serializes competing first writers for the scope.
                await session.execute(insert(TelegramProviderAccount).values(
                    owner_id=request.owner_id,
                    integration_id=request.integration_id,
                    state="BOUND",
                    bot_id=str(request.bot_id),
                ).on_conflict_do_nothing(index_elements=[
                    TelegramProviderAccount.owner_id,
                    TelegramProviderAccount.integration_id,
                ]))
                account = await session.scalar(select(TelegramProviderAccount).where(
                    TelegramProviderAccount.owner_id == request.owner_id,
                    TelegramProviderAccount.integration_id == request.integration_id,
                ).with_for_update())
                if (account is None or account.state != "BOUND"
                        or account.bot_id != str(request.bot_id)):
                    raise DomainError(ErrorCode.NOT_FOUND)
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
                event_id, canonical_message_id = uuid4(), uuid4()
                conversation.last_message_sequence += 1
                conversation.input_revision += 1
                # Input freshness fences previous drafts even in immediate mode.
                conversation.settling_revision += 1
                policy = settings.settling_policy_for(Channel.TELEGRAM, request.integration_id)
                clock, burst_id, policy_version, quiet_ms, max_ms = (
                    await freeze_admission_membership(
                        session, conversation, policy, conversation.last_message_sequence
                    )
                )
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
                    sequence=conversation.last_message_sequence,
                    input_revision=conversation.input_revision,
                    settling_revision=conversation.settling_revision,
                    settling_policy_version=policy_version,
                    settling_quiet_window_ms=quiet_ms,
                    settling_max_wait_ms=max_ms,
                    settling_burst_id=burst_id,
                    actor="CUSTOMER",
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
            result = TelegramAdmissionResult(
                route=route, admission_id=row.id, replayed=False, bot_id=request.bot_id,
                coordination_mode="SETTLING" if burst_id is not None else "IMMEDIATE",
            )
    return result


async def admission_turn(
    session: AsyncSession, admission: TelegramAdmissionRow
) -> AgentTurn | None:
    if admission.route != "MANAGED" or admission.canonical_message_id is None:
        return None
    turn = await session.scalar(select(AgentTurn).join(
        TelegramImmediateTurn, TelegramImmediateTurn.turn_id == AgentTurn.id,
    ).where(TelegramImmediateTurn.message_id == admission.canonical_message_id))
    return turn


def _delivery_terminal(status: str) -> bool:
    return status in {
        "SUPERSEDED", "PROVIDER_ACCEPTED", "DELIVERED", "READ", "FAILED",
        "UNKNOWN", "PARTIAL", "INTERRUPTED",
    }


async def _telegram_account(
    session: AsyncSession, owner_id: str, integration_id: UUID
) -> TelegramProviderAccount | None:
    value = await session.scalar(select(TelegramProviderAccount).where(
        TelegramProviderAccount.owner_id == owner_id,
        TelegramProviderAccount.integration_id == integration_id,
    ))
    return value


async def result(database: Database, principal: Principal, request: TelegramIdentity,
                 settings: Settings) -> TelegramResult:
    authorize(principal, request, settings)
    async with database.session_factory() as session, session.begin():
        admission = await retained(session, request)
        if admission.route != "MANAGED" or admission.conversation_id is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        conversation = await authorized_conversation(
            session, principal, admission.conversation_id, lock=True
        )
        message = await session.get(Message, admission.canonical_message_id)
        if message is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        turn = await admission_turn(session, admission)
        delivery = None
        if turn is None or turn.status in {"PENDING", "CLAIMED"}:
            state: Literal["PENDING", "DRAFT_READY", "NO_RESPONSE", "FAILED", "UNKNOWN"] = (
                "PENDING"
            )
        elif turn.status == "FAILED":
            state = "UNKNOWN" if turn.failure_code == "EXECUTION_UNKNOWN" else "FAILED"
        else:
            state = "DRAFT_READY" if turn.outcome == "DRAFT_READY" else "NO_RESPONSE"
        if turn is not None:
            delivery = await session.scalar(select(DeliveryAttempt).where(
                DeliveryAttempt.source_turn_id == turn.id,
                DeliveryAttempt.conversation_id == conversation.id,
            ))
        delivery_state = cast(Literal[
            "NOT_STARTED", "GENERATED", "DISPATCH_REQUESTED", "SUPERSEDED",
            "PROVIDER_ACCEPTED", "DELIVERED", "READ", "FAILED", "UNKNOWN",
            "PARTIAL", "INTERRUPTED",
        ], delivery.status if delivery is not None else "NOT_STARTED")
        account = await _telegram_account(
            session, admission.owner_id, admission.integration_id
        )
        unresolved = False
        marked_recovery = bool(turn is not None and turn.status in {"PENDING", "CLAIMED"}
                               and turn.managed_dispatch_epoch is not None)
        if turn is not None and delivery is not None and delivery.status == "DISPATCH_REQUESTED":
            unresolved = bool(await session.scalar(select(TelegramResponseAttempt.id).where(
                TelegramResponseAttempt.delivery_id == delivery.id,
                TelegramResponseAttempt.conversation_id == conversation.id,
                TelegramResponseAttempt.send_authorized.is_(True),
                TelegramResponseAttempt.outcome.is_(None),
            )))
        disposition: Literal["WAIT", "TERMINAL", "AUTHORITY_FENCED", "ACCOUNT_CONFLICT"]
        if state in {"NO_RESPONSE", "FAILED", "UNKNOWN"}:
            disposition = "TERMINAL"
        elif _delivery_terminal(delivery_state):
            disposition = "TERMINAL"
        elif account is not None and account.state == "CONFLICT" and not (
            unresolved or marked_recovery
        ):
            disposition = "ACCOUNT_CONFLICT"
        elif (turn is not None and state == "DRAFT_READY"
              and conversation.authority_revision > turn.authority_revision and not unresolved):
            disposition = "AUTHORITY_FENCED"
        else:
            disposition = "WAIT"
        identity = request.model_dump()
        identity["bot_id"] = int(admission.bot_id)
        return TelegramResult(
            **identity,
            state=state,
            draft_text=turn.draft_text if state == "DRAFT_READY" and turn is not None else None,
            admission_id=admission.id,
            coordination_mode=("SETTLING" if message.settling_burst_id is not None
                               else "IMMEDIATE"),
            turn_id=turn.id if turn is not None else None,
            delivery_state=delivery_state,
            progress_disposition=disposition,
        )


def _reconciliation_key(response_attempt_id: UUID, status: str, reference: str = "") -> UUID:
    return uuid5(RESPONSE_NAMESPACE, f"{response_attempt_id}:{status}:{reference}")


def _record_reconciliation(session: AsyncSession, delivery: DeliveryAttempt, conversation_id: UUID,
                           status: str, *, provider_message_id: str | None = None,
                           error_code: str | None = None, key: UUID,
                           expected_revision: int, authority_revision: int | None = None,
                           expected_settling_revision: int | None = None,
                           result_settling_revision: int | None = None,
                           clock: datetime) -> None:
    delivery.status = status
    delivery.revision += 1
    delivery.updated_at = clock
    delivery.provider_message_id = provider_message_id or delivery.provider_message_id
    delivery.error_code = error_code
    if status in {"DISPATCH_REQUESTED", "SUPERSEDED"} and delivery.dispatch_requested_at is None:
        delivery.dispatch_requested_at = clock
    if status == "PROVIDER_ACCEPTED" and delivery.provider_accepted_at is None:
        delivery.provider_accepted_at = clock
    if expected_settling_revision is not None:
        delivery.expected_settling_revision = expected_settling_revision
        delivery.fenced_settling_revision = result_settling_revision
    payload = {"status": status, "provider_message_id": provider_message_id,
               "error_code": error_code, "expected_authority_revision": authority_revision,
               "expected_settling_revision": expected_settling_revision,
               "result_settling_revision": result_settling_revision}
    session.add(DeliveryReconciliation(
        id=uuid4(), conversation_id=conversation_id, delivery_attempt_id=delivery.id,
        reconciliation_key=key,
        fingerprint=fingerprint({"contract_version": 1, "delivery_attempt_id": str(delivery.id),
                                 **payload}),
        expected_revision=expected_revision, result_revision=delivery.revision,
        status=status, provider_message_id=provider_message_id, error_code=error_code,
        expected_authority_revision=authority_revision,
        expected_settling_revision=expected_settling_revision,
        result_settling_revision=result_settling_revision, accepted_at=clock,
    ))


async def _validated_turn_admissions(
    session: AsyncSession,
    conversation: Conversation,
    turn: AgentTurn,
    agent_id: UUID,
) -> list[TelegramAdmissionRow]:
    mapped = (await session.execute(select(TurnInputMessage, Message).join(
        Message, and_(Message.id == TurnInputMessage.message_id,
                      Message.conversation_id == TurnInputMessage.conversation_id),
    ).where(TurnInputMessage.turn_id == turn.id,
            TurnInputMessage.conversation_id == conversation.id)
        .order_by(TurnInputMessage.sequence, TurnInputMessage.ordinal))).all()
    if not mapped:
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    if (mapped[0][1].sequence != turn.input_first_sequence
            or mapped[-1][1].sequence != turn.input_last_sequence):
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    if any(row.ordinal != index or row.sequence != message.sequence
           for index, (row, message) in enumerate(mapped)):
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    map_ids = [message.id for _, message in mapped]
    channel_ids = list((await session.scalars(select(TelegramImmediateTurn.message_id).where(
        TelegramImmediateTurn.turn_id == turn.id,
        TelegramImmediateTurn.conversation_id == conversation.id,
    ))).all())
    if set(channel_ids) != set(map_ids) or len(channel_ids) != len(map_ids):
        raise DomainError(ErrorCode.VALIDATION_ERROR)
    admissions: list[TelegramAdmissionRow] = []
    for _, message in mapped:
        if (message.actor != "CUSTOMER" or message.direction != "INBOUND"
                or message.settling_burst_id != turn.settling_burst_id):
            raise DomainError(ErrorCode.VALIDATION_ERROR)
        admission = await session.scalar(select(TelegramAdmissionRow).where(
            TelegramAdmissionRow.canonical_message_id == message.id,
            TelegramAdmissionRow.conversation_id == conversation.id,
        ))
        if (admission is None or admission.route != "MANAGED"
                or admission.owner_id != conversation.owner_id
                or admission.integration_id != conversation.integration_id
                or admission.channel != "TELEGRAM"
                or admission.provider_namespace != TELEGRAM_NAMESPACE
                or admission.agent_id != agent_id
                or admission.provider_thread_id != conversation.provider_thread_id
                or admission.chat_id != conversation.provider_thread_id
                or admission.sender_id != admission.chat_id
                or admission.provider_event_id != (
                    f"b{admission.bot_id}c{admission.chat_id}m{admission.message_id}"
                )):
            raise DomainError(ErrorCode.VALIDATION_ERROR)
        admissions.append(admission)
    return admissions


def _response_echo(request: TelegramIdentity) -> _IdentityEcho:
    return {
        "owner_id": request.owner_id,
        "integration_id": request.integration_id,
        "agent_id": request.agent_id,
        "provider_thread_id": request.provider_thread_id,
        "provider_event_id": request.provider_event_id,
    }


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
        retained_turn = await admission_turn(session, admission)
        if retained_turn is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        turn = await session.scalar(select(AgentTurn).where(
            AgentTurn.id == retained_turn.id,
            AgentTurn.conversation_id == conversation.id,
        ).with_for_update().execution_options(populate_existing=True))
        if turn is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        if turn.status != "COMPLETED" or turn.draft_text is None:
            raise DomainError(ErrorCode.NO_DRAFT)
        attempt_probe = await session.scalar(select(TelegramResponseAttempt).where(
            TelegramResponseAttempt.id == request.response_attempt_id,
            TelegramResponseAttempt.conversation_id == conversation.id,
        ))
        delivery = None
        if attempt_probe is not None:
            delivery = await session.scalar(select(DeliveryAttempt).where(
                DeliveryAttempt.id == attempt_probe.delivery_id,
                DeliveryAttempt.conversation_id == conversation.id,
            ).with_for_update().execution_options(populate_existing=True))
        else:
            delivery = await session.scalar(select(DeliveryAttempt).where(
                DeliveryAttempt.source_turn_id == turn.id,
                DeliveryAttempt.conversation_id == conversation.id,
            ).with_for_update().execution_options(populate_existing=True))
        attempt = None
        if attempt_probe is not None:
            attempt = await session.scalar(select(TelegramResponseAttempt).where(
                TelegramResponseAttempt.id == request.response_attempt_id,
                TelegramResponseAttempt.conversation_id == conversation.id,
            ).with_for_update().execution_options(populate_existing=True))
            if (attempt is None or attempt.admission_id != admission.id
                    or delivery is None or delivery.source_turn_id != turn.id):
                raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
            if attempt.send_authorized:
                if delivery.status == "DISPATCH_REQUESTED" and attempt.outcome is None:
                    clock = await db_clock(session)
                    _record_reconciliation(
                        session, delivery, conversation.id, "UNKNOWN", error_code="INTERRUPTED",
                        key=_reconciliation_key(attempt.id, "UNKNOWN", "INTERRUPTED"),
                        expected_revision=delivery.revision, clock=clock,
                    )
                    await session.flush()
                return TelegramResponseBegun(
                    **_response_echo(request), response_attempt_id=attempt.id,
                    admission_id=admission.id, bot_id=int(admission.bot_id),
                    work_kind="RECOVER_ONLY", draft_text=turn.draft_text,
                    send=False, replayed=True,
                )
            return TelegramResponseBegun(
                **_response_echo(request), response_attempt_id=attempt.id,
                admission_id=admission.id, bot_id=int(admission.bot_id),
                work_kind="SEND_CANDIDATE", draft_text=turn.draft_text,
                send=False, replayed=True,
            )

        account = await _telegram_account(session, admission.owner_id, admission.integration_id)
        if (account is None or account.state != "BOUND"
                or account.bot_id != admission.bot_id):
            raise DomainError(ErrorCode.NOT_FOUND)
        members = await _validated_turn_admissions(
            session, conversation, turn, admission.agent_id
        )
        if (not members or members[0].id != admission.id
                or any(member.bot_id != admission.bot_id
                       for member in members)):
            raise DomainError(ErrorCode.NOT_FOUND)
        if (conversation.control_mode != "AGENT"
                or conversation.authority_revision != turn.authority_revision):
            raise DomainError(ErrorCode.AUTHORITY_REVISION_CONFLICT)
        clock = await db_clock(session)
        if delivery is None:
            delivery = DeliveryAttempt(
                id=uuid4(), conversation_id=conversation.id, delivery_key=turn.id,
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

        if delivery.status != "GENERATED":
            return TelegramResponseBegun(
                **_response_echo(request),
                response_attempt_id=request.response_attempt_id,
                admission_id=admission.id, bot_id=int(admission.bot_id),
                work_kind="SEND_CANDIDATE", draft_text=turn.draft_text,
                send=False, replayed=False,
            )

        settling = turn.settling_burst_id is not None
        stale = settling and conversation.settling_revision > turn.settling_revision
        expected_settling = turn.settling_revision if settling else None
        fenced_settling = conversation.settling_revision if settling else None
        if stale:
            _record_reconciliation(
                session, delivery, conversation.id, "SUPERSEDED", key=turn.id,
                expected_revision=delivery.revision,
                expected_settling_revision=expected_settling,
                result_settling_revision=fenced_settling, clock=clock,
            )
        else:
            _record_reconciliation(
                session, delivery, conversation.id, "DISPATCH_REQUESTED",
                key=turn.id, expected_revision=delivery.revision,
                authority_revision=conversation.authority_revision,
                expected_settling_revision=expected_settling,
                result_settling_revision=fenced_settling, clock=clock,
            )
        session.add(TelegramResponseAttempt(
            id=request.response_attempt_id, admission_id=admission.id,
            delivery_id=delivery.id, conversation_id=conversation.id,
            send_authorized=not stale, created_at=clock,
        ))
        await session.flush()
        return TelegramResponseBegun(
            **_response_echo(request),
            response_attempt_id=request.response_attempt_id,
            admission_id=admission.id, bot_id=int(admission.bot_id),
            work_kind="SEND_CANDIDATE", draft_text=turn.draft_text,
            send=not stale, replayed=False,
        )


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
        attempt_probe = await session.scalar(select(TelegramResponseAttempt).where(
            TelegramResponseAttempt.id == request.response_attempt_id,
            TelegramResponseAttempt.admission_id == admission.id,
            TelegramResponseAttempt.conversation_id == conversation.id,
        ))
        if attempt_probe is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        delivery_probe = await session.scalar(select(DeliveryAttempt).where(
            DeliveryAttempt.id == attempt_probe.delivery_id,
            DeliveryAttempt.conversation_id == conversation.id,
        ))
        if delivery_probe is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        turn = await session.scalar(select(AgentTurn).where(
            AgentTurn.id == delivery_probe.source_turn_id,
            AgentTurn.conversation_id == conversation.id,
        ).with_for_update().execution_options(populate_existing=True))
        if turn is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        delivery = await session.scalar(select(DeliveryAttempt).where(
            DeliveryAttempt.id == delivery_probe.id,
            DeliveryAttempt.conversation_id == conversation.id,
        ).with_for_update().execution_options(populate_existing=True))
        attempt = await session.scalar(select(TelegramResponseAttempt).where(
            TelegramResponseAttempt.id == request.response_attempt_id,
            TelegramResponseAttempt.admission_id == admission.id,
            TelegramResponseAttempt.conversation_id == conversation.id,
        ).with_for_update().execution_options(populate_existing=True))
        if (delivery is None or attempt is None or attempt.delivery_id != delivery.id
                or delivery.source_turn_id != turn.id):
            raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
        outcome = request.outcome
        if attempt.outcome == outcome and (
                delivery.provider_message_id == request.provider_message_id
                and delivery.error_code == request.error_code):
            return TelegramResponseCompleted(
                **_response_echo(request), response_attempt_id=attempt.id,
                admission_id=admission.id, bot_id=int(admission.bot_id),
                outcome=outcome, replayed=True,
            )
        if not attempt.send_authorized:
            raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
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
        return TelegramResponseCompleted(
            **_response_echo(request), response_attempt_id=attempt.id,
            admission_id=admission.id, bot_id=int(admission.bot_id),
            outcome=outcome, replayed=False,
        )


async def delivery_work(database: Database, principal: Principal,
                        request: TelegramDeliveryWorkRequest,
                        settings: Settings) -> TelegramDeliveryWork:
    """Return bounded, fully validated provider sends and exact recoveries."""
    if principal.role not in {"FULL", "TELEGRAM"}:
        raise DomainError(ErrorCode.NOT_FOUND)
    binding = next((item for item in settings.managed_ai_bindings
                    if item.owner_id == request.owner_id
                    and item.integration_id == request.integration_id
                    and item.agent_id == request.agent_id
                    and item.channel == Channel.TELEGRAM), None)
    if (binding is None or not any(
            grant.owner_id == request.owner_id
            and grant.integration_id == request.integration_id
            and grant.channel == Channel.TELEGRAM
            and grant.provider_namespace == TELEGRAM_NAMESPACE
            for grant in principal.grants)):
        raise DomainError(ErrorCode.NOT_FOUND)
    cursor: tuple[datetime, UUID] | None = None
    items: list[TelegramDeliveryWorkItem] = []
    while len(items) < 100:
        candidate_query = select(
            AgentTurn.id,
            AgentTurn.created_at,
        ).join(Conversation, Conversation.id == AgentTurn.conversation_id).where(
            AgentTurn.status == "COMPLETED",
            AgentTurn.outcome == "DRAFT_READY",
            AgentTurn.draft_text.is_not(None),
            Conversation.owner_id == request.owner_id,
            Conversation.integration_id == request.integration_id,
            Conversation.channel == Channel.TELEGRAM.value,
        )
        if cursor is not None:
            candidate_query = candidate_query.where(tuple_(
                AgentTurn.created_at, AgentTurn.id
            ) > tuple_(literal(cursor[0]), literal(cursor[1])))
        async with database.session_factory() as session, session.begin():
            page = (await session.execute(candidate_query.order_by(
                AgentTurn.created_at, AgentTurn.id
            ).limit(CANDIDATE_PAGE_SIZE))).all()
        if not page:
            break
        for turn_id, turn_created_at in page:
            try:
                async with database.session_factory() as session, session.begin():
                    conversation_id = await session.scalar(select(AgentTurn.conversation_id).where(
                        AgentTurn.id == turn_id,
                    ))
                    conversation = (await session.get(Conversation, conversation_id)
                                    if conversation_id is not None else None)
                    turn = (await session.get(AgentTurn, turn_id)
                            if conversation is not None else None)
                    if (conversation is None or turn is None
                            or conversation.owner_id != request.owner_id
                            or conversation.integration_id != request.integration_id
                            or conversation.channel != Channel.TELEGRAM.value
                            or turn.status != "COMPLETED" or turn.outcome != "DRAFT_READY"
                            or turn.draft_text is None):
                        continue
                    members = await _validated_turn_admissions(
                        session, conversation, turn, request.agent_id
                    )
                    representative = members[0]
                    if (representative.owner_id != request.owner_id
                            or representative.integration_id != request.integration_id
                            or representative.agent_id != request.agent_id):
                        raise DomainError(ErrorCode.VALIDATION_ERROR)
                    delivery = await session.scalar(select(DeliveryAttempt).where(
                        DeliveryAttempt.source_turn_id == turn.id,
                        DeliveryAttempt.conversation_id == conversation.id,
                    ))
                    account = await _telegram_account(
                        session, conversation.owner_id, conversation.integration_id
                    )
                    attempts: list[tuple[TelegramResponseAttempt, TelegramAdmissionRow]] = []
                    if delivery is not None:
                        attempt_rows = (await session.execute(
                            select(TelegramResponseAttempt, TelegramAdmissionRow).join(
                                TelegramAdmissionRow,
                                TelegramAdmissionRow.id == TelegramResponseAttempt.admission_id,
                            ).where(
                                TelegramResponseAttempt.delivery_id == delivery.id,
                                TelegramResponseAttempt.conversation_id == conversation.id,
                                TelegramResponseAttempt.send_authorized.is_(True),
                                TelegramResponseAttempt.outcome.is_(None),
                                TelegramAdmissionRow.route == "MANAGED",
                            ).order_by(TelegramResponseAttempt.created_at,
                                       TelegramResponseAttempt.id)
                        )).all()
                        attempts = [(row[0], row[1]) for row in attempt_rows]
                    member_ids = {member.id for member in members}
                    recovery = next((pair for pair in attempts
                        if delivery is not None
                        and delivery.status == "DISPATCH_REQUESTED"
                        and pair[1].id in member_ids
                        and pair[0].id == expected_response_attempt_id(pair[1].id)
                        and pair[1].owner_id == request.owner_id
                        and pair[1].integration_id == request.integration_id
                        and pair[1].agent_id == request.agent_id
                        and int(pair[1].bot_id) == request.bot_id), None)
                    if recovery is not None:
                        recovery_account_ok = (
                            account is not None
                            and (account.state == "CONFLICT"
                                 or (account.state == "BOUND"
                                     and account.bot_id == recovery[1].bot_id))
                        )
                        if not recovery_account_ok:
                            continue
                        item_admission = recovery[1]
                        work_kind: Literal["SEND_CANDIDATE", "RECOVER_ONLY"] = "RECOVER_ONLY"
                    else:
                        if (account is None or account.state != "BOUND"
                                or account.bot_id != str(request.bot_id)
                                or conversation.control_mode != "AGENT"
                                or conversation.authority_revision != turn.authority_revision
                                or any(member.bot_id != account.bot_id for member in members)):
                            continue
                        if attempts:
                            continue
                        if delivery is not None and not (
                            delivery.status == "GENERATED" and delivery.revision == 0
                            and delivery.dispatch_requested_at is None
                        ):
                            continue
                        item_admission = representative
                        work_kind = "SEND_CANDIDATE"
                    items.append(TelegramDeliveryWorkItem(
                        owner_id=item_admission.owner_id,
                        integration_id=item_admission.integration_id,
                        agent_id=item_admission.agent_id,
                        provider_thread_id=item_admission.provider_thread_id,
                        provider_event_id=item_admission.provider_event_id,
                        bot_id=int(item_admission.bot_id),
                        admission_id=item_admission.id,
                        turn_id=turn.id,
                        response_attempt_id=expected_response_attempt_id(item_admission.id),
                        work_kind=work_kind,
                        draft_text=turn.draft_text,
                    ))
                    if len(items) == 100:
                        break
            except DomainError as exc:
                if exc.code == ErrorCode.VALIDATION_ERROR:
                    # Transaction rollback precedes paging past malformed data.
                    continue
                raise
        if len(items) == 100 or len(page) < CANDIDATE_PAGE_SIZE:
            break
        last_turn_id, last_created_at = page[-1]
        cursor = (last_created_at, last_turn_id)
    return TelegramDeliveryWork(items=items)


async def select_immediate(database: Database, principal: Principal,
                           settings: Settings) -> UUID | None:
    bindings = [b for b in settings.managed_ai_bindings if b.channel == Channel.TELEGRAM]
    scopes = (or_(*(and_(TelegramAdmissionRow.owner_id == b.owner_id,
                         TelegramAdmissionRow.integration_id == b.integration_id,
                         TelegramAdmissionRow.agent_id == b.agent_id)
                    for b in bindings)) if bindings else false())
    active_telegram_turn = exists(select(TelegramImmediateTurn.turn_id).join(
        AgentTurn, AgentTurn.id == TelegramImmediateTurn.turn_id,
    ).where(AgentTurn.conversation_id == Conversation.id,
            AgentTurn.status.in_(("PENDING", "CLAIMED"))))
    candidate = select(Conversation.id).where(
            or_(exists(select(Message.id).join(
                    TelegramAdmissionRow,
                    TelegramAdmissionRow.canonical_message_id == Message.id,
                ).where(
                    Message.conversation_id == Conversation.id,
                    Message.actor == "CUSTOMER",
                    Message.direction == "INBOUND",
                    TelegramAdmissionRow.route == "MANAGED",
                    scopes,
                    ~message_is_consumed(),
                    ~exists(select(TelegramImmediateTurn.message_id).where(
                        TelegramImmediateTurn.message_id == Message.id)),
                )),
                active_telegram_turn)).distinct()
    last_candidate_id: UUID | None = None
    while True:
        ids = await _candidate_ids(database, candidate, principal, last_candidate_id)
        if not ids:
            return None
        for conversation_id in ids:
            try:
                turn_id = await _select_candidate(
                    database, principal, settings, conversation_id
                )
            except DomainError as exc:
                if exc.code == ErrorCode.VALIDATION_ERROR:
                    # Candidate transaction has rolled back; malformed input
                    # remains a FIFO barrier for its Conversation only.
                    continue
                raise
            if turn_id is not None:
                return turn_id
        # Scan position is local to this poll. It advances past skipped rows and
        # resets on the next poll; it is not persistent fairness or scheduling state.
        last_candidate_id = ids[-1]


async def _select_candidate(database: Database, principal: Principal,
                            settings: Settings, conversation_id: UUID,
                            ) -> UUID | None:
    async with database.session_factory() as session, session.begin():
        # Candidate membership is only a hint. Revalidate every decision after
        # acquiring the Conversation lock.
        conversation = await authorized_conversation(session, principal, conversation_id,
                                                     lock=True)
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
            # A persisted AI dispatch marker owns same-Turn recovery even after
            # control changed; unmarked Turns still require fresh AGENT control.
            if active.managed_dispatch_epoch is None:
                if conversation.control_mode != "AGENT":
                    return None
                admission = await session.scalar(
                    select(TelegramAdmissionRow).join(
                        TelegramImmediateTurn,
                        TelegramImmediateTurn.message_id
                        == TelegramAdmissionRow.canonical_message_id,
                    ).where(
                        TelegramImmediateTurn.turn_id == active.id,
                        TelegramImmediateTurn.conversation_id == conversation_id,
                        TelegramAdmissionRow.conversation_id == conversation_id,
                        TelegramAdmissionRow.route == "MANAGED",
                    )
                )
                binding = next((item for item in settings.managed_ai_bindings
                                if item.channel == Channel.TELEGRAM
                                and item.owner_id == conversation.owner_id
                                and item.integration_id == conversation.integration_id
                                and admission is not None
                                and item.agent_id == admission.agent_id), None)
                account = await _telegram_account(
                    session, conversation.owner_id, conversation.integration_id
                )
                if (admission is None
                        or admission.owner_id != conversation.owner_id
                        or admission.integration_id != conversation.integration_id
                        or binding is None
                        or account is None or account.state != "BOUND"
                        or account.bot_id != admission.bot_id
                        or admission.provider_thread_id != conversation.provider_thread_id
                        or admission.chat_id != conversation.provider_thread_id
                        or admission.sender_id != admission.chat_id
                        or admission.provider_event_id != (
                            f"b{admission.bot_id}c{admission.chat_id}m{admission.message_id}"
                        )
                        or (active.managed_contract_version == 2
                            and active.managed_projection_agent_id != admission.agent_id)):
                    return None
            if (active.status == "CLAIMED" and active.lease_expires_at is not None
                    and active.lease_expires_at > clock):
                return None
            return active.id
        if conversation.control_mode != "AGENT":
            return None
        # Rank the global pending inbound head before channel/mode checks so a
        # generic event or a settling member cannot be bypassed by a later
        # immediate Telegram message.
        message = await session.scalar(select(Message).where(
                Message.conversation_id == conversation_id,
                Message.actor == "CUSTOMER", Message.direction == "INBOUND",
                ~message_is_consumed())
            .order_by(Message.sequence).limit(1).with_for_update())
        if message is None or message.settling_burst_id is not None:
            return None
        if await session.scalar(select(TelegramImmediateTurn.turn_id).where(
                TelegramImmediateTurn.message_id == message.id)) is not None:
            return None
        # Policy-bound inputs belong to the durable settling coordinator. The
        # immediate fallback must never bypass its quiet/max-wait deadline.
        admission = await session.scalar(select(TelegramAdmissionRow).where(
            TelegramAdmissionRow.canonical_message_id == message.id,
            TelegramAdmissionRow.conversation_id == conversation_id,
        ))
        account = await _telegram_account(
            session, conversation.owner_id, conversation.integration_id
        )
        binding = next((item for item in settings.managed_ai_bindings
                        if item.channel == Channel.TELEGRAM
                        and item.owner_id == conversation.owner_id
                        and item.integration_id == conversation.integration_id), None)
        if (admission is None or admission.route != "MANAGED"
                or admission.owner_id != conversation.owner_id
                or admission.integration_id != conversation.integration_id
                or binding is None or admission.agent_id != binding.agent_id
                or admission.route != "MANAGED"
                or admission.provider_thread_id != conversation.provider_thread_id
                or admission.chat_id != conversation.provider_thread_id
                or admission.sender_id != admission.chat_id
                or admission.provider_event_id != (
                    f"b{admission.bot_id}c{admission.chat_id}m{admission.message_id}"
                )):
            raise DomainError(ErrorCode.VALIDATION_ERROR)
        if (account is None or account.state != "BOUND"
                or account.bot_id != admission.bot_id):
            return None
        agent_id = admission.agent_id
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
            settling_revision=conversation.settling_revision,
            settling_burst_id=None,
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
        session.add(TurnInputMessage(message_id=message.id,
                                     conversation_id=conversation_id, turn_id=turn.id,
                                     sequence=message.sequence, ordinal=0))
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

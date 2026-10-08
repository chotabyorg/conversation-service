"""Atomic sticky routing and honest retained HTTP reply evidence."""
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert
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
    WebsiteAdmission,
    WebsiteImmediateTurn,
    WebsiteResponseAttempt,
)
from schemas.contracts import (
    DeliveryCreate,
    DeliveryErrorCode,
    DeliveryReconcile,
    DeliveryStatus,
    ErrorCode,
    Principal,
)
from schemas.website import (
    WEBSITE_NAMESPACE,
    WebsiteAdmissionResult,
    WebsiteIdentity,
    WebsiteResponseBegin,
    WebsiteResponseBegun,
    WebsiteResponseComplete,
    WebsiteResponseCompleted,
    WebsiteResult,
)
from schemas.website import (
    WebsiteAdmission as AdmissionRequest,
)
from services.common import authorized_conversation, db_clock, fingerprint


def authorize(principal: Principal, request: WebsiteIdentity, settings: Settings) -> None:
    if principal.role not in {"FULL", "WEBSITE"}:
        raise DomainError(ErrorCode.NOT_FOUND)
    if not any(g.owner_id == request.owner_id and g.integration_id == request.integration_id
               and g.channel.value == "WEBSITE_WIDGET" and g.provider_namespace == WEBSITE_NAMESPACE
               for g in principal.grants):
        raise DomainError(ErrorCode.NOT_FOUND)
    if not any(b.owner_id == request.owner_id and b.integration_id == request.integration_id
               and b.agent_id == request.agent_id for b in settings.website_bindings):
        raise DomainError(ErrorCode.NOT_FOUND)


def identity_filter(request: WebsiteIdentity) -> tuple[ColumnElement[bool], ...]:
    return (WebsiteAdmission.owner_id == request.owner_id,
            WebsiteAdmission.integration_id == request.integration_id,
            WebsiteAdmission.channel == "WEBSITE_WIDGET",
            WebsiteAdmission.provider_namespace == WEBSITE_NAMESPACE,
            WebsiteAdmission.message_id == request.message_id)


async def retained(session: AsyncSession, request: WebsiteIdentity) -> WebsiteAdmission:
    admission = await session.scalar(select(WebsiteAdmission).where(*identity_filter(request)))
    if (admission is None or admission.agent_id != request.agent_id
            or admission.session_id != request.session_id):
        raise DomainError(ErrorCode.NOT_FOUND)
    return admission


async def admit(database: Database, principal: Principal, request: AdmissionRequest,
                settings: Settings) -> WebsiteAdmissionResult:
    authorize(principal, request, settings)
    digest = fingerprint({"contract_version": 1, "channel": "WEBSITE_WIDGET",
                          "provider_namespace": WEBSITE_NAMESPACE,
                          **request.model_dump(mode="json", exclude={"desired_route"})})
    async with database.session_factory() as session, session.begin():
        # Serialize old/new routing policies before creating any candidate parents.
        # The scoped transaction advisory lock and retained unique constraint both
        # protect the decision; managed parents and decision share this transaction.
        key = fingerprint({"owner": request.owner_id, "integration": str(request.integration_id),
                           "namespace": WEBSITE_NAMESPACE, "message": str(request.message_id)})
        from sqlalchemy import text
        await session.execute(text("SELECT pg_advisory_xact_lock(:key)"),
                              {"key": int(key[:16], 16) - (1 << 63)})
        existing = await session.scalar(select(WebsiteAdmission).where(*identity_filter(request)))
        if existing is not None:
            if existing.fingerprint != digest:
                raise DomainError(ErrorCode.EVENT_CONFLICT)
            result = WebsiteAdmissionResult(
                route="MANAGED" if existing.route == "MANAGED" else "LEGACY",
                                            admission_id=existing.id,
                                            replayed=True)
        else:
            conversation_id = canonical_id = None
            if request.desired_route == "MANAGED":
                await session.execute(insert(Conversation).values(
                    id=uuid4(), owner_id=request.owner_id, integration_id=request.integration_id,
                    channel="WEBSITE_WIDGET", provider_thread_id=str(request.session_id),
                ).on_conflict_do_nothing(constraint="uq_conversation_identity"))
                conversation = await session.scalar(select(Conversation).where(
                    Conversation.owner_id == request.owner_id,
                    Conversation.integration_id == request.integration_id,
                    Conversation.channel == "WEBSITE_WIDGET",
                    Conversation.provider_thread_id == str(request.session_id),
                ).with_for_update().execution_options(populate_existing=True))
                if conversation is None:
                    raise DomainError(ErrorCode.NOT_FOUND)
                clock = await db_clock(session)
                event_id, canonical_id = uuid4(), uuid4()
                conversation.last_message_sequence += 1
                conversation.input_revision += 1
                conversation.updated_at = clock
                event = InboundEvent(
                    id=event_id, conversation_id=conversation.id, owner_id=request.owner_id,
                    integration_id=request.integration_id, channel="WEBSITE_WIDGET",
                    provider_namespace=WEBSITE_NAMESPACE, provider_event_id=str(request.message_id),
                    contract_version=1, fingerprint=digest, accepted_at=clock,
                )
                session.add(event)
                await session.flush()
                session.add(Message(id=canonical_id, conversation_id=conversation.id,
                                    sequence=conversation.last_message_sequence, actor="CUSTOMER",
                                    direction="INBOUND", content_type="TEXT", text=request.text,
                                    source_inbound_event_id=event_id, created_at=clock))
                await session.flush()
                conversation_id = conversation.id
            admission = WebsiteAdmission(
                id=uuid4(), owner_id=request.owner_id, integration_id=request.integration_id,
                agent_id=request.agent_id, channel="WEBSITE_WIDGET",
                provider_namespace=WEBSITE_NAMESPACE, message_id=request.message_id,
                session_id=request.session_id, fingerprint=digest, route=request.desired_route,
                conversation_id=conversation_id, canonical_message_id=canonical_id,
            )
            session.add(admission)
            await session.flush()
            result = WebsiteAdmissionResult(route=request.desired_route, admission_id=admission.id,
                                            replayed=False)
    # Return only after session.begin positively commits. Uncertainty fails closed.
    return result


async def admission_turn(session: AsyncSession, admission: WebsiteAdmission) -> AgentTurn | None:
    if admission.route != "MANAGED":
        raise DomainError(ErrorCode.NOT_FOUND)
    turn: AgentTurn | None = await session.scalar(select(AgentTurn).join(
        WebsiteImmediateTurn, WebsiteImmediateTurn.turn_id == AgentTurn.id,
    ).where(WebsiteImmediateTurn.message_id == admission.canonical_message_id))
    return turn


async def result(database: Database, principal: Principal, request: WebsiteIdentity,
                 settings: Settings) -> WebsiteResult:
    authorize(principal, request, settings)
    async with database.session_factory() as session, session.begin():
        admission = await retained(session, request)
        turn = await admission_turn(session, admission)
        if turn is None or turn.status in {"PENDING", "CLAIMED"}:
            return WebsiteResult(state="PENDING")
        if turn.status == "FAILED":
            return WebsiteResult(state="UNKNOWN" if turn.failure_code == "EXECUTION_UNKNOWN"
                                 else "FAILED")
        return WebsiteResult(
            state="DRAFT_READY" if turn.outcome == "DRAFT_READY" else "NO_RESPONSE",
                             draft_text=turn.draft_text)


async def begin_response(database: Database, principal: Principal, request: WebsiteResponseBegin,
                         settings: Settings) -> WebsiteResponseBegun:
    authorize(principal, request, settings)
    async with database.session_factory() as session, session.begin():
        admission = await retained(session, request)
        if admission.conversation_id is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        conversation = await authorized_conversation(session, principal,
                                                      admission.conversation_id, lock=True)
        turn = await admission_turn(session, admission)
        if turn is None or turn.status != "COMPLETED" or turn.draft_text is None:
            raise DomainError(ErrorCode.NO_DRAFT)
        if conversation.control_mode != "AGENT":
            raise DomainError(ErrorCode.CONTROL_MODE_CONFLICT)
        if conversation.authority_revision != turn.authority_revision:
            raise DomainError(ErrorCode.AUTHORITY_REVISION_CONFLICT)
        attempt = await session.get(WebsiteResponseAttempt, request.response_attempt_id)
        if attempt is not None:
            if attempt.admission_id != admission.id:
                raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
            return WebsiteResponseBegun(response_attempt_id=attempt.id,
                                        draft_text=turn.draft_text, replayed=True)
        clock = await db_clock(session)
        delivery = await session.scalar(select(DeliveryAttempt).where(
            DeliveryAttempt.source_turn_id == turn.id))
        if delivery is None:
            creation = DeliveryCreate(delivery_key=turn.id, source_turn_id=turn.id,
                                      expected_authority_revision=conversation.authority_revision)
            delivery = DeliveryAttempt(
                id=uuid4(), conversation_id=conversation.id, delivery_key=turn.id,
                source_turn_id=turn.id, creation_fingerprint=fingerprint({
                    "contract_version": 1, "conversation_id": str(conversation.id),
                    **creation.model_dump(mode="json")}),
                authority_revision=conversation.authority_revision,
                status="GENERATED", revision=0, created_at=clock, updated_at=clock,
            )
            session.add(delivery)
            await session.flush()
            reconcile = DeliveryReconcile(reconciliation_key=turn.id, expected_revision=0,
                                           status=DeliveryStatus.DISPATCH_REQUESTED,
                                           expected_authority_revision=turn.authority_revision)
            delivery.status, delivery.revision = "DISPATCH_REQUESTED", 1
            delivery.dispatch_requested_at = clock
            session.add(DeliveryReconciliation(
                id=uuid4(), conversation_id=conversation.id, delivery_attempt_id=delivery.id,
                reconciliation_key=turn.id, fingerprint=fingerprint({
                    "contract_version": 1, "delivery_attempt_id": str(delivery.id),
                    **reconcile.model_dump(mode="json")}), expected_revision=0, result_revision=1,
                status="DISPATCH_REQUESTED", expected_authority_revision=turn.authority_revision,
                accepted_at=clock))
            await session.flush()
        if delivery.authority_revision != conversation.authority_revision:
            raise DomainError(ErrorCode.AUTHORITY_REVISION_CONFLICT)
        session.add(WebsiteResponseAttempt(id=request.response_attempt_id,
                                           admission_id=admission.id, delivery_id=delivery.id,
                                           conversation_id=conversation.id, created_at=clock))
        await session.flush()
        result = WebsiteResponseBegun(response_attempt_id=request.response_attempt_id,
                                      draft_text=turn.draft_text, replayed=False)
    return result


async def complete_response(database: Database, principal: Principal,
                            request: WebsiteResponseComplete,
                            settings: Settings) -> WebsiteResponseCompleted:
    authorize(principal, request, settings)
    async with database.session_factory() as session, session.begin():
        admission = await retained(session, request)
        if admission.conversation_id is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        await authorized_conversation(session, principal, admission.conversation_id, lock=True)
        attempt = await session.get(WebsiteResponseAttempt, request.response_attempt_id)
        if attempt is None or attempt.admission_id != admission.id:
            raise DomainError(ErrorCode.NOT_FOUND)
        replayed = attempt.outcome is not None
        if replayed and attempt.outcome != request.outcome:
            raise DomainError(ErrorCode.DELIVERY_RECONCILIATION_CONFLICT)
        if not replayed:
            clock = await db_clock(session)
            attempt.outcome = request.outcome
            attempt.finished_at = clock
            delivery = await session.get(DeliveryAttempt, attempt.delivery_id)
            if delivery is None:
                raise DomainError(ErrorCode.NOT_FOUND)
            if delivery.status == "DISPATCH_REQUESTED":
                delivery.status = "UNKNOWN"
                delivery.error_code = "TRANSPORT_ERROR"
                delivery.revision += 1
                delivery.updated_at = clock
                reconcile = DeliveryReconcile(reconciliation_key=request.response_attempt_id,
                                               expected_revision=1, status=DeliveryStatus.UNKNOWN,
                                               error_code=DeliveryErrorCode.TRANSPORT_ERROR)
                session.add(DeliveryReconciliation(
                    id=uuid4(), conversation_id=admission.conversation_id,
                    delivery_attempt_id=delivery.id, reconciliation_key=request.response_attempt_id,
                    fingerprint=fingerprint({"contract_version": 1,
                        "delivery_attempt_id": str(delivery.id),
                        **reconcile.model_dump(mode="json")}), expected_revision=1,
                    result_revision=2, status="UNKNOWN", error_code="TRANSPORT_ERROR",
                    accepted_at=clock))
            await session.flush()
        result = WebsiteResponseCompleted(response_attempt_id=attempt.id,
                                          outcome=request.outcome, replayed=replayed)
    return result

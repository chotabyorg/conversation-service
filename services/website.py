"""Atomic sticky routing and honest retained HTTP reply evidence."""
from typing import Literal
from uuid import NAMESPACE_URL, UUID, uuid4, uuid5

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
    Channel,
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
from services import catch_up
from services.common import authorized_conversation, db_clock, fingerprint
from services.settling import freeze_admission_membership


def authorize(
    principal: Principal, request: WebsiteIdentity, settings: Settings, *,
    require_current_binding: bool = True,
) -> None:
    if principal.role not in {"FULL", "WEBSITE"}:
        raise DomainError(ErrorCode.NOT_FOUND)
    if not any(g.owner_id == request.owner_id and g.integration_id == request.integration_id
               and g.channel.value == "WEBSITE_WIDGET" and g.provider_namespace == WEBSITE_NAMESPACE
               for g in principal.grants):
        raise DomainError(ErrorCode.NOT_FOUND)
    if require_current_binding and not any(
        b.owner_id == request.owner_id and b.integration_id == request.integration_id
        and b.agent_id == request.agent_id for b in settings.website_bindings
    ):
        raise DomainError(ErrorCode.NOT_FOUND)


def expected_v2_response_attempt_id(admission_id: UUID) -> UUID:
    return uuid5(NAMESPACE_URL, f"main-chota.website-widget-response.v2:{admission_id}")


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
                event_id, canonical_id = uuid4(), uuid4()
                conversation.last_message_sequence += 1
                conversation.input_revision += 1
                # Input freshness fences previous drafts even in immediate mode.
                conversation.settling_revision += 1
                policy = settings.settling_policy_for(
                    Channel.WEBSITE_WIDGET, request.integration_id
                )
                clock, burst_id, policy_version, quiet_ms, max_ms = (
                    await freeze_admission_membership(
                        session, conversation, policy, conversation.last_message_sequence,
                        separate_authority_epoch=catch_up.scope_enabled(
                            settings, request.owner_id, request.integration_id,
                            "WEBSITE_WIDGET", request.agent_id,
                        ),
                    )
                )
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
                                    sequence=conversation.last_message_sequence,
                                    input_revision=conversation.input_revision,
                                    settling_revision=conversation.settling_revision,
                                    settling_policy_version=policy_version,
                                    settling_quiet_window_ms=quiet_ms,
                                    settling_max_wait_ms=max_ms,
                                    settling_burst_id=burst_id,
                                    actor="CUSTOMER",
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
        retained_turn = await admission_turn(session, admission)
        if retained_turn is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        turn = await session.scalar(select(AgentTurn).where(
            AgentTurn.id == retained_turn.id,
            AgentTurn.conversation_id == conversation.id,
        ).with_for_update().execution_options(populate_existing=True))
        if turn is None or turn.status != "COMPLETED" or turn.draft_text is None:
            raise DomainError(ErrorCode.NO_DRAFT)
        attempt_probe = await session.scalar(select(WebsiteResponseAttempt).where(
            WebsiteResponseAttempt.id == request.response_attempt_id,
        ))
        if attempt_probe is not None and (
            attempt_probe.admission_id != admission.id
            or attempt_probe.conversation_id != conversation.id
        ):
            raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
        if (attempt_probe is None
                and request.response_attempt_id != expected_v2_response_attempt_id(admission.id)):
            raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)

        delivery_probe = None
        if attempt_probe is not None:
            delivery_probe = await session.scalar(select(DeliveryAttempt).where(
                DeliveryAttempt.id == attempt_probe.delivery_id,
                DeliveryAttempt.conversation_id == conversation.id,
            ))
        else:
            delivery_probe = await session.scalar(select(DeliveryAttempt).where(
                DeliveryAttempt.source_turn_id == turn.id,
                DeliveryAttempt.conversation_id == conversation.id,
            ))
        delivery = None
        if delivery_probe is not None:
            delivery = await session.scalar(select(DeliveryAttempt).where(
                DeliveryAttempt.id == delivery_probe.id,
                DeliveryAttempt.conversation_id == conversation.id,
            ).with_for_update().execution_options(populate_existing=True))
        attempt = None
        if attempt_probe is not None:
            attempt = await session.scalar(select(WebsiteResponseAttempt).where(
                WebsiteResponseAttempt.id == request.response_attempt_id,
                WebsiteResponseAttempt.admission_id == admission.id,
                WebsiteResponseAttempt.conversation_id == conversation.id,
            ).with_for_update().execution_options(populate_existing=True))
            if attempt is None or delivery is None or attempt.delivery_id != delivery.id:
                raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
            if delivery.source_turn_id != turn.id:
                raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)

        def settling_revision_is_current() -> bool:
            if turn.settling_burst_id is None:
                return True
            return conversation.settling_revision == turn.settling_revision

        def persisted_settling_fence_is_current() -> bool:
            if turn.settling_burst_id is None:
                return True
            return (delivery is not None
                    and delivery.expected_settling_revision == turn.settling_revision
                    and delivery.fenced_settling_revision == turn.settling_revision)

        authority_current = (
            conversation.control_mode == "AGENT"
            and conversation.authority_revision == turn.authority_revision
        )
        if delivery is not None:
            source = await session.scalar(select(WebsiteResponseAttempt).where(
                WebsiteResponseAttempt.admission_id == admission.id,
                WebsiteResponseAttempt.delivery_id == delivery.id,
                WebsiteResponseAttempt.conversation_id == conversation.id,
                WebsiteResponseAttempt.send_authorized.is_(True),
                WebsiteResponseAttempt.outcome == "WRITE_COMPLETED",
            ).order_by(WebsiteResponseAttempt.created_at, WebsiteResponseAttempt.id).limit(1))
            # This returns the exact committed result without another write.
            # Later input cannot revoke that result, but authority, the frozen
            # admission proof and the original persisted fence still apply.
            if (source is not None and authority_current
                    and persisted_settling_fence_is_current()
                    and await catch_up.validate(session, conversation, turn)):
                return WebsiteResponseBegun(
                    action="REPLAY_COMPLETED",
                    response_attempt_id=request.response_attempt_id,
                    replay_source_attempt_id=source.id,
                    draft_text=turn.draft_text,
                    send=False,
                    replayed=True,
                    denial_reason=None,
                )

        settling_stale = (
            turn.settling_burst_id is not None
            and conversation.settling_revision > turn.settling_revision
        )
        if turn.catch_up_version:
            settling_stale = settling_stale or not await catch_up.communication_current(
                session, conversation, turn,
            )
        permission_current = (
            authority_current and settling_revision_is_current() and not settling_stale
        )

        if (attempt is not None and attempt.send_authorized
                and attempt.outcome is None and delivery is not None
                and delivery.status == "DISPATCH_REQUESTED"):
            clock = await db_clock(session)
            expected_revision = delivery.revision
            delivery.status = "UNKNOWN"
            delivery.error_code = "INTERRUPTED"
            delivery.revision += 1
            delivery.updated_at = clock
            evidence = DeliveryReconcile(
                reconciliation_key=request.response_attempt_id,
                expected_revision=expected_revision,
                status=DeliveryStatus.UNKNOWN,
                error_code=DeliveryErrorCode.INTERRUPTED,
            )
            session.add(DeliveryReconciliation(
                id=uuid4(), conversation_id=conversation.id, delivery_attempt_id=delivery.id,
                reconciliation_key=request.response_attempt_id,
                fingerprint=fingerprint({"contract_version": 1,
                    "delivery_attempt_id": str(delivery.id),
                    **evidence.model_dump(mode="json")}),
                expected_revision=expected_revision, result_revision=delivery.revision,
                status="UNKNOWN", error_code="INTERRUPTED", accepted_at=clock,
            ))
            await session.flush()

        # Existing response IDs, including historical v1 UUIDs and persisted
        # DENY rows, never authorize another browser write.
        if attempt is not None:
            prior_denial_reason: Literal["SUPERSEDED", "DELIVERY_NOT_RETRYABLE"] = (
                "SUPERSEDED" if not permission_current else "DELIVERY_NOT_RETRYABLE"
            )
            return WebsiteResponseBegun(
                action="DENY", response_attempt_id=request.response_attempt_id,
                draft_text=turn.draft_text, send=False, replayed=True,
                denial_reason=prior_denial_reason,
            )

        clock = await db_clock(session)
        if delivery is None:
            creation = DeliveryCreate(
                delivery_key=turn.id,
                source_turn_id=turn.id,
                expected_authority_revision=turn.authority_revision,
            )
            delivery = DeliveryAttempt(
                id=uuid4(), conversation_id=conversation.id, delivery_key=turn.id,
                source_turn_id=turn.id, creation_fingerprint=fingerprint({
                    "contract_version": 1, "conversation_id": str(conversation.id),
                    **creation.model_dump(mode="json")}),
                authority_revision=turn.authority_revision,
                expected_settling_revision=(turn.settling_revision
                    if turn.settling_burst_id is not None else None),
                status="GENERATED", revision=0, created_at=clock, updated_at=clock,
            )
            session.add(delivery)
            await session.flush()
        prior_attempt = await session.scalar(select(WebsiteResponseAttempt.id).where(
            WebsiteResponseAttempt.delivery_id == delivery.id,
            WebsiteResponseAttempt.conversation_id == conversation.id,
        ).limit(1))
        truly_undispatched = (
            prior_attempt is None and delivery.status == "GENERATED"
            and delivery.revision == 0 and delivery.dispatch_requested_at is None
            and delivery.authority_revision == turn.authority_revision
        )
        send = permission_current and truly_undispatched
        denial_reason: Literal["SUPERSEDED", "DELIVERY_NOT_RETRYABLE"] | None = None
        if not send:
            denial_reason = (
                "SUPERSEDED" if not permission_current else "DELIVERY_NOT_RETRYABLE"
            )
        if send:
            expected_settling = (turn.settling_revision
                                 if turn.settling_burst_id is not None else None)
            delivery.expected_settling_revision = expected_settling
            delivery.fenced_settling_revision = expected_settling
            expected_revision = delivery.revision
            delivery.status = "DISPATCH_REQUESTED"
            delivery.revision += 1
            delivery.dispatch_requested_at = clock
            delivery.updated_at = clock
            evidence = DeliveryReconcile(
                reconciliation_key=turn.id,
                expected_revision=expected_revision,
                status=DeliveryStatus.DISPATCH_REQUESTED,
                expected_authority_revision=turn.authority_revision,
                expected_settling_revision=expected_settling,
                result_settling_revision=expected_settling,
            )
            session.add(DeliveryReconciliation(
                id=uuid4(), conversation_id=conversation.id, delivery_attempt_id=delivery.id,
                reconciliation_key=turn.id, fingerprint=fingerprint({
                    "contract_version": 1, "delivery_attempt_id": str(delivery.id),
                    **evidence.model_dump(mode="json")}),
                expected_revision=expected_revision, result_revision=delivery.revision,
                status="DISPATCH_REQUESTED",
                expected_authority_revision=turn.authority_revision,
                expected_settling_revision=expected_settling,
                result_settling_revision=expected_settling, accepted_at=clock,
            ))
        elif settling_stale and truly_undispatched:
            # A fresh v2 request on an undispatched SETTLING draft must retain
            # the freshness decision as a terminal communication fence. This
            # does not apply to IMMEDIATE Turns or rewrite prior write evidence.
            expected_revision = delivery.revision
            expected_settling = turn.settling_revision
            result_settling = conversation.settling_revision
            evidence = DeliveryReconcile(
                reconciliation_key=turn.id,
                expected_revision=expected_revision,
                status=DeliveryStatus.SUPERSEDED,
                expected_settling_revision=expected_settling,
                result_settling_revision=result_settling,
            )
            delivery.status = "SUPERSEDED"
            delivery.revision += 1
            delivery.updated_at = clock
            delivery.dispatch_requested_at = clock
            delivery.expected_settling_revision = expected_settling
            delivery.fenced_settling_revision = result_settling
            session.add(DeliveryReconciliation(
                id=uuid4(), conversation_id=conversation.id,
                delivery_attempt_id=delivery.id,
                reconciliation_key=turn.id,
                fingerprint=fingerprint({
                    "contract_version": 1,
                    "delivery_attempt_id": str(delivery.id),
                    **evidence.model_dump(mode="json"),
                }),
                expected_revision=expected_revision,
                result_revision=delivery.revision,
                status="SUPERSEDED",
                expected_settling_revision=expected_settling,
                result_settling_revision=result_settling,
                accepted_at=clock,
            ))
        session.add(WebsiteResponseAttempt(
            id=request.response_attempt_id,
            admission_id=admission.id,
            delivery_id=delivery.id,
            conversation_id=conversation.id,
            send_authorized=send,
            created_at=clock,
        ))
        await session.flush()
        return WebsiteResponseBegun(
            action="SEND" if send else "DENY",
            response_attempt_id=request.response_attempt_id,
            draft_text=turn.draft_text,
            send=send,
            replayed=False,
            denial_reason=denial_reason,
        )


async def complete_response(database: Database, principal: Principal,
                            request: WebsiteResponseComplete,
                            settings: Settings) -> WebsiteResponseCompleted:
    # Completion records provider evidence for the exact retained authorized
    # attempt even after current integration binding has changed.
    authorize(principal, request, settings, require_current_binding=False)
    async with database.session_factory() as session, session.begin():
        admission = await retained(session, request)
        if admission.conversation_id is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        conversation = await authorized_conversation(
            session, principal, admission.conversation_id, lock=True)
        retained_turn = await admission_turn(session, admission)
        turn = None
        if retained_turn is not None:
            turn = await session.scalar(select(AgentTurn).where(
                AgentTurn.id == retained_turn.id,
                AgentTurn.conversation_id == conversation.id,
            ).with_for_update().execution_options(populate_existing=True))
        attempt_probe = await session.scalar(select(WebsiteResponseAttempt).where(
            WebsiteResponseAttempt.id == request.response_attempt_id,
        ))
        if (attempt_probe is None or attempt_probe.admission_id != admission.id
                or attempt_probe.conversation_id != conversation.id):
            raise DomainError(ErrorCode.NOT_FOUND)
        delivery = await session.scalar(select(DeliveryAttempt).where(
            DeliveryAttempt.id == attempt_probe.delivery_id,
            DeliveryAttempt.conversation_id == conversation.id,
        ).with_for_update().execution_options(populate_existing=True))
        attempt = await session.scalar(select(WebsiteResponseAttempt).where(
            WebsiteResponseAttempt.id == request.response_attempt_id,
            WebsiteResponseAttempt.admission_id == admission.id,
            WebsiteResponseAttempt.conversation_id == conversation.id,
        ).with_for_update().execution_options(populate_existing=True))
        if attempt is None or delivery is None or attempt.delivery_id != delivery.id:
            raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
        if turn is None or delivery.source_turn_id != turn.id:
            raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
        if not attempt.send_authorized:
            raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
        previous_outcome = attempt.outcome
        replayed = previous_outcome is not None
        if replayed and previous_outcome == request.outcome:
            return WebsiteResponseCompleted(response_attempt_id=attempt.id,
                                             outcome=request.outcome, replayed=True)
        # UNKNOWN is an unresolved authorized write. Keep its one response
        # attempt eligible for late client evidence; definitive results remain
        # immutable and a denied attempt can never borrow another attempt's send.
        late_evidence = (previous_outcome == "UNKNOWN"
                         and request.outcome in {"WRITE_COMPLETED", "WRITE_FAILED"})
        if replayed and not late_evidence:
            raise DomainError(ErrorCode.DELIVERY_RECONCILIATION_CONFLICT)
        if late_evidence:
            replayed = False
        if not replayed:
            # Permission is bound to this exact transport attempt. A separate
            # attempt can share the Turn's delivery row but must not borrow its
            # dispatch authorization. Its evidence remains independent of the
            # aggregate delivery state, which may reflect another attempt.
            clock = await db_clock(session)
            attempt.outcome = request.outcome
            attempt.finished_at = clock
            if request.outcome == "WRITE_COMPLETED":
                turn = await session.scalar(select(AgentTurn).where(
                    AgentTurn.id == delivery.source_turn_id,
                    AgentTurn.conversation_id == conversation.id))
                if turn is None or turn.status != "COMPLETED" or turn.draft_text is None:
                    raise DomainError(ErrorCode.NO_DRAFT)
                existing = await session.scalar(select(Message.id).where(
                    Message.source_agent_turn_id == turn.id,
                    Message.conversation_id == conversation.id))
                if existing is None:
                    conversation.last_message_sequence += 1
                    conversation.updated_at = clock
                    session.add(Message(
                        id=uuid4(), conversation_id=conversation.id,
                        sequence=conversation.last_message_sequence, actor="AGENT",
                        direction="OUTBOUND", content_type="TEXT", text=turn.draft_text,
                        source_agent_turn_id=turn.id, created_at=clock))
            if delivery.status == "DISPATCH_REQUESTED":
                expected_revision = delivery.revision
                delivery.status = "UNKNOWN"
                delivery.error_code = "TRANSPORT_ERROR"
                delivery.revision += 1
                delivery.updated_at = clock
                reconcile = DeliveryReconcile(reconciliation_key=request.response_attempt_id,
                                               expected_revision=expected_revision,
                                               status=DeliveryStatus.UNKNOWN,
                                               error_code=DeliveryErrorCode.TRANSPORT_ERROR)
                session.add(DeliveryReconciliation(
                    id=uuid4(), conversation_id=admission.conversation_id,
                    delivery_attempt_id=delivery.id, reconciliation_key=request.response_attempt_id,
                    fingerprint=fingerprint({"contract_version": 1,
                        "delivery_attempt_id": str(delivery.id),
                        **reconcile.model_dump(mode="json")}),
                    expected_revision=expected_revision,
                    result_revision=delivery.revision,
                    status="UNKNOWN", error_code="TRANSPORT_ERROR",
                    accepted_at=clock))
            await session.flush()
        result = WebsiteResponseCompleted(response_attempt_id=attempt.id,
                                          outcome=request.outcome, replayed=replayed)
    return result

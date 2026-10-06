"""Delivery evidence only: no provider send, outbound Message or implicit retry."""

from uuid import UUID, uuid4

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from core.errors import DomainError
from db.database import Database
from models import AgentTurn, Conversation, DeliveryAttempt, DeliveryReconciliation
from schemas.contracts import (
    DeliveryAttemptRead,
    DeliveryCreate,
    DeliveryReconcile,
    DeliveryReconcileResult,
    DeliveryResult,
    ErrorCode,
    Principal,
)
from services.common import (
    authorized_conversation,
    db_clock,
    fingerprint,
    locked_delivery,
    scope_filter,
    unique_violation,
)

TRANSITIONS: dict[str, frozenset[str]] = {
    "GENERATED": frozenset({"DISPATCH_REQUESTED"}),
    "DISPATCH_REQUESTED": frozenset(
        {
            "PROVIDER_ACCEPTED",
            "DELIVERED",
            "READ",
            "FAILED",
            "UNKNOWN",
            "PARTIAL",
            "INTERRUPTED",
        }
    ),
    "PROVIDER_ACCEPTED": frozenset(
        {"DELIVERED", "READ", "FAILED", "UNKNOWN", "PARTIAL", "INTERRUPTED"}
    ),
    "UNKNOWN": frozenset(
        {"PROVIDER_ACCEPTED", "DELIVERED", "READ", "FAILED", "PARTIAL", "INTERRUPTED"}
    ),
    "PARTIAL": frozenset({"DELIVERED", "READ", "FAILED", "UNKNOWN", "INTERRUPTED"}),
    "INTERRUPTED": frozenset({"DELIVERED", "READ", "FAILED", "UNKNOWN", "PARTIAL"}),
    "DELIVERED": frozenset({"READ"}),
    "READ": frozenset(),
    "FAILED": frozenset(),
}


async def create_delivery(
    database: Database, principal: Principal, conversation_id: UUID, request: DeliveryCreate
) -> DeliveryResult:
    digest = fingerprint(
        {
            "contract_version": 1,
            "conversation_id": str(conversation_id),
            **request.model_dump(mode="json"),
        }
    )
    try:
        async with database.session_factory() as session, session.begin():
            conversation = await authorized_conversation(
                session, principal, conversation_id, lock=True
            )
            existing = await session.scalar(
                select(DeliveryAttempt).where(
                    DeliveryAttempt.conversation_id == conversation.id,
                    DeliveryAttempt.delivery_key == request.delivery_key,
                )
            )
            if existing is not None:
                if existing.creation_fingerprint != digest:
                    raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
                result = DeliveryResult(
                    attempt=DeliveryAttemptRead.model_validate(existing), replayed=True
                )
            else:
                source = await session.scalar(
                    select(AgentTurn)
                    .where(
                        AgentTurn.id == request.source_turn_id,
                        AgentTurn.conversation_id == conversation.id,
                    )
                    .with_for_update()
                    .execution_options(populate_existing=True)
                )
                if source is None:
                    raise DomainError(ErrorCode.NOT_FOUND)
                existing_intent = await session.scalar(
                    select(DeliveryAttempt.id).where(
                        DeliveryAttempt.source_turn_id == source.id,
                        DeliveryAttempt.conversation_id == conversation.id,
                    )
                )
                if existing_intent is not None:
                    raise DomainError(ErrorCode.DELIVERY_INTENT_EXISTS)
                if (
                    source.status != "COMPLETED"
                    or source.outcome != "DRAFT_READY"
                    or source.draft_text is None
                ):
                    raise DomainError(ErrorCode.NO_DRAFT)
                if conversation.control_mode != "AGENT":
                    raise DomainError(ErrorCode.CONTROL_MODE_CONFLICT)
                if not (
                    request.expected_authority_revision
                    == conversation.authority_revision
                    == source.authority_revision
                ):
                    raise DomainError(ErrorCode.AUTHORITY_REVISION_CONFLICT)
                clock = await db_clock(session)
                attempt = DeliveryAttempt(
                    id=uuid4(),
                    conversation_id=conversation.id,
                    delivery_key=request.delivery_key,
                    source_turn_id=source.id,
                    creation_fingerprint=digest,
                    authority_revision=conversation.authority_revision,
                    status="GENERATED",
                    revision=0,
                    created_at=clock,
                    updated_at=clock,
                )
                session.add(attempt)
                await session.flush()
                result = DeliveryResult(
                    attempt=DeliveryAttemptRead.model_validate(attempt), replayed=False
                )
    except IntegrityError as exc:
        if unique_violation(exc, "uq_delivery_source_turn"):
            raise DomainError(ErrorCode.DELIVERY_INTENT_EXISTS) from None
        raise
    return result


async def get_delivery(
    database: Database, principal: Principal, delivery_attempt_id: UUID
) -> DeliveryAttemptRead:
    async with database.session_factory() as session, session.begin():
        attempt = await session.scalar(
            select(DeliveryAttempt)
            .join(Conversation, Conversation.id == DeliveryAttempt.conversation_id)
            .where(DeliveryAttempt.id == delivery_attempt_id, scope_filter(principal))
        )
        if attempt is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        return DeliveryAttemptRead.model_validate(attempt)


def merged_reference(current: str | None, reported: str | None) -> str | None:
    if current is not None and reported is not None and current != reported:
        raise DomainError(ErrorCode.DELIVERY_REFERENCE_CONFLICT)
    return reported if reported is not None else current


async def reconcile_delivery(
    database: Database, principal: Principal, delivery_attempt_id: UUID, request: DeliveryReconcile
) -> DeliveryReconcileResult:
    digest = fingerprint(
        {
            "contract_version": 1,
            "delivery_attempt_id": str(delivery_attempt_id),
            **request.model_dump(mode="json"),
        }
    )
    async with database.session_factory() as session, session.begin():
        conversation, attempt = await locked_delivery(session, principal, delivery_attempt_id)
        previous = await session.scalar(
            select(DeliveryReconciliation).where(
                DeliveryReconciliation.delivery_attempt_id == attempt.id,
                DeliveryReconciliation.conversation_id == conversation.id,
                DeliveryReconciliation.reconciliation_key == request.reconciliation_key,
            )
        )
        if previous is not None:
            if previous.fingerprint != digest:
                raise DomainError(ErrorCode.DELIVERY_RECONCILIATION_CONFLICT)
            result = DeliveryReconcileResult(
                attempt=DeliveryAttemptRead.model_validate(attempt),
                applied_revision=previous.result_revision,
                replayed=True,
            )
        else:
            if request.expected_revision != attempt.revision:
                raise DomainError(ErrorCode.DELIVERY_REVISION_CONFLICT)
            status = request.status.value
            if status != attempt.status and status not in TRANSITIONS[attempt.status]:
                raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
            if status == "DISPATCH_REQUESTED":
                if conversation.control_mode != "AGENT":
                    raise DomainError(ErrorCode.CONTROL_MODE_CONFLICT)
                if not (
                    request.expected_authority_revision
                    == attempt.authority_revision
                    == conversation.authority_revision
                ):
                    raise DomainError(ErrorCode.AUTHORITY_REVISION_CONFLICT)
            operation_ref = merged_reference(
                attempt.provider_operation_id, request.provider_operation_id
            )
            message_ref = merged_reference(attempt.provider_message_id, request.provider_message_id)
            clock = await db_clock(session)
            attempt.status = status
            attempt.revision += 1
            attempt.updated_at = clock
            attempt.provider_operation_id = operation_ref
            attempt.provider_message_id = message_ref
            attempt.error_code = (
                request.error_code.value if request.error_code is not None else None
            )
            if status == "DISPATCH_REQUESTED" and attempt.dispatch_requested_at is None:
                attempt.dispatch_requested_at = clock
            if status == "PROVIDER_ACCEPTED" and attempt.provider_accepted_at is None:
                attempt.provider_accepted_at = clock
            if status in ("DELIVERED", "READ") and attempt.delivered_at is None:
                attempt.delivered_at = clock
            if status == "READ" and attempt.read_at is None:
                attempt.read_at = clock
            session.add(
                DeliveryReconciliation(
                    id=uuid4(),
                    conversation_id=conversation.id,
                    delivery_attempt_id=attempt.id,
                    reconciliation_key=request.reconciliation_key,
                    fingerprint=digest,
                    expected_revision=request.expected_revision,
                    result_revision=attempt.revision,
                    status=status,
                    provider_operation_id=request.provider_operation_id,
                    provider_message_id=request.provider_message_id,
                    provider_occurred_at=request.provider_occurred_at,
                    error_code=request.error_code.value if request.error_code is not None else None,
                    expected_authority_revision=request.expected_authority_revision,
                    accepted_at=clock,
                )
            )
            await session.flush()
            result = DeliveryReconcileResult(
                attempt=DeliveryAttemptRead.model_validate(attempt),
                applied_revision=attempt.revision,
                replayed=False,
            )
    return result

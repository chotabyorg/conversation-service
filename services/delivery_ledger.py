"""Shared append-only delivery evidence; provider namespaces remain in adapters."""
from datetime import datetime
from uuid import UUID, uuid4

from sqlalchemy.ext.asyncio import AsyncSession

from models import DeliveryAttempt, DeliveryReconciliation
from services.common import fingerprint


def _record_reconciliation(
    session: AsyncSession,
    delivery: DeliveryAttempt,
    conversation_id: UUID,
    status: str,
    *,
    provider_message_id: str | None = None,
    error_code: str | None = None,
    key: UUID,
    expected_revision: int,
    authority_revision: int | None = None,
    expected_settling_revision: int | None = None,
    result_settling_revision: int | None = None,
    clock: datetime,
) -> None:
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
    payload = {
        "status": status,
        "provider_message_id": provider_message_id,
        "error_code": error_code,
        "expected_authority_revision": authority_revision,
        "expected_settling_revision": expected_settling_revision,
        "result_settling_revision": result_settling_revision,
    }
    session.add(
        DeliveryReconciliation(
            id=uuid4(),
            conversation_id=conversation_id,
            delivery_attempt_id=delivery.id,
            reconciliation_key=key,
            fingerprint=fingerprint(
                {"contract_version": 1, "delivery_attempt_id": str(delivery.id), **payload}
            ),
            expected_revision=expected_revision,
            result_revision=delivery.revision,
            status=status,
            provider_message_id=provider_message_id,
            error_code=error_code,
            expected_authority_revision=authority_revision,
            expected_settling_revision=expected_settling_revision,
            result_settling_revision=result_settling_revision,
            accepted_at=clock,
        )
    )

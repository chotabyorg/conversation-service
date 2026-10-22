"""Persist positive lifecycle facts, never infer them from absence or timeout."""

from __future__ import annotations

from uuid import UUID, uuid4

from pydantic import TypeAdapter
from sqlalchemy import select

from core.config import Settings
from core.errors import DomainError
from db.database import Database
from models import Conversation, WriterRecoveryCommand, WriterRegistration, WriterWitness
from schemas.contracts import ErrorCode, Principal
from schemas.handoff import TelegramBeginRequest
from schemas.writer_attestation import (
    AttestationReceipt,
    RecoveryResult,
    WebsiteRegistrationIdentity,
)
from schemas.writer_attestation import (
    WriterRegistration as RegistrationRequest,
)
from schemas.writer_attestation import (
    WriterWitness as WitnessRequest,
)
from schemas.writer_retirement import (
    RetireCommand,
    WebsiteWriterIdentity,
    WebsiteWriterRetireCommand,
    WebsiteWriterTerminationProof,
    WriterTerminationProof,
)
from services.common import db_clock, fingerprint
from services.writer_retirement import _load_writer

StoredDescriptor = TelegramBeginRequest | WebsiteWriterIdentity
DESCRIPTOR: TypeAdapter[StoredDescriptor] = TypeAdapter(StoredDescriptor)
COMMAND: TypeAdapter[RetireCommand] = TypeAdapter(RetireCommand)


def _command(
    descriptor: StoredDescriptor, proof_ref: UUID, revision: int, command_id: UUID
) -> RetireCommand:
    payload = descriptor.model_dump(mode="json")
    payload.update(
        command_id=str(command_id), proof_ref=str(proof_ref), expected_delivery_revision=revision
    )
    if not isinstance(descriptor, WebsiteWriterIdentity):
        payload["work_kind"] = "RECOVER_ONLY"
    return COMMAND.validate_python(payload)


def _authorize(principal: Principal, descriptor: StoredDescriptor, settings: Settings) -> None:
    channel = "WEBSITE_WIDGET" if isinstance(descriptor, WebsiteWriterIdentity) else "TELEGRAM"
    producer = next(
        (
            p
            for p in settings.writer_producers
            if "writer-producer:" + p.principal_id == principal.principal_id
        ),
        None,
    )
    if principal.role != "INBOX_LIFECYCLE" or producer is None or not any(
        g.owner_id == descriptor.owner_id
        and g.integration_id == descriptor.integration_id
        and g.agent_id == descriptor.agent_id
        and g.channel == channel
        for g in producer.grants
    ):
        raise DomainError(ErrorCode.NOT_FOUND)


async def register(
    database: Database, principal: Principal, request: RegistrationRequest, settings: Settings
) -> AttestationReceipt:
    raw_descriptor = request.descriptor
    digest = fingerprint(request.model_dump(mode="json"))
    async with database.session_factory() as session, session.begin():
        descriptor: StoredDescriptor
        if isinstance(raw_descriptor, WebsiteRegistrationIdentity):
            root_id = await session.scalar(
                select(Conversation.id).filter_by(
                    owner_id=raw_descriptor.owner_id,
                    integration_id=raw_descriptor.integration_id,
                    channel="WEBSITE_WIDGET",
                    provider_thread_id=str(raw_descriptor.session_id),
                )
            )
            if root_id is None:
                raise DomainError(ErrorCode.NOT_FOUND)
            descriptor = WebsiteWriterIdentity.model_validate(
                dict(raw_descriptor.model_dump(mode="json"), conversation_id=str(root_id))
            )
        else:
            descriptor = raw_descriptor
        _authorize(principal, descriptor, settings)
        _, _, intent = await _load_writer(
            session,
            principal,
            settings,
            _command(descriptor, request.registration_id, 0, request.registration_id),
        )
        existing = await session.get(WriterRegistration, request.registration_id)
        if existing is not None:
            if existing.fingerprint != digest or existing.producer_id != principal.principal_id:
                raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
            closed = await session.get(WriterWitness, existing.id)
            return AttestationReceipt(
                registration_id=existing.id, state="CLOSED" if closed else "OPEN", replayed=True
            )
        same = await session.scalar(
            select(WriterRegistration.id).filter_by(
                intent_id=intent.id,
                supplier_instance_id=descriptor.supplier_instance_id,
                process_incarnation_id=descriptor.process_incarnation_id,
            )
        )
        if same is not None:
            raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
        # Never retroactively attach provenance to an unsupervised acquisition.
        if intent.writer_state != "NONE" or intent.status != "GENERATED":
            raise DomainError(ErrorCode.INVALID_DELIVERY_TRANSITION)
        session.add(
            WriterRegistration(
                id=request.registration_id,
                intent_id=intent.id,
                producer_id=principal.principal_id,
                supplier_instance_id=descriptor.supplier_instance_id,
                process_incarnation_id=descriptor.process_incarnation_id,
                descriptor=descriptor.model_dump(mode="json"),
                fingerprint=digest,
                created_at=await db_clock(session),
            )
        )
        await session.flush()
        return AttestationReceipt(
            registration_id=request.registration_id, state="OPEN", replayed=False
        )


async def witness(
    database: Database, principal: Principal, request: WitnessRequest, settings: Settings
) -> AttestationReceipt:
    async with database.session_factory() as session, session.begin():
        registration = await session.get(WriterRegistration, request.registration_id)
        if registration is None or registration.producer_id != principal.principal_id:
            raise DomainError(ErrorCode.NOT_FOUND)
        descriptor = DESCRIPTOR.validate_python(registration.descriptor)
        _authorize(principal, descriptor, settings)
        # Same scope/root order as actual begin; CLOSED is an immutable witness row.
        await _load_writer(
            session,
            principal,
            settings,
            _command(descriptor, request.proof_ref, 0, request.registration_id),
        )
        digest = fingerprint(request.model_dump(mode="json"))
        existing = await session.get(WriterWitness, registration.id)
        if existing is not None:
            if existing.fingerprint != digest:
                raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
            return AttestationReceipt(
                registration_id=registration.id, state="CLOSED", replayed=True
            )
        if (
            await session.scalar(
                select(WriterWitness.registration_id).filter_by(proof_ref=request.proof_ref)
            )
            is not None
        ):
            raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
        session.add(
            WriterWitness(
                registration_id=registration.id,
                proof_ref=request.proof_ref,
                witness=request.model_dump(mode="json"),
                fingerprint=digest,
                created_at=await db_clock(session),
            )
        )
        await session.flush()
        return AttestationReceipt(registration_id=registration.id, state="CLOSED", replayed=False)


async def proof(
    database: Database, request: RetireCommand, settings: Settings
) -> WriterTerminationProof | WebsiteWriterTerminationProof:
    # Only immutable evidence is read. No locks, synthesis of lifecycle facts or writes.
    async with database.session_factory() as session:
        row = (
            await session.execute(
                select(WriterRegistration, WriterWitness)
                .join(WriterWitness, WriterWitness.registration_id == WriterRegistration.id)
                .where(
                    WriterWitness.proof_ref == request.proof_ref,
                    WriterRegistration.intent_id == request.outbound_intent_id,
                    WriterRegistration.supplier_instance_id == request.supplier_instance_id,
                    WriterRegistration.process_incarnation_id == request.process_incarnation_id,
                )
            )
        ).first()
        if row is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        registration, retained = row
        descriptor = DESCRIPTOR.validate_python(registration.descriptor)
        expected = _command(
            descriptor, request.proof_ref, request.expected_delivery_revision, request.command_id
        )
        if expected != request:
            raise DomainError(ErrorCode.DELIVERY_IDENTITY_CONFLICT)
        from models import DeliveryAttempt

        intent = await session.get(DeliveryAttempt, request.outbound_intent_id)
        if (
            intent is None
            or intent.writer_supplier_instance_id != request.supplier_instance_id
            or intent.writer_process_incarnation_id != request.process_incarnation_id
        ):
            raise DomainError(ErrorCode.NOT_FOUND)
        value = request.model_dump(mode="json")
        value.update(
            {k: retained.witness[k] for k in ("proof_kind", "fenced", "tasks_ended", "observed_at")}
        )
        value["request_fingerprint"] = fingerprint(request.model_dump(mode="json"))
        cls = (
            WebsiteWriterTerminationProof
            if isinstance(request, WebsiteWriterRetireCommand)
            else WriterTerminationProof
        )
        return cls.model_validate(value)


async def recovery(database: Database, registration_id: UUID, settings: Settings) -> RecoveryResult:
    async with database.session_factory() as session, session.begin():
        registration = await session.get(WriterRegistration, registration_id)
        if registration is None:
            raise DomainError(ErrorCode.NOT_FOUND)
        descriptor = DESCRIPTOR.validate_python(registration.descriptor)
        principal = Principal(
            principal_id="delivery-writer-supervisor", role="INBOX_LIFECYCLE", grants=()
        )
        _, _, intent = await _load_writer(
            session, principal, settings, _command(descriptor, registration_id, 0, registration_id)
        )
        previous = await session.scalar(
            select(WriterRecoveryCommand)
            .filter_by(registration_id=registration_id)
            .order_by(WriterRecoveryCommand.expected_revision.desc()).limit(1)
        )
        # Preserve exact command even after commit/ACK loss or normal completion racing it.
        if previous is not None and (
            intent.writer_retirement_command_id is not None
            or intent.revision == previous.expected_revision
            or intent.writer_state != "LIVE"
        ):
            return RecoveryResult(
                state="COMMAND", command=COMMAND.validate_python(previous.command)
            )
        retained = await session.get(WriterWitness, registration_id)
        if retained is None:
            return RecoveryResult(state="PENDING")
        if (
            intent.writer_state != "LIVE"
            or intent.writer_supplier_instance_id != descriptor.supplier_instance_id
            or intent.writer_process_incarnation_id != descriptor.process_incarnation_id
        ):
            return RecoveryResult(state="DONE")
        command = _command(descriptor, retained.proof_ref, intent.revision, uuid4())
        value = command.model_dump(mode="json")
        session.add(
            WriterRecoveryCommand(
                id=command.command_id,
                registration_id=registration_id,
                expected_revision=intent.revision,
                command=value,
                fingerprint=fingerprint(value),
                created_at=await db_clock(session),
            )
        )
        await session.flush()
        return RecoveryResult(state="COMMAND", command=command)

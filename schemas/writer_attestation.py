"""Strict text-free lifecycle provenance; registration never acquires a writer."""

from typing import Literal
from uuid import UUID

from pydantic import StrictBool, model_validator

from schemas.contracts import Contract, UtcTimestamp
from schemas.handoff import ContractVersion1, TelegramBeginRequest
from schemas.website_delivery import WebsiteDeliveryDescriptor
from schemas.website_session import WebsiteSessionScope
from schemas.writer_retirement import RetireCommand


class WebsiteRegistrationIdentity(WebsiteSessionScope, WebsiteDeliveryDescriptor):
    contract_version: ContractVersion1
    channel: Literal["WEBSITE_WIDGET"]
    session_id: UUID
    supplier_instance_id: UUID
    process_incarnation_id: UUID


WriterDescriptor = TelegramBeginRequest | WebsiteRegistrationIdentity


class WriterRegistration(Contract):
    contract_version: ContractVersion1
    registration_id: UUID
    descriptor: WriterDescriptor


class WriterWitness(Contract):
    contract_version: ContractVersion1
    registration_id: UUID
    proof_ref: UUID
    proof_kind: Literal["PROCESS_TERMINATED", "TASKS_FENCED_AND_JOINED"]
    fenced: StrictBool
    tasks_ended: StrictBool
    observed_at: UtcTimestamp

    @model_validator(mode="after")
    def positive(self) -> "WriterWitness":
        if self.fenced is not True or self.tasks_ended is not True:
            raise ValueError("positive termination evidence required")
        return self


class AttestationReceipt(Contract):
    contract_version: ContractVersion1 = 1
    registration_id: UUID
    state: Literal["OPEN", "CLOSED"]
    replayed: StrictBool


class RecoveryRequest(Contract):
    contract_version: ContractVersion1
    registration_id: UUID


class RecoveryResult(Contract):
    contract_version: ContractVersion1 = 1
    state: Literal["PENDING", "COMMAND", "DONE"]
    command: RetireCommand | None = None

"""Writer evidence only: no authority transition or new send permission."""
from typing import Literal
from uuid import UUID

from pydantic import Field, StrictBool, model_validator

from schemas.contracts import Contract, UtcTimestamp
from schemas.handoff import ContractVersion1, Revision, TelegramBeginRequest


class WriterRetireCommand(TelegramBeginRequest):
    work_kind: Literal["RECOVER_ONLY"]
    command_id: UUID
    expected_delivery_revision: Revision
    proof_ref: UUID


class WriterTerminationProof(WriterRetireCommand):
    request_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    proof_kind: Literal["PROCESS_TERMINATED", "TASKS_FENCED_AND_JOINED"]
    fenced: StrictBool
    tasks_ended: StrictBool
    observed_at: UtcTimestamp

    @model_validator(mode="after")
    def positive_proof(self) -> "WriterTerminationProof":
        if self.fenced is not True or self.tasks_ended is not True:
            raise ValueError("positive termination evidence required")
        return self


class WriterRetired(Contract):
    contract_version: ContractVersion1
    command_id: UUID
    outbound_intent_id: UUID
    response_attempt_id: UUID
    supplier_instance_id: UUID
    process_incarnation_id: UUID
    proof_ref: UUID
    proof_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    writer_state: Literal["RETIRED"]
    retired_at: UtcTimestamp
    replayed: StrictBool

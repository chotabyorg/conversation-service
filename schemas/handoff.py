"""Closed owner Inbox, supplier lifecycle, and generalized Telegram DTOs."""

from __future__ import annotations

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, StrictBool, StrictInt, model_validator

from schemas.contracts import (
    Contract,
    ExternalId,
    OwnerId,
    ProviderReference,
    TextContent,
    UtcTimestamp,
)

Revision = Annotated[StrictInt, Field(ge=0, le=9_223_372_036_854_775_807)]
PositiveRevision = Annotated[StrictInt, Field(ge=1, le=9_223_372_036_854_775_807)]
Generation = Annotated[StrictInt, Field(ge=0, le=9_223_372_036_854_775_807)]
PositiveGeneration = Annotated[StrictInt, Field(ge=1, le=9_223_372_036_854_775_807)]


class HumanPrincipal(Contract):
    account_id: UUID
    email_verified: StrictBool
    consent_required: StrictBool


class CaseSummary(Contract):
    case_id: UUID
    conversation_id: UUID
    owner_id: OwnerId
    integration_id: UUID
    channel: Literal["TELEGRAM"]
    provider_thread_id: ExternalId
    status: Literal["OPEN", "CLAIMED", "RETURNED"]
    case_revision: PositiveRevision
    authority_revision: PositiveRevision
    assignment_revision: Revision
    assigned_account_id: UUID | None
    reason: Literal[
        "CUSTOMER_REQUEST",
        "AGENT_LIMIT",
        "BUSINESS_UNCERTAIN",
        "POLICY_REQUIRES_HUMAN",
        "OTHER",
    ]
    note: str | None
    created_at: UtcTimestamp
    updated_at: UtcTimestamp


class MessageView(Contract):
    message_id: UUID
    sequence: PositiveRevision
    actor: Literal["CUSTOMER", "AGENT", "HUMAN", "SYSTEM"]
    direction: Literal["INBOUND", "OUTBOUND"]
    content_type: Literal["TEXT"]
    text: TextContent
    delivery_state: (
        Literal[
            "PENDING",
            "PROVIDER_ACCEPTED",
            "FAILED",
            "UNKNOWN",
            "SUPERSEDED",
        ]
        | None
    )
    provider_message_id: ProviderReference | None
    delivery_blocked_by_live_writer: StrictBool = False


class CasePage(Contract):
    items: list[CaseSummary] = Field(max_length=50)
    next_cursor: str | None


class CaseDetail(Contract):
    case: CaseSummary
    messages: list[MessageView] = Field(max_length=100)
    next_after_sequence: Revision | None


class InboxCommand(Contract):
    command_id: UUID
    expected_authority_revision: Revision
    expected_case_revision: Revision


class ClaimCommand(InboxCommand):
    pass


class SendHumanCommand(InboxCommand):
    expected_assignment_revision: Revision
    text: TextContent


class ReturnToAgentCommand(InboxCommand):
    pass


class CommandReceipt(Contract):
    command_id: UUID
    kind: Literal["CLAIM", "SEND_HUMAN", "RETURN_TO_AGENT"]
    case_id: UUID
    applied_case_revision: PositiveRevision
    applied_authority_revision: Revision
    applied_assignment_revision: Revision | None
    message_id: UUID | None
    outbound_intent_id: UUID | None
    replayed: StrictBool


class InboxCommandResult(Contract):
    receipt: CommandReceipt
    case: CaseSummary


class CpBindingProofRequest(Contract):
    contract_version: Literal[1]
    command_id: UUID
    owner_id: OwnerId
    integration_id: UUID
    channel: Literal["TELEGRAM"]
    agent_id: UUID
    expected_binding_generation: Generation


class CpBindingProof(Contract):
    contract_version: Literal[1]
    account_id: UUID
    owner_id: OwnerId
    integration_id: UUID
    channel: Literal["TELEGRAM"]
    agent_id: UUID
    binding_generation: Generation


class AiAgentProofRequest(Contract):
    contract_version: Literal[1]
    owner_id: OwnerId
    integration_id: UUID
    channel: Literal["TELEGRAM"]
    agent_id: UUID


class AiAgentProof(Contract):
    contract_version: Literal[1]
    owner_id: OwnerId
    integration_id: UUID
    channel: Literal["TELEGRAM"]
    agent_id: UUID
    agent_owner_id: UUID
    agent_lifecycle_generation: Generation


class InboxGrantProvision(Contract):
    contract_version: Literal[1]
    command_id: UUID
    owner_id: OwnerId
    integration_id: UUID
    channel: Literal["TELEGRAM"]
    agent_id: UUID
    expected_binding_generation: Generation


class InboxGrantProvisioned(Contract):
    contract_version: Literal[1]
    command_id: UUID
    account_id: UUID
    owner_id: OwnerId
    integration_id: UUID
    channel: Literal["TELEGRAM"]
    agent_id: UUID
    binding_generation: Generation
    agent_owner_id: UUID
    agent_lifecycle_generation: Generation
    grant_generation: PositiveGeneration
    active: Literal[True]
    request_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    replayed: StrictBool


class InboxGrantRevokeBase(Contract):
    contract_version: Literal[1]
    command_id: UUID
    owner_id: OwnerId
    integration_id: UUID
    channel: Literal["TELEGRAM"]
    agent_id: UUID


class CpGrantRevoke(InboxGrantRevokeBase):
    lifecycle_source: Literal["CONTROL_PLANE_BINDING"]
    source_binding_generation: Generation
    target_binding_generation: PositiveGeneration

    @model_validator(mode="after")
    def target_advances(self) -> CpGrantRevoke:
        if self.target_binding_generation != self.source_binding_generation + 1:
            raise ValueError("invalid lifecycle generation")
        return self


class AiGrantRevoke(InboxGrantRevokeBase):
    lifecycle_source: Literal["AI_AGENT"]
    agent_owner_id: UUID
    source_agent_lifecycle_generation: Generation
    target_agent_lifecycle_generation: PositiveGeneration

    @model_validator(mode="after")
    def target_advances(self) -> AiGrantRevoke:
        if self.target_agent_lifecycle_generation != self.source_agent_lifecycle_generation + 1:
            raise ValueError("invalid lifecycle generation")
        return self


GrantRevoke = Annotated[CpGrantRevoke | AiGrantRevoke, Field(discriminator="lifecycle_source")]


class InboxGrantRevokedBase(Contract):
    contract_version: Literal[1]
    command_id: UUID
    owner_id: OwnerId
    integration_id: UUID
    channel: Literal["TELEGRAM"]
    agent_id: UUID
    grant_generation: Generation
    active: Literal[False]
    request_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    replayed: StrictBool


class CpGrantRevoked(InboxGrantRevokedBase):
    lifecycle_source: Literal["CONTROL_PLANE_BINDING"]
    source_binding_generation: Generation
    target_binding_generation: PositiveGeneration


class AiGrantRevoked(InboxGrantRevokedBase):
    lifecycle_source: Literal["AI_AGENT"]
    agent_owner_id: UUID
    source_agent_lifecycle_generation: Generation
    target_agent_lifecycle_generation: PositiveGeneration


InboxGrantRevoked = Annotated[
    CpGrantRevoked | AiGrantRevoked, Field(discriminator="lifecycle_source")
]


class TelegramWorkScope(Contract):
    owner_id: OwnerId
    integration_id: UUID
    agent_id: UUID
    bot_id: PositiveRevision


class TelegramWorkRequest(TelegramWorkScope):
    contract_version: Literal[2]


class TelegramDeliveryIdentity(Contract):
    contract_version: Literal[2]
    owner_id: OwnerId
    integration_id: UUID
    agent_id: UUID
    conversation_id: UUID
    provider_thread_id: ExternalId
    bot_id: PositiveRevision
    outbound_intent_id: UUID
    outbound_ordinal: PositiveRevision
    source_kind: Literal["AGENT_TURN", "OUTBOUND_MESSAGE"]
    source_id: UUID
    response_attempt_id: UUID
    work_kind: Literal["SEND_CANDIDATE", "RECOVER_ONLY"]


class TelegramWorkItem(TelegramDeliveryIdentity):
    text: TextContent


class TelegramWorkResult(Contract):
    items: list[TelegramWorkItem] = Field(max_length=100)


class TelegramBeginRequest(TelegramDeliveryIdentity):
    supplier_instance_id: UUID
    process_incarnation_id: UUID


class TelegramBeginResult(TelegramWorkItem):
    supplier_instance_id: UUID
    process_incarnation_id: UUID
    send: StrictBool
    writer_state: Literal["NONE", "LIVE", "ENDED", "RETIRED"]
    replayed: StrictBool


class TelegramWriterEnd(Contract):
    kind: Literal["TASK_JOINED"]
    supplier_instance_id: UUID
    process_incarnation_id: UUID


class TelegramCompleteRequest(TelegramBeginRequest):
    outcome: Literal["PROVIDER_ACCEPTED", "FAILED", "UNKNOWN"]
    provider_message_id: ProviderReference | None = None
    error_code: Literal["PROVIDER_REJECTED", "TRANSPORT_ERROR", "TIMEOUT"] | None = None
    writer_end: TelegramWriterEnd | None = None

    @model_validator(mode="after")
    def provider_evidence_shape(self) -> TelegramCompleteRequest:
        if self.outcome == "PROVIDER_ACCEPTED":
            if self.provider_message_id is None or self.error_code is not None:
                raise ValueError("invalid provider evidence")
        elif self.outcome == "FAILED":
            if self.provider_message_id is not None or self.error_code != "PROVIDER_REJECTED":
                raise ValueError("invalid provider evidence")
        elif self.provider_message_id is not None or self.error_code not in {
            "TRANSPORT_ERROR",
            "TIMEOUT",
        }:
            raise ValueError("invalid provider evidence")
        if self.writer_end is not None and (
            self.writer_end.supplier_instance_id != self.supplier_instance_id
            or self.writer_end.process_incarnation_id != self.process_incarnation_id
        ):
            raise ValueError("writer end identity mismatch")
        return self


class TelegramCompleteResult(Contract):
    contract_version: Literal[2]
    outbound_intent_id: UUID
    response_attempt_id: UUID
    outcome: Literal["PROVIDER_ACCEPTED", "FAILED", "UNKNOWN"]
    writer_state: Literal["LIVE", "ENDED", "RETIRED"]
    replayed: StrictBool

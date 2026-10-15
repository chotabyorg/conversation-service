"""Narrow Website communication contract; no public credential or business policy."""
from typing import Literal
from uuid import UUID

from pydantic import Field, model_validator

from schemas.contracts import Contract, OperationReference, OwnerId, TextContent

WEBSITE_NAMESPACE = "main-chota.website-widget.v1"


class WebsiteIdentity(Contract):
    owner_id: OwnerId
    integration_id: UUID
    agent_id: UUID
    session_id: UUID
    message_id: UUID


class WebsiteAdmission(WebsiteIdentity):
    text: TextContent
    desired_route: Literal["MANAGED", "LEGACY"]


class WebsiteAdmissionResult(Contract):
    route: Literal["MANAGED", "LEGACY"]
    admission_id: UUID
    replayed: bool


class WebsiteResult(Contract):
    state: Literal["PENDING", "DRAFT_READY", "NO_RESPONSE", "FAILED", "UNKNOWN"]
    draft_text: TextContent | None = None


class WebsiteResponseBegin(WebsiteIdentity):
    response_attempt_id: UUID


class WebsiteResponseBegun(Contract):
    action: Literal["SEND", "REPLAY_COMPLETED", "DENY"]
    response_attempt_id: UUID
    replay_source_attempt_id: UUID | None = None
    draft_text: TextContent
    send: bool
    replayed: bool
    denial_reason: Literal["SUPERSEDED", "DELIVERY_NOT_RETRYABLE"] | None = None

    @model_validator(mode="after")
    def action_truth_table(self) -> "WebsiteResponseBegun":
        if self.action == "SEND":
            valid = (self.send and not self.replayed
                     and self.replay_source_attempt_id is None
                     and self.denial_reason is None)
        elif self.action == "REPLAY_COMPLETED":
            valid = (not self.send and self.replayed
                     and self.replay_source_attempt_id is not None
                     and self.denial_reason is None)
        else:
            valid = (not self.send and self.replay_source_attempt_id is None
                     and self.denial_reason is not None)
        if not valid:
            raise ValueError("invalid Website response action")
        return self


class WebsiteResponseComplete(WebsiteResponseBegin):
    outcome: Literal["WRITE_COMPLETED", "WRITE_FAILED", "UNKNOWN"]


class WebsiteResponseCompleted(Contract):
    response_attempt_id: UUID
    outcome: Literal["WRITE_COMPLETED", "WRITE_FAILED", "UNKNOWN"]
    replayed: bool


class ManagedEvidenceRequest(Contract):
    contract_version: Literal[1] = 1
    owner_id: OwnerId
    integration_id: UUID
    channel: Literal["WEBSITE_WIDGET"] = "WEBSITE_WIDGET"
    agent_id: UUID
    conversation_id: UUID
    turn_id: UUID


class ManagedEvidenceResult(Contract):
    contract_version: Literal[1] = 1
    conversation_id: UUID
    turn_id: UUID
    agent_id: UUID
    operation_refs: list[OperationReference] = Field(max_length=32)

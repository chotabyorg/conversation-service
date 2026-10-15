"""Strict local mirror of the AI service's enrolled managed-turn v3 wire."""

from __future__ import annotations

from typing import Annotated, Literal, Self
from uuid import UUID

from pydantic import BeforeValidator, Field, StrictInt, field_validator, model_validator

from schemas.contracts import Contract, OwnerId, TextContent, validate_operation_id

Counter = Annotated[StrictInt, Field(ge=1, le=9_223_372_036_854_775_807)]
Revision = Annotated[StrictInt, Field(ge=0, le=9_223_372_036_854_775_807)]
ContractVersion3 = Annotated[StrictInt, Field(ge=3, le=3)]
ERROR_CODES = Literal[
    "AGENT_INACTIVE",
    "AGENT_KEY_MISSING",
    "AGENT_CONFIGURATION_ERROR",
    "CONTEXT_LIMIT",
    "MODEL_AUTH_FAILED",
    "MODEL_INSUFFICIENT_FUNDS",
    "MODEL_RATE_LIMITED",
    "MODEL_TIMEOUT",
    "MODEL_UNAVAILABLE",
    "MODEL_ERROR",
    "PRIVACY_UNAVAILABLE",
    "TOOL_SETUP_FAILED",
    "TOOL_EXECUTION_UNKNOWN",
    "TOOL_CLEANUP_FAILED",
    "EXECUTION_TIMEOUT",
    "INVALID_RESULT",
    "INTERNAL_ERROR",
]
OUTCOMES = Literal["DRAFT_READY", "NO_RESPONSE", "FAILED", "UNKNOWN"]
OPERATION_OUTCOMES = Literal["SUCCEEDED", "FAILED", "UNKNOWN"]


def _canonical_uuid(value: object) -> UUID:
    if isinstance(value, UUID):
        return value
    if not isinstance(value, str):
        raise ValueError("UUID must be a canonical string")
    try:
        parsed = UUID(value)
    except (ValueError, TypeError, AttributeError):
        raise ValueError("UUID must be a canonical string") from None
    if str(parsed) != value:
        raise ValueError("UUID must be a canonical lowercase string")
    return parsed


CanonicalUUID = Annotated[UUID, BeforeValidator(_canonical_uuid)]


class V3Message(Contract):
    id: CanonicalUUID
    sequence: Counter
    actor: Literal["CUSTOMER", "AGENT", "HUMAN", "SYSTEM"]
    direction: Literal["INBOUND", "OUTBOUND"]
    content_type: Literal["TEXT"]
    text: TextContent


class AuthorityEventFact(Contract):
    kind: Literal["AUTHORITY_EVENT"]
    event_id: CanonicalUUID
    authority_revision: Revision
    from_mode: Literal["AGENT", "HUMAN"]
    to_mode: Literal["AGENT", "HUMAN"]
    case_id: CanonicalUUID


class DeliveryObservationFact(Contract):
    kind: Literal["DELIVERY_OBSERVATION"]
    message_id: CanonicalUUID
    delivery_attempt_id: CanonicalUUID | None
    source_authority_revision: Revision
    status: Literal[
        "NOT_ACQUIRED",
        "GENERATED",
        "DISPATCH_REQUESTED",
        "PROVIDER_ACCEPTED",
        "DELIVERED",
        "READ",
        "FAILED",
        "SUPERSEDED",
        "UNKNOWN",
        "PARTIAL",
        "INTERRUPTED",
    ]

    @model_validator(mode="after")
    def attempt_matches_status(self) -> Self:
        if (self.status == "NOT_ACQUIRED") != (self.delivery_attempt_id is None):
            raise ValueError("delivery attempt identity does not match status")
        return self


CommunicationFact = Annotated[
    AuthorityEventFact | DeliveryObservationFact, Field(discriminator="kind")
]


class V3OperationReference(Contract):
    origin: Literal["AI_AGENT_SERVICE"]
    operation_id: str
    outcome: OPERATION_OUTCOMES | None = Field(...)

    @field_validator("operation_id")
    @classmethod
    def valid_operation_id(cls, value: str) -> str:
        return validate_operation_id(value)


class PriorOperationReference(Contract):
    origin_turn_id: CanonicalUUID
    operation_ref: V3OperationReference


class ManagedTurnV3Request(Contract):
    contract_version: ContractVersion3
    conversation_id: CanonicalUUID
    turn_id: CanonicalUUID
    owner_id: OwnerId
    integration_id: CanonicalUUID
    agent_id: CanonicalUUID
    channel: Literal["TELEGRAM"]
    input_revision: Counter
    authority_revision: Revision
    input_first_sequence: Counter
    input_last_sequence: Counter
    context_last_sequence: Counter
    messages: list[V3Message] = Field(min_length=1, max_length=256)
    selected_input_message_ids: list[CanonicalUUID] = Field(min_length=1, max_length=256)
    communication_facts: list[CommunicationFact] = Field(max_length=64)
    prior_operation_refs: list[PriorOperationReference] = Field(max_length=32)

    @model_validator(mode="after")
    def validate_projection(self) -> Self:
        first, last, bound = (
            self.input_first_sequence,
            self.input_last_sequence,
            self.context_last_sequence,
        )
        if not self.messages[0].sequence <= first <= last <= bound:
            raise ValueError("invalid frozen boundary")
        sequences = [item.sequence for item in self.messages]
        if sequences[-1] != bound or sequences != list(
            range(sequences[0], sequences[0] + len(sequences))
        ):
            raise ValueError("invalid canonical ordering")
        selected = [item.id for item in self.messages if first <= item.sequence <= last]
        if self.selected_input_message_ids != selected:
            raise ValueError("invalid selected input")
        if sum(len(item.text.encode("utf-8")) for item in self.messages) > 524_288:
            raise ValueError("projected messages exceed limit")
        prior_keys = [
            (str(item.origin_turn_id), item.operation_ref.origin, item.operation_ref.operation_id)
            for item in self.prior_operation_refs
        ]
        if prior_keys != sorted(prior_keys) or len(prior_keys) != len(set(prior_keys)):
            raise ValueError("prior operation references are not unique and sorted")
        outbound = {
            item.id
            for item in self.messages
            if item.actor in {"HUMAN", "SYSTEM"} and item.direction == "OUTBOUND"
        }
        observations = [
            item.message_id
            for item in self.communication_facts
            if isinstance(item, DeliveryObservationFact)
        ]
        if len(observations) != len(set(observations)) or not outbound.issubset(observations):
            raise ValueError("outbound HUMAN/SYSTEM messages need unique delivery observations")
        if len(self.model_dump_json().encode("utf-8")) > 1_048_576:
            raise ValueError("request exceeds limit")
        return self


HandoffReason = Literal[
    "CUSTOMER_REQUEST",
    "AGENT_LIMIT",
    "BUSINESS_UNCERTAIN",
    "POLICY_REQUIRES_HUMAN",
    "OTHER",
]


class HandoffControlRequest(Contract):
    type: Literal["HUMAN_HANDOFF"]
    request_id: CanonicalUUID
    reason: HandoffReason
    note: str | None

    @field_validator("note")
    @classmethod
    def bounded_note(cls, value: str | None) -> str | None:
        if value is None:
            return None
        if (
            not isinstance(value, str)
            or not 1 <= len(value) <= 1000
            or len(value.encode("utf-8")) > 4000
            or "\x00" in value
            or any(0xD800 <= ord(char) <= 0xDFFF for char in value)
        ):
            raise ValueError("invalid control note")
        return value


class ManagedTurnV3Result(Contract):
    contract_version: ContractVersion3
    result_kind: Literal["EXECUTION_RESULT", "PLATFORM_CONTROL_REQUEST"]
    original_dispatch_id: CanonicalUUID
    request_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    conversation_id: CanonicalUUID
    turn_id: CanonicalUUID
    agent_id: CanonicalUUID
    input_revision: Counter
    authority_revision: Revision
    input_first_sequence: Counter
    input_last_sequence: Counter
    context_last_sequence: Counter
    processing_outcome: OUTCOMES
    draft_text: TextContent | None
    error_code: ERROR_CODES | None
    operation_refs: list[V3OperationReference] = Field(max_length=32)
    control_request: HandoffControlRequest | None

    @model_validator(mode="after")
    def valid_result(self) -> Self:
        control = self.result_kind == "PLATFORM_CONTROL_REQUEST"
        if control != (self.control_request is not None):
            raise ValueError("invalid control result shape")
        if control:
            if (
                self.processing_outcome not in {"NO_RESPONSE", "FAILED", "UNKNOWN"}
                or self.draft_text is not None
            ):
                raise ValueError("invalid control result")
        elif (self.processing_outcome == "DRAFT_READY") != (self.draft_text is not None):
            raise ValueError("invalid execution draft")
        if (self.processing_outcome in {"FAILED", "UNKNOWN"}) != (self.error_code is not None):
            raise ValueError("invalid processing error")
        if (
            self.processing_outcome in {"DRAFT_READY", "NO_RESPONSE"}
            and self.error_code is not None
        ):
            raise ValueError("unexpected processing error")
        keys = [(item.origin, item.operation_id) for item in self.operation_refs]
        if len(keys) != len(set(keys)):
            raise ValueError("duplicate operation reference")
        return self

    def correlate(
        self, request: ManagedTurnV3Request, dispatch_id: UUID, request_fingerprint: str
    ) -> None:
        expected = {
            "contract_version": 3,
            "original_dispatch_id": dispatch_id,
            "request_fingerprint": request_fingerprint,
            "conversation_id": request.conversation_id,
            "turn_id": request.turn_id,
            "agent_id": request.agent_id,
            "input_revision": request.input_revision,
            "authority_revision": request.authority_revision,
            "input_first_sequence": request.input_first_sequence,
            "input_last_sequence": request.input_last_sequence,
            "context_last_sequence": request.context_last_sequence,
        }
        if self.model_dump(include=set(expected)) != expected:
            raise ValueError("result correlation mismatch")


class ControlEvidenceRequest(Contract):
    contract_version: ContractVersion3
    owner_id: OwnerId
    integration_id: CanonicalUUID
    channel: Literal["TELEGRAM"]
    agent_id: CanonicalUUID
    conversation_id: CanonicalUUID
    turn_id: CanonicalUUID
    input_revision: Counter
    authority_revision: Revision
    input_first_sequence: Counter
    input_last_sequence: Counter
    context_last_sequence: Counter
    request_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    original_dispatch_id: CanonicalUUID


class ControlEvidenceResult(Contract):
    contract_version: Literal[3] = 3
    evidence_kind: Literal["CONTROL_REQUEST", "SEALED_NO_CONTROL", "UNSEALED", "UNAVAILABLE"]
    conversation_id: CanonicalUUID
    turn_id: CanonicalUUID
    agent_id: CanonicalUUID
    input_revision: Counter
    authority_revision: Revision
    input_first_sequence: Counter
    input_last_sequence: Counter
    context_last_sequence: Counter
    original_dispatch_id: CanonicalUUID
    request_fingerprint: str = Field(pattern=r"^[0-9a-f]{64}$")
    execution_state: Literal["RUNNING", "SEALED"] | None
    processing_outcome: OUTCOMES | None
    error_code: ERROR_CODES | None
    control_request: HandoffControlRequest | None
    operation_refs: list[V3OperationReference] = Field(max_length=32)

    @model_validator(mode="after")
    def evidence_truth(self) -> Self:
        if self.evidence_kind == "CONTROL_REQUEST" and self.control_request is None:
            raise ValueError("control evidence needs a request")
        if self.evidence_kind == "SEALED_NO_CONTROL" and (
            self.execution_state != "SEALED"
            or self.control_request is not None
            or self.processing_outcome is None
        ):
            raise ValueError("invalid sealed no-control evidence")
        if self.evidence_kind in {"UNSEALED", "UNAVAILABLE"} and (
            self.control_request is not None
            or self.execution_state is not None
            or self.processing_outcome is not None
            or self.error_code is not None
            or self.operation_refs
        ):
            raise ValueError("unproven evidence asserted an outcome")
        return self

    def correlate(self, request: ControlEvidenceRequest) -> None:
        expected = request.model_dump(
            include={
                "conversation_id",
                "turn_id",
                "agent_id",
                "input_revision",
                "authority_revision",
                "input_first_sequence",
                "input_last_sequence",
                "context_last_sequence",
                "original_dispatch_id",
                "request_fingerprint",
            }
        )
        expected["contract_version"] = 3
        if self.model_dump(include=set(expected)) != expected:
            raise ValueError("control evidence correlation mismatch")

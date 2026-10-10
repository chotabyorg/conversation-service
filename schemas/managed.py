"""Independent versioned AI wire contract; no prompt or business policy lives here."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Annotated, Literal
from uuid import UUID

from pydantic import ConfigDict, Field, field_validator, model_validator

from schemas.contracts import (
    Actor,
    Channel,
    Contract,
    Counter,
    Direction,
    OperationOutcome,
    OperationReference,
    OwnerId,
    PositiveCounter,
    TextContent,
    canonical_operation_refs,
)

EXECUTION_NAMESPACE = "main-chota.conversation.v1"
MAX_REQUEST_BYTES = 1048576
MAX_RESULT_BYTES = 262144
MAX_MESSAGES = 256
MAX_PROJECTED_TEXT_BYTES = 524288
ManagedErrorCode = Literal[
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


class ManagedValue(Contract):
    model_config = ConfigDict(extra="forbid", frozen=True, from_attributes=True)


class ManagedMessage(ManagedValue):
    id: UUID
    sequence: PositiveCounter
    actor: Actor
    direction: Direction
    content_type: Literal["TEXT"]
    text: TextContent


class ManagedTurnRequest(ManagedValue):
    contract_version: Annotated[int, Field(strict=True, ge=1, le=2)] = 1
    conversation_id: UUID
    turn_id: UUID
    owner_id: OwnerId
    integration_id: UUID
    agent_id: UUID
    channel: Channel
    input_revision: PositiveCounter
    authority_revision: Counter
    input_first_sequence: PositiveCounter
    input_last_sequence: PositiveCounter
    context_last_sequence: PositiveCounter
    messages: tuple[ManagedMessage, ...] = Field(min_length=1, max_length=MAX_MESSAGES)
    selected_input_message_ids: tuple[UUID, ...] = Field(min_length=1, max_length=MAX_MESSAGES)

    @model_validator(mode="after")
    def frozen_projection(self) -> ManagedTurnRequest:
        first, last, bound = (
            self.input_first_sequence,
            self.input_last_sequence,
            self.context_last_sequence,
        )
        window_first = 1 if self.contract_version == 1 else self.messages[0].sequence
        if not window_first <= first <= last <= bound or self.messages[-1].sequence != bound:
            raise ValueError("invalid frozen boundary")
        if self.contract_version == 1 and bound != len(self.messages):
            raise ValueError("invalid frozen boundary")
        if tuple(m.sequence for m in self.messages) != tuple(
            range(window_first, window_first + len(self.messages))
        ):
            raise ValueError("invalid canonical ordering")
        ids = tuple(m.id for m in self.messages)
        if (len(set(ids)) != len(ids)
                or self.selected_input_message_ids != ids[
                    first - window_first : last - window_first + 1]):
            raise ValueError("invalid selected input")
        if sum(len(m.text.encode("utf-8")) for m in self.messages) > MAX_PROJECTED_TEXT_BYTES:
            raise ValueError("projection exceeds limit")
        if (self.contract_version == 2
                and len(self.model_dump_json().encode("utf-8")) > MAX_REQUEST_BYTES):
            raise ValueError("request exceeds limit")
        return self


class ManagedOperationReference(OperationReference):
    # The AI wire shape requires an explicit nullable outcome, unlike Goal1 input.
    outcome: OperationOutcome | None = Field(...)


class ManagedTurnResult(ManagedValue):
    contract_version: Annotated[int, Field(strict=True, ge=1, le=2)] = 1
    conversation_id: UUID
    turn_id: UUID
    agent_id: UUID
    input_revision: PositiveCounter
    authority_revision: Counter
    input_first_sequence: PositiveCounter
    input_last_sequence: PositiveCounter
    context_last_sequence: PositiveCounter
    status: Literal["DRAFT_READY", "NO_RESPONSE", "FAILED", "UNKNOWN"]
    draft_text: TextContent | None
    error_code: ManagedErrorCode | None
    operation_refs: list[ManagedOperationReference] = Field(default_factory=list, max_length=32)

    @model_validator(mode="after")
    def result_shape(self) -> ManagedTurnResult:
        if (self.status == "DRAFT_READY") != (self.draft_text is not None):
            raise ValueError("invalid draft result")
        if (self.status in {"FAILED", "UNKNOWN"}) != (self.error_code is not None):
            raise ValueError("invalid error result")
        canonical_operation_refs(list(self.operation_refs))
        return self

    @field_validator("operation_refs")
    @classmethod
    def sorted_refs(cls, refs: list[ManagedOperationReference]) -> list[ManagedOperationReference]:
        return sorted(refs, key=lambda ref: (ref.origin.value, ref.operation_id))

    def correlate(self, request: ManagedTurnRequest) -> None:
        for name in ECHO_FIELDS:
            if getattr(self, name) != getattr(request, name):
                raise ValueError("invalid result correlation")


ECHO_FIELDS = (
    "contract_version",
    "conversation_id",
    "turn_id",
    "agent_id",
    "input_revision",
    "authority_revision",
    "input_first_sequence",
    "input_last_sequence",
    "context_last_sequence",
)


@dataclass(frozen=True)
class PreparedManagedRequest:
    request: ManagedTurnRequest
    body: bytes
    fingerprint: str


def prepare_request(request: ManagedTurnRequest) -> PreparedManagedRequest:
    fields = request.model_dump(mode="json")
    body = json.dumps(
        fields, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False
    ).encode("utf-8")
    if len(body) > MAX_REQUEST_BYTES:
        raise ValueError("managed request exceeds limit")
    identity = json.dumps(
        {"execution_namespace": EXECUTION_NAMESPACE, "request": fields},
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return PreparedManagedRequest(request, body, hashlib.sha256(identity).hexdigest())

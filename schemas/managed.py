"""Independent versioned AI wire contract; no prompt or business policy lives here."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Annotated, Any, Literal
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
DEFAULT_MAX_PARTIAL_CHARS = 2000
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
    stream_partials: bool = False
    max_partial_chars: Annotated[int, Field(ge=1, le=8000)] = DEFAULT_MAX_PARTIAL_CHARS

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
        if (
            self.contract_version == 2
            and len(self.model_dump_json().encode("utf-8")) > MAX_REQUEST_BYTES
        ):
            raise ValueError("request exceeds limit")
        if self.stream_partials and (
            self.contract_version != 2 or self.channel != Channel.VOICE
        ):
            raise ValueError("streaming partials require voice contract version 2")
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
    partial_text: TextContent | None = None
    partial_final: bool = False

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
    return _prepare_request_fields(request, fields)


def _prepare_request_fields(
    request: ManagedTurnRequest, fields: dict[str, Any]
) -> PreparedManagedRequest:
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


def prepare_request_for_fingerprint(
    request: ManagedTurnRequest, expected_fingerprint: str
) -> PreparedManagedRequest:
    """Select the exact persisted wire profile without guessing from its age.

    Before stream fields were introduced, both extension keys were absent from
    the canonical request. Existing projections therefore carry one of a small
    number of complete historical digests. Reconstruct each legal profile from
    the immutable request core and return only the one whose full digest
    matches. Any non-default extension value has only its full current
    representation as a candidate, so it cannot be silently erased.
    """
    fields = request.model_dump(mode="json")
    candidates: list[PreparedManagedRequest] = []
    default_extension = request.max_partial_chars == DEFAULT_MAX_PARTIAL_CHARS

    if default_extension:
        pre_stream_request = request.model_copy(update={
            "stream_partials": False,
            "max_partial_chars": DEFAULT_MAX_PARTIAL_CHARS,
        })
        legacy_fields = pre_stream_request.model_dump(mode="json")
        legacy_fields.pop("stream_partials")
        legacy_fields.pop("max_partial_chars")
        candidates.append(_prepare_request_fields(pre_stream_request, legacy_fields))
        candidates.append(_prepare_request_fields(
            pre_stream_request, pre_stream_request.model_dump(mode="json")
        ))
        if request.contract_version == 2 and request.channel == Channel.VOICE:
            voice_stream_request = request.model_copy(update={
                "stream_partials": True,
                "max_partial_chars": DEFAULT_MAX_PARTIAL_CHARS,
            })
            candidates.append(_prepare_request_fields(
                voice_stream_request, voice_stream_request.model_dump(mode="json")
            ))

    # Non-default custom limits remain a complete identity and cannot be
    # stripped or rewritten into one of the known historical profiles.
    if not default_extension:
        candidates.append(_prepare_request_fields(request, fields))

    matches = [candidate for candidate in candidates
               if candidate.fingerprint == expected_fingerprint]
    if len(matches) != 1:
        raise ValueError("stored managed request fingerprint is not an exact profile match")
    return matches[0]

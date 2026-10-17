"""Candidate v1 Voice boundary: authenticated call facts, never client authority."""

from typing import Annotated, Literal
from uuid import UUID

from pydantic import Field, StrictBool, model_validator

from schemas.contracts import Contract, Counter, OwnerId, PositiveCounter, TextContent

VOICE_NAMESPACE = "main-chota.voice.v1"
PlaybackState = Literal[
    "NOT_STARTED", "STARTED", "PARTIAL", "COMPLETED", "INTERRUPTED", "FAILED", "UNKNOWN"
]
AudioBytes = Annotated[int, Field(strict=True, ge=0, le=8 * 1024 * 1024)]


class CallIdentity(Contract):
    call_id: UUID
    media_session_id: UUID


class CallStart(CallIdentity):
    # Supplied by a trusted routing adapter, never by caller audio or a public form.
    owner_id: OwnerId
    integration_id: UUID
    agent_id: UUID
    # Validated caller ANI (the number being called from); optional.
    caller_number: Annotated[str, Field(pattern=r"^\+375[0-9]{9}$")] | None = None


class CallResult(CallIdentity):
    conversation_id: UUID
    state: Literal["ACTIVE", "ENDED", "EXPIRED"]
    replayed: bool = False


class Utterance(CallIdentity):
    sequence: PositiveCounter
    text: TextContent


class UtteranceResult(CallIdentity):
    sequence: PositiveCounter
    message_id: UUID
    replayed: bool


class ResponseCandidate(CallIdentity):
    turn_id: UUID | None = None


class ResponseBegin(CallIdentity):
    turn_id: UUID


class ResponsePermission(ResponseBegin):
    response_id: UUID
    send: bool
    text: TextContent | None = None
    replayed: bool

    @model_validator(mode="after")
    def permission_shape(self) -> "ResponsePermission":
        if self.send != (self.text is not None):
            raise ValueError("invalid speech permission")
        return self


class Playback(CallIdentity):
    response_id: UUID
    event_sequence: PositiveCounter
    state: PlaybackState
    generated_bytes: AudioBytes
    transport_accepted_bytes: AudioBytes
    terminal: StrictBool

    @model_validator(mode="after")
    def fact_shape(self) -> "Playback":
        if self.transport_accepted_bytes > self.generated_bytes:
            raise ValueError("invalid audio counters")
        if self.generated_bytes % 2 or self.transport_accepted_bytes % 2:
            raise ValueError("invalid PCM alignment")
        if self.state in {"STARTED", "PARTIAL"}:
            if self.terminal or not self.transport_accepted_bytes:
                raise ValueError("invalid progress")
        elif not self.terminal:
            raise ValueError("terminal fact required")
        if self.state == "COMPLETED" and (
            not self.generated_bytes or self.generated_bytes != self.transport_accepted_bytes
        ):
            raise ValueError("incomplete transport")
        if self.state == "NOT_STARTED" and self.transport_accepted_bytes:
            raise ValueError("unexpected transport evidence")
        return self


class PlaybackResult(CallIdentity):
    response_id: UUID
    event_sequence: Counter
    replayed: bool


class CallEnd(CallIdentity):
    reason: Literal["DISCONNECTED", "TIMEOUT", "CANCELLED", "FAILED"]

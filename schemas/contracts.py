"""Goal 1 immutable DTOs and the token-free trusted principal contract."""

from __future__ import annotations

import re
import unicodedata
from datetime import UTC, datetime
from enum import Enum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import (
    AfterValidator,
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    PlainSerializer,
    SecretStr,
    field_validator,
    model_validator,
)

MAX_COUNTER = 9223372036854775807
Counter = Annotated[int, Field(strict=True, ge=0, le=MAX_COUNTER)]
PositiveCounter = Annotated[int, Field(strict=True, ge=1, le=MAX_COUNTER)]
LeaseSeconds = Annotated[int, Field(strict=True, ge=1, le=300)]


class Channel(str, Enum):
    TELEGRAM = "TELEGRAM"
    INSTAGRAM = "INSTAGRAM"
    WEBSITE_WIDGET = "WEBSITE_WIDGET"
    WHATSAPP = "WHATSAPP"
    VOICE = "VOICE"


class ControlMode(str, Enum):
    AGENT = "AGENT"
    HUMAN_PENDING = "HUMAN_PENDING"
    HUMAN = "HUMAN"


class Actor(str, Enum):
    CUSTOMER = "CUSTOMER"
    AGENT = "AGENT"
    HUMAN = "HUMAN"
    SYSTEM = "SYSTEM"


class Direction(str, Enum):
    INBOUND = "INBOUND"
    OUTBOUND = "OUTBOUND"


class ContentType(str, Enum):
    TEXT = "TEXT"


class TurnStatus(str, Enum):
    PENDING = "PENDING"
    CLAIMED = "CLAIMED"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class TurnOutcome(str, Enum):
    DRAFT_READY = "DRAFT_READY"
    NO_RESPONSE = "NO_RESPONSE"


class TurnFailureCode(str, Enum):
    PROCESSING_ERROR = "PROCESSING_ERROR"
    TIMEOUT = "TIMEOUT"
    INVALID_RESULT = "INVALID_RESULT"
    EXECUTION_UNKNOWN = "EXECUTION_UNKNOWN"


class OperationOrigin(str, Enum):
    AI_AGENT_SERVICE = "AI_AGENT_SERVICE"


class OperationOutcome(str, Enum):
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"


class DeliveryStatus(str, Enum):
    GENERATED = "GENERATED"
    DISPATCH_REQUESTED = "DISPATCH_REQUESTED"
    SUPERSEDED = "SUPERSEDED"
    PROVIDER_ACCEPTED = "PROVIDER_ACCEPTED"
    DELIVERED = "DELIVERED"
    READ = "READ"
    FAILED = "FAILED"
    UNKNOWN = "UNKNOWN"
    PARTIAL = "PARTIAL"
    INTERRUPTED = "INTERRUPTED"


class DeliveryErrorCode(str, Enum):
    PROVIDER_REJECTED = "PROVIDER_REJECTED"
    TRANSPORT_ERROR = "TRANSPORT_ERROR"
    TIMEOUT = "TIMEOUT"
    INTERRUPTED = "INTERRUPTED"
    UNSPECIFIED = "UNSPECIFIED"


class ErrorCode(str, Enum):
    UNAUTHENTICATED = "UNAUTHENTICATED"
    NOT_FOUND = "NOT_FOUND"
    EVENT_CONFLICT = "EVENT_CONFLICT"
    TURN_SELECTION_CONFLICT = "TURN_SELECTION_CONFLICT"
    ACTIVE_TURN_EXISTS = "ACTIVE_TURN_EXISTS"
    INPUT_REVISION_CONFLICT = "INPUT_REVISION_CONFLICT"
    SETTLING_REQUIRED = "SETTLING_REQUIRED"
    AUTHORITY_REVISION_CONFLICT = "AUTHORITY_REVISION_CONFLICT"
    CONTROL_MODE_CONFLICT = "CONTROL_MODE_CONFLICT"
    INVALID_INPUT_BOUNDARY = "INVALID_INPUT_BOUNDARY"
    TURN_TERMINAL = "TURN_TERMINAL"
    LEASE_HELD = "LEASE_HELD"
    LEASE_EXPIRED = "LEASE_EXPIRED"
    STALE_CLAIM = "STALE_CLAIM"
    TERMINAL_RESULT_CONFLICT = "TERMINAL_RESULT_CONFLICT"
    MANAGED_EXECUTION_STARTED = "MANAGED_EXECUTION_STARTED"
    NO_DRAFT = "NO_DRAFT"
    DELIVERY_IDENTITY_CONFLICT = "DELIVERY_IDENTITY_CONFLICT"
    DELIVERY_INTENT_EXISTS = "DELIVERY_INTENT_EXISTS"
    DELIVERY_RECONCILIATION_CONFLICT = "DELIVERY_RECONCILIATION_CONFLICT"
    DELIVERY_REVISION_CONFLICT = "DELIVERY_REVISION_CONFLICT"
    INVALID_DELIVERY_TRANSITION = "INVALID_DELIVERY_TRANSITION"
    DELIVERY_REFERENCE_CONFLICT = "DELIVERY_REFERENCE_CONFLICT"
    PAYLOAD_TOO_LARGE = "PAYLOAD_TOO_LARGE"
    VALIDATION_ERROR = "VALIDATION_ERROR"
    SERVICE_UNAVAILABLE = "SERVICE_UNAVAILABLE"
    INTERNAL_ERROR = "INTERNAL_ERROR"


def validate_identity(value: object, *, characters: int, byte_limit: int) -> str:
    if not isinstance(value, str):
        raise ValueError("invalid identity")
    if not 1 <= len(value) <= characters or not value.strip():
        raise ValueError("invalid identity")
    if any(unicodedata.category(c) in {"Cc", "Cs"} for c in value):
        raise ValueError("invalid identity")
    if len(value.encode("utf-8")) > byte_limit:
        raise ValueError("invalid identity")
    return value


def validate_owner(value: object) -> str:
    result = validate_identity(value, characters=255, byte_limit=1020)
    if result == "*":
        raise ValueError("invalid owner")
    return result


def validate_external_id(value: object) -> str:
    return validate_identity(value, characters=512, byte_limit=512)


def validate_text(value: object) -> str:
    if not isinstance(value, str) or not 1 <= len(value) <= 16384:
        raise ValueError("invalid text")
    if "\x00" in value or any(0xD800 <= ord(c) <= 0xDFFF for c in value):
        raise ValueError("invalid text")
    if len(value.encode("utf-8")) > 32768:
        raise ValueError("invalid text")
    return value


def validate_ascii(value: object, pattern: str) -> str:
    if not isinstance(value, str) or not value.isascii() or re.fullmatch(pattern, value) is None:
        raise ValueError("invalid identifier")
    return value


def validate_namespace(value: object) -> str:
    return validate_ascii(value, r"[a-z0-9][a-z0-9._:-]{0,127}")


def validate_worker(value: object) -> str:
    return validate_ascii(value, r"[A-Za-z0-9][A-Za-z0-9._:-]{0,63}")


def validate_provider_reference(value: object) -> str:
    return validate_ascii(value, r"[A-Za-z0-9][A-Za-z0-9._:-]{0,254}")


def validate_operation_id(value: object) -> str:
    return validate_ascii(value, r"[A-Za-z0-9][A-Za-z0-9._:-]{0,127}")


def normalize_channel(value: object) -> Channel:
    if not isinstance(value, str) or not value.isascii():
        raise ValueError("invalid channel")
    try:
        return Channel(value.upper())
    except ValueError:
        raise ValueError("invalid channel") from None


def utc_datetime(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        raise ValueError("timezone required")
    try:
        return value.astimezone(UTC)
    except (ValueError, OverflowError):
        raise ValueError("invalid timestamp") from None


def canonical_timestamp(value: datetime) -> str:
    result = utc_datetime(value)
    return (
        f"{result.year:04d}-{result.month:02d}-{result.day:02d}T"
        f"{result.hour:02d}:{result.minute:02d}:{result.second:02d}."
        f"{result.microsecond:06d}Z"
    )


def parse_provider_timestamp(value: object) -> datetime:
    if (
        not isinstance(value, str)
        or re.fullmatch(
            r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})", value
        )
        is None
    ):
        raise ValueError("invalid timestamp")
    try:
        result = datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        raise ValueError("invalid timestamp") from None
    # fromisoformat accepts out-of-range minute components in numeric offsets.
    if value[-1] != "Z" and (int(value[-5:-3]) > 23 or int(value[-2:]) > 59):
        raise ValueError("invalid timestamp")
    return utc_datetime(result)


def validate_claim_token(value: object) -> SecretStr:
    raw = value.get_secret_value() if isinstance(value, SecretStr) else value
    return SecretStr(validate_ascii(raw, r"[A-Za-z0-9_-]{43}"))


OwnerId = Annotated[str, BeforeValidator(validate_owner)]
ExternalId = Annotated[str, BeforeValidator(validate_external_id)]
TextContent = Annotated[str, BeforeValidator(validate_text)]
Namespace = Annotated[str, BeforeValidator(validate_namespace)]
WorkerRef = Annotated[str, BeforeValidator(validate_worker)]
ProviderReference = Annotated[str, BeforeValidator(validate_provider_reference)]
OperationId = Annotated[str, BeforeValidator(validate_operation_id)]
NormalizedChannel = Annotated[Channel, BeforeValidator(normalize_channel)]
UtcTimestamp = Annotated[
    datetime, AfterValidator(utc_datetime), PlainSerializer(canonical_timestamp, return_type=str)
]
ProviderTimestamp = Annotated[UtcTimestamp, BeforeValidator(parse_provider_timestamp)]
ClaimToken = Annotated[SecretStr, BeforeValidator(validate_claim_token)]


class Contract(BaseModel):
    model_config = ConfigDict(extra="forbid", from_attributes=True)


class ScopeGrant(Contract):
    model_config = ConfigDict(extra="forbid", frozen=True)
    owner_id: OwnerId
    integration_id: UUID
    channel: NormalizedChannel
    provider_namespace: Namespace


class Principal(Contract):
    model_config = ConfigDict(extra="forbid", frozen=True)
    principal_id: WorkerRef
    role: Literal["FULL", "WEBSITE", "PROCESSING", "TELEGRAM", "TELEGRAM_PROCESSING"] = "FULL"
    grants: tuple[ScopeGrant, ...] = Field(min_length=1, max_length=64)


class OperationReference(Contract):
    origin: OperationOrigin
    operation_id: OperationId
    outcome: OperationOutcome | None = None


def canonical_operation_refs(refs: list[OperationReference]) -> list[OperationReference]:
    keys = [(ref.origin.value, ref.operation_id) for ref in refs]
    if len(set(keys)) != len(keys):
        raise ValueError("duplicate operation reference")
    return sorted(refs, key=lambda ref: (ref.origin.value, ref.operation_id))


class InboundEventCreate(Contract):
    contract_version: Annotated[int, Field(strict=True, ge=1, le=1)] = 1
    owner_id: OwnerId
    integration_id: UUID
    channel: NormalizedChannel
    provider_thread_id: ExternalId
    provider_namespace: Namespace
    provider_event_id: ExternalId
    text: TextContent
    provider_occurred_at: ProviderTimestamp | None = None


class ConversationRead(Contract):
    id: UUID
    owner_id: OwnerId
    integration_id: UUID
    channel: NormalizedChannel
    provider_thread_id: ExternalId
    last_message_sequence: Counter
    input_revision: Counter
    control_mode: ControlMode
    authority_revision: Counter
    created_at: UtcTimestamp
    updated_at: UtcTimestamp


class MessageRead(Contract):
    id: UUID
    conversation_id: UUID
    sequence: PositiveCounter
    actor: Actor
    direction: Direction
    content_type: ContentType
    text: TextContent
    source_inbound_event_id: UUID | None
    created_at: UtcTimestamp


class InboundEventRead(Contract):
    id: UUID
    conversation_id: UUID
    provider_namespace: Namespace
    provider_event_id: ExternalId
    contract_version: Literal[1]
    fingerprint: Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]
    provider_occurred_at: UtcTimestamp | None
    accepted_at: UtcTimestamp


class InboundEventResult(Contract):
    conversation: ConversationRead
    event: InboundEventRead
    message: MessageRead
    replayed: bool


class MessagePage(Contract):
    items: list[MessageRead]
    next_after_sequence: PositiveCounter | None


class TurnCreate(Contract):
    selection_key: UUID
    input_first_sequence: PositiveCounter
    input_last_sequence: PositiveCounter
    expected_input_revision: Counter
    expected_authority_revision: Counter

    @model_validator(mode="after")
    def validate_boundaries(self) -> TurnCreate:
        if self.input_first_sequence > self.input_last_sequence:
            raise ValueError("invalid input boundary")
        return self


class TurnClaim(Contract):
    worker_ref: WorkerRef
    lease_seconds: LeaseSeconds


class TurnRenew(Contract):
    claim_epoch: PositiveCounter
    claim_token: ClaimToken = Field(repr=False)
    lease_seconds: LeaseSeconds


class TurnExecute(Contract):
    claim_epoch: PositiveCounter
    claim_token: ClaimToken = Field(repr=False)


class TerminalRequest(Contract):
    claim_epoch: PositiveCounter
    claim_token: ClaimToken = Field(repr=False)
    operation_refs: list[OperationReference] = Field(default_factory=list, max_length=32)

    @field_validator("operation_refs")
    @classmethod
    def sort_refs(cls, value: list[OperationReference]) -> list[OperationReference]:
        return canonical_operation_refs(value)


class TurnComplete(TerminalRequest):
    outcome: TurnOutcome
    draft_text: TextContent | None = None

    @model_validator(mode="after")
    def validate_draft(self) -> TurnComplete:
        if (self.outcome == TurnOutcome.DRAFT_READY) != (self.draft_text is not None):
            raise ValueError("invalid draft outcome")
        return self


class TurnFail(TerminalRequest):
    failure_code: TurnFailureCode


class TurnRead(Contract):
    id: UUID
    conversation_id: UUID
    selection_key: UUID
    input_first_sequence: PositiveCounter
    input_last_sequence: PositiveCounter
    context_last_sequence: PositiveCounter
    input_revision: PositiveCounter
    authority_revision: Counter
    status: TurnStatus
    claim_epoch: Counter
    worker_ref: WorkerRef | None
    lease_expires_at: UtcTimestamp | None
    created_at: UtcTimestamp
    started_at: UtcTimestamp | None
    completed_at: UtcTimestamp | None
    outcome: TurnOutcome | None
    draft_text: TextContent | None
    failure_code: TurnFailureCode | None
    operation_refs: list[OperationReference] = Field(max_length=32)
    managed_agent_id: UUID | None
    managed_dispatch_epoch: PositiveCounter | None
    managed_dispatch_started_at: UtcTimestamp | None
    managed_request_fingerprint: Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")] | None


class TurnResult(Contract):
    turn: TurnRead
    replayed: bool


class TurnClaimResult(Contract):
    turn: TurnRead
    # The sole intentional bearer exposure, excluded from repr but returned over HTTP.
    claim_token: Annotated[str, Field(pattern=r"^[A-Za-z0-9_-]{43}$", repr=False)]


class ControlTransition(Contract):
    expected_authority_revision: Counter
    control_mode: ControlMode


class DeliveryCreate(Contract):
    delivery_key: UUID
    source_turn_id: UUID
    expected_authority_revision: Counter


class DeliveryReconcile(Contract):
    reconciliation_key: UUID
    expected_revision: Counter
    status: DeliveryStatus
    provider_operation_id: ProviderReference | None = None
    provider_message_id: ProviderReference | None = None
    provider_occurred_at: ProviderTimestamp | None = None
    error_code: DeliveryErrorCode | None = None
    expected_authority_revision: Counter | None = None
    expected_settling_revision: Counter | None = None
    result_settling_revision: Counter | None = None

    @model_validator(mode="after")
    def validate_evidence(self) -> DeliveryReconcile:
        if self.status == DeliveryStatus.GENERATED:
            raise ValueError("invalid reconciliation status")
        if (self.status == DeliveryStatus.DISPATCH_REQUESTED) != (
            self.expected_authority_revision is not None
        ):
            raise ValueError("invalid authority evidence")
        if (self.expected_settling_revision is None) != (self.result_settling_revision is None):
            raise ValueError("invalid settling evidence")
        if (self.expected_settling_revision is not None
                and self.result_settling_revision is not None
                and self.result_settling_revision < self.expected_settling_revision):
            raise ValueError("invalid settling evidence")
        if self.error_code is not None and self.status not in {
            DeliveryStatus.FAILED,
            DeliveryStatus.UNKNOWN,
            DeliveryStatus.PARTIAL,
            DeliveryStatus.INTERRUPTED,
        }:
            raise ValueError("invalid error evidence")
        return self


class DeliveryAttemptRead(Contract):
    id: UUID
    conversation_id: UUID
    delivery_key: UUID
    source_turn_id: UUID
    authority_revision: Counter
    expected_settling_revision: Counter | None
    fenced_settling_revision: Counter | None
    status: DeliveryStatus
    revision: Counter
    provider_operation_id: ProviderReference | None
    provider_message_id: ProviderReference | None
    error_code: DeliveryErrorCode | None
    created_at: UtcTimestamp
    updated_at: UtcTimestamp
    dispatch_requested_at: UtcTimestamp | None
    provider_accepted_at: UtcTimestamp | None
    delivered_at: UtcTimestamp | None
    read_at: UtcTimestamp | None


class DeliveryResult(Contract):
    attempt: DeliveryAttemptRead
    replayed: bool


class DeliveryReconcileResult(Contract):
    attempt: DeliveryAttemptRead
    applied_revision: PositiveCounter
    replayed: bool

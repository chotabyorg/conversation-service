"""Private, business-semantics-free authority sidecar DTOs."""

from __future__ import annotations

import hashlib
import json
from enum import StrEnum
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BeforeValidator, Field

from schemas.contracts import Channel, Contract, Counter, OwnerId

Digest = Annotated[str, Field(strict=True, pattern=r"^[0-9a-f]{64}$")]
BusinessEffectNamespace = Literal["main-chota.ai-business.v1"]


def _require_contract_version_one(value: object) -> int:
    # Literal[1] accepts values such as True and 1.0 through Python equality.
    # Sidecar metadata is a strict wire contract, so reject those before Literal.
    if type(value) is not int or value != 1:
        raise ValueError("contract_version must be the integer 1")
    return value


ContractVersion = Annotated[Literal[1], BeforeValidator(_require_contract_version_one)]


class EffectReason(StrEnum):
    ELIGIBLE = "ELIGIBLE"
    AUTHORITY_CHANGED = "AUTHORITY_CHANGED"
    EXECUTION_INACTIVE = "EXECUTION_INACTIVE"
    INPUT_BEFORE_AUTHORITY = "INPUT_BEFORE_AUTHORITY"
    INPUT_INCOMPLETE = "INPUT_INCOMPLETE"
    INPUT_SUPERSEDED = "INPUT_SUPERSEDED"
    ORIGIN_STALE = "ORIGIN_STALE"
    ORIGIN_INCOMPLETE = "ORIGIN_INCOMPLETE"
    INTERVENING_SPLIT = "INTERVENING_SPLIT"


class ExternalEffectContext(Contract):
    contract_version: ContractVersion
    conversation_id: UUID
    turn_id: UUID
    origin_turn_id: UUID
    owner_id: OwnerId
    integration_id: UUID
    channel: Channel
    agent_id: UUID
    authority_revision: Counter
    managed_request_fingerprint: Digest


class ExternalEffectEligibilityRequest(ExternalEffectContext):
    pass


class ExternalEffectPermitRequest(ExternalEffectContext):
    operation_namespace: BusinessEffectNamespace
    operation_id: UUID
    operation_fingerprint: Digest


class ExternalEffectEligibilityResult(Contract):
    contract_version: ContractVersion
    request_fingerprint: Digest
    eligible: bool
    reason: EffectReason
    authority_revision: Counter
    authority_input_floor_sequence: Counter
    split_input_floor_sequence: Counter


class ExternalEffectPermitResult(Contract):
    contract_version: ContractVersion
    request_fingerprint: Digest
    permit_id: UUID
    replayed: bool


class ExternalEffectDenial(Contract):
    code: Literal["EXTERNAL_EFFECT_DENIED"]
    reason: EffectReason


class ExternalEffectContextConflict(Contract):
    code: Literal["EXTERNAL_EFFECT_CONTEXT_CONFLICT"]


class ExternalEffectPermitConflict(Contract):
    code: Literal["PERMIT_CONFLICT"]


def request_fingerprint(request: Contract) -> str:
    """Canonical hash for the complete sidecar DTO, distinct from managed projection hashes."""
    identity = json.dumps(
        request.model_dump(mode="json"),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
        allow_nan=False,
    ).encode("utf-8")
    return hashlib.sha256(identity).hexdigest()

"""Private-chat Telegram canary boundary; provider facts only, no business policy."""

from typing import Literal
from uuid import UUID

from pydantic import Field, model_validator

from schemas.contracts import (
    Contract,
    Counter,
    OwnerId,
    PositiveCounter,
    ProviderReference,
    TextContent,
    WorkerRef,
)

TELEGRAM_NAMESPACE = "main-chota.telegram.v1"


class TelegramIdentity(Contract):
    owner_id: OwnerId
    integration_id: UUID
    agent_id: UUID
    provider_thread_id: WorkerRef
    provider_event_id: WorkerRef


class TelegramAdmission(TelegramIdentity):
    update_id: Counter
    chat_id: PositiveCounter
    message_id: PositiveCounter
    sender_id: PositiveCounter
    bot_id: PositiveCounter
    text: TextContent
    desired_route: Literal["MANAGED", "LEGACY"]

    @model_validator(mode="after")
    def private_message_identity(self) -> "TelegramAdmission":
        if (self.provider_thread_id != str(self.chat_id)
                or self.provider_event_id != f"b{self.bot_id}c{self.chat_id}m{self.message_id}"
                or self.sender_id != self.chat_id):
            raise ValueError("invalid Telegram identity")
        return self


class TelegramAdmissionResult(Contract):
    route: Literal["MANAGED", "LEGACY"]
    admission_id: UUID
    replayed: bool


class TelegramResult(TelegramIdentity):
    state: Literal["PENDING", "DRAFT_READY", "NO_RESPONSE", "FAILED", "UNKNOWN"]
    draft_text: TextContent | None = None


class TelegramResponseBegin(TelegramIdentity):
    response_attempt_id: UUID


class TelegramResponseBegun(Contract):
    response_attempt_id: UUID
    draft_text: TextContent
    send: bool
    replayed: bool


class TelegramResponseComplete(TelegramResponseBegin):
    outcome: Literal["PROVIDER_ACCEPTED", "FAILED", "UNKNOWN"]
    provider_message_id: ProviderReference | None = None
    error_code: Literal["PROVIDER_REJECTED", "TRANSPORT_ERROR", "TIMEOUT"] | None = None

    @model_validator(mode="after")
    def evidence_shape(self) -> "TelegramResponseComplete":
        if self.outcome == "PROVIDER_ACCEPTED":
            if self.provider_message_id is None or self.error_code is not None:
                raise ValueError("invalid Telegram provider evidence")
        elif self.outcome == "FAILED":
            if self.provider_message_id is not None or self.error_code != "PROVIDER_REJECTED":
                raise ValueError("invalid Telegram provider evidence")
        elif self.provider_message_id is not None or self.error_code not in {
            "TRANSPORT_ERROR", "TIMEOUT"
        }:
            raise ValueError("invalid Telegram provider evidence")
        return self


class TelegramResponseCompleted(Contract):
    response_attempt_id: UUID
    outcome: Literal["PROVIDER_ACCEPTED", "FAILED", "UNKNOWN"]
    replayed: bool


class TelegramDeliveryWorkRequest(Contract):
    owner_id: OwnerId
    integration_id: UUID
    agent_id: UUID


class TelegramDeliveryWorkItem(TelegramIdentity):
    turn_id: UUID
    response_attempt_id: UUID
    draft_text: TextContent


class TelegramDeliveryWork(Contract):
    items: list[TelegramDeliveryWorkItem] = Field(max_length=100)

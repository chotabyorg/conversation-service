"""Closed private Website delivery descriptors and bounded public frame shapes."""
from typing import Literal
from uuid import UUID

from pydantic import StrictBool, model_validator

from schemas.contracts import Contract, PositiveCounter, TextContent
from schemas.website_poll import WebsitePollAccess
from schemas.website_session import WebsiteSessionAccess


class WebsitePublicFrame(Contract):
    delivery_id: UUID
    actor: Literal["AGENT", "HUMAN"]
    text: TextContent


class WebsiteDeliveryDescriptor(Contract):
    outbound_intent_id: UUID
    outbound_ordinal: PositiveCounter
    source_kind: Literal["AGENT_TURN", "OUTBOUND_MESSAGE"]
    source_id: UUID
    response_attempt_id: UUID
    delivery_id: UUID


class WebsiteDeliveryRecovery(Contract):
    delivery_id: UUID
    status: Literal["PENDING", "WRITE_IN_PROGRESS", "TRANSPORT_ACCEPTED", "FAILED",
                    "UNKNOWN", "SUPERSEDED"]
    writer_state: Literal["NONE", "LIVE", "ENDED", "RETIRED"]


class WebsiteDeliveryWorkResult(Contract):
    state: Literal["EMPTY", "SEND_CANDIDATE", "RECOVER_ONLY", "FRAME_TOO_LARGE"]
    item: WebsiteDeliveryDescriptor | None = None
    frame: WebsitePublicFrame | None = None
    recovery: WebsiteDeliveryRecovery | None = None

    @model_validator(mode="after")
    def closed_shape(self) -> "WebsiteDeliveryWorkResult":
        if self.state == "EMPTY":
            valid = self.item is None and self.frame is None and self.recovery is None
        elif self.state == "SEND_CANDIDATE":
            valid = self.item is not None and self.frame is not None and self.recovery is None
        else:
            valid = self.item is not None and self.frame is None and self.recovery is not None
        if not valid:
            raise ValueError("invalid delivery work shape")
        return self


class WebsiteDeliveryIdentity(WebsiteSessionAccess, WebsiteDeliveryDescriptor):
    supplier_instance_id: UUID
    process_incarnation_id: UUID
    recover_only: StrictBool


class WebsiteDeliveryBegin(WebsiteDeliveryIdentity, WebsitePollAccess):
    pass


class WebsiteDeliveryBegun(Contract):
    state: Literal["ACQUIRED", "RECOVER_ONLY", "NO_SEND", "FRAME_TOO_LARGE"]
    item: WebsiteDeliveryDescriptor
    send: StrictBool
    replayed: StrictBool
    recovery: WebsiteDeliveryRecovery
    frame: WebsitePublicFrame | None = None

    @model_validator(mode="after")
    def acquisition_shape(self) -> "WebsiteDeliveryBegun":
        if self.send != (self.state == "ACQUIRED") or self.send != (self.frame is not None):
            raise ValueError("invalid acquisition shape")
        return self


class WebsiteWriterEnd(Contract):
    kind: Literal["TASK_JOINED"]
    supplier_instance_id: UUID
    process_incarnation_id: UUID


class WebsiteDeliveryComplete(WebsiteDeliveryIdentity):
    outcome: Literal["WRITE_COMPLETED", "WRITE_FAILED", "UNKNOWN"]
    error_code: Literal["ORIGINAL_WRITE_REJECTED", "TRANSPORT_ERROR", "TIMEOUT"] | None = None
    writer_end: WebsiteWriterEnd | None = None

    @model_validator(mode="after")
    def original_evidence(self) -> "WebsiteDeliveryComplete":
        if self.outcome == "WRITE_COMPLETED":
            valid = self.error_code is None
        elif self.outcome == "WRITE_FAILED":
            valid = self.error_code == "ORIGINAL_WRITE_REJECTED"
        else:
            valid = self.error_code in {"TRANSPORT_ERROR", "TIMEOUT"}
        if not valid or (self.writer_end is not None and (
            self.writer_end.supplier_instance_id != self.supplier_instance_id
            or self.writer_end.process_incarnation_id != self.process_incarnation_id)):
            raise ValueError("invalid original writer evidence")
        return self


class WebsiteDeliveryCompleted(Contract):
    recovery: WebsiteDeliveryRecovery
    replayed: StrictBool

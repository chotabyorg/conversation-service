"""Private poll resource grants; replay never returns another executable grant."""
from datetime import datetime
from typing import Literal
from uuid import UUID

from pydantic import StrictBool, model_validator

from schemas.contracts import Contract
from schemas.website_session import WebsiteSessionAccess


class WebsitePollBegin(WebsiteSessionAccess):
    request_id: UUID
    supplier_instance_id: UUID
    process_incarnation_id: UUID


class WebsitePollStarted(Contract):
    state: Literal["ACQUIRED", "RECOVER_ONLY"]
    poll_id: UUID | None = None
    deadline_at: datetime | None = None

    @model_validator(mode="after")
    def closed_shape(self) -> "WebsitePollStarted":
        acquired = self.state == "ACQUIRED"
        if acquired != (self.poll_id is not None) or acquired != (self.deadline_at is not None):
            raise ValueError("invalid poll grant shape")
        return self


class WebsitePollAccess(WebsiteSessionAccess):
    poll_id: UUID
    supplier_instance_id: UUID
    process_incarnation_id: UUID


class WebsitePollEnded(Contract):
    replayed: StrictBool

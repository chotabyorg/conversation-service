"""Visitor-safe canonical content; opaque root-scoped visibility cursors."""
from typing import Literal
from uuid import UUID

from pydantic import Field, model_validator

from schemas.contracts import Contract, TextContent
from schemas.website_session import WebsiteSessionAccess


class WebsiteHistoryRequest(WebsiteSessionAccess):
    cursor: UUID | None = None


class WebsiteHistoryMessage(Contract):
    id: UUID
    actor: Literal["CUSTOMER", "AGENT", "HUMAN"]
    text: TextContent
    client_message_id: UUID | None = None


class WebsiteHistoryPage(Contract):
    state: Literal["PAGE", "FRAME_TOO_LARGE"] = "PAGE"
    items: list[WebsiteHistoryMessage] = Field(max_length=50)
    next_cursor: UUID | None = None
    oversized_message_id: UUID | None = None

    @model_validator(mode="after")
    def closed_shape(self) -> "WebsiteHistoryPage":
        if ((self.state == "FRAME_TOO_LARGE") != (self.oversized_message_id is not None)
                or (self.state == "FRAME_TOO_LARGE" and self.items)):
            raise ValueError("invalid history page shape")
        return self

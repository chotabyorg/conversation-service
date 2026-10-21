"""Private credential issuance port. Browser UUIDs never attach old roots."""
from datetime import datetime
from uuid import UUID

from pydantic import Field, SecretStr, field_serializer

from schemas.contracts import Contract, OwnerId


class WebsiteSessionScope(Contract):
    owner_id: OwnerId
    integration_id: UUID
    agent_id: UUID
    binding_generation: int = Field(ge=1, le=9223372036854775807)


class WebsiteSessionCreated(Contract):
    session_id: UUID
    session_token: SecretStr = Field(repr=False)
    expires_at: datetime

    @field_serializer("session_token", when_used="json")
    def wire_token(self, value: SecretStr) -> str:
        return value.get_secret_value()


class WebsiteSessionAccess(WebsiteSessionScope):
    session_id: UUID
    session_token: SecretStr = Field(repr=False)

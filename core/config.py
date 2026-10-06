"""Fail-closed server-provisioned credentials and exact scope grants."""

from __future__ import annotations

import json
from typing import Annotated, Literal

from pydantic import (
    BaseModel,
    BeforeValidator,
    ConfigDict,
    Field,
    SecretStr,
    ValidationError,
    field_validator,
    model_validator,
)
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict
from pydantic_settings.exceptions import SettingsError
from sqlalchemy.engine import make_url

from schemas.contracts import Principal, ScopeGrant, WorkerRef, validate_ascii


def validate_service_token(value: object) -> SecretStr:
    raw = value.get_secret_value() if isinstance(value, SecretStr) else value
    return SecretStr(validate_ascii(raw, r"[A-Za-z0-9._~-]{32,256}"))


ServiceToken = Annotated[SecretStr, BeforeValidator(validate_service_token)]


class AuthPrincipal(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    principal_id: WorkerRef
    token: ServiceToken = Field(repr=False)
    grants: tuple[ScopeGrant, ...] = Field(min_length=1, max_length=64, repr=False)

    @model_validator(mode="after")
    def distinct_grants(self) -> AuthPrincipal:
        if len(set(self.grants)) != len(self.grants):
            raise ValueError("duplicate scope grant")
        return self

    def principal(self) -> Principal:
        return Principal(principal_id=self.principal_id, grants=self.grants)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        populate_by_name=True,
        frozen=True,
    )
    database_url: SecretStr = Field(alias="DATABASE_URL", repr=False)
    auth_principals: Annotated[tuple[AuthPrincipal, ...], NoDecode] = Field(
        alias="CONVERSATION_AUTH_PRINCIPALS", min_length=1, max_length=32, repr=False
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO", alias="LOG_LEVEL"
    )

    @field_validator("database_url")
    @classmethod
    def database_is_postgres(cls, value: SecretStr) -> SecretStr:
        try:
            url = make_url(value.get_secret_value())
            if url.drivername != "postgresql+asyncpg" or not url.database:
                raise ValueError
        except Exception:
            raise ValueError("invalid database configuration") from None
        return value

    @field_validator("auth_principals", mode="before")
    @classmethod
    def parse_registry(cls, value: object) -> object:
        if isinstance(value, str):
            if len(value) > 4194304:
                raise ValueError("invalid authentication configuration")
            try:
                return json.loads(value)
            except (ValueError, TypeError):
                raise ValueError("invalid authentication configuration") from None
        return value

    @model_validator(mode="after")
    def distinct_credentials(self) -> Settings:
        ids = [entry.principal_id for entry in self.auth_principals]
        tokens = [entry.token.get_secret_value() for entry in self.auth_principals]
        if len(set(ids)) != len(ids) or len(set(tokens)) != len(tokens):
            raise ValueError("duplicate principal configuration")
        return self


def load_settings() -> Settings:
    """Do not let validation traceback render environment values at startup."""
    try:
        return Settings()
    except (ValidationError, SettingsError, ValueError, TypeError):
        raise RuntimeError("Invalid conversation-service configuration.") from None

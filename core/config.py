"""Fail-closed server-provisioned credentials and exact scope grants."""

from __future__ import annotations

import json
from typing import Annotated, Literal
from urllib.parse import urlsplit
from uuid import UUID

from pydantic import (
    AliasChoices,
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

from schemas.contracts import Channel, OwnerId, Principal, ScopeGrant, WorkerRef, validate_ascii


def validate_service_token(value: object) -> SecretStr:
    raw = value.get_secret_value() if isinstance(value, SecretStr) else value
    return SecretStr(validate_ascii(raw, r"[A-Za-z0-9._~-]{32,256}"))


ServiceToken = Annotated[SecretStr, BeforeValidator(validate_service_token)]


class AuthPrincipal(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    principal_id: WorkerRef
    role: Literal["FULL", "WEBSITE", "PROCESSING"] = "FULL"
    token: ServiceToken = Field(repr=False)
    grants: tuple[ScopeGrant, ...] = Field(min_length=1, max_length=64, repr=False)

    @model_validator(mode="after")
    def distinct_grants(self) -> AuthPrincipal:
        if len(set(self.grants)) != len(self.grants):
            raise ValueError("duplicate scope grant")
        return self

    def principal(self) -> Principal:
        return Principal(principal_id=self.principal_id, role=self.role, grants=self.grants)


class WebsiteBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    owner_id: OwnerId
    integration_id: UUID
    agent_id: UUID


class ManagedBinding(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)
    owner_id: OwnerId
    integration_id: UUID
    channel: Channel
    agent_id: UUID
    service_token: ServiceToken = Field(repr=False)

    def scope(self) -> tuple[str, UUID, Channel]:
        return self.owner_id, self.integration_id, self.channel


TimeoutSeconds = Annotated[float, Field(strict=True, gt=0, le=240, allow_inf_nan=False)]


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
        frozen=True,
    )
    # Both constructor spellings use one source key, so explicit values outrank env/dotenv.
    # If both are supplied, the uppercase alias wins deterministically.
    database_url: SecretStr = Field(
        alias="DATABASE_URL",
        validation_alias=AliasChoices("DATABASE_URL", "database_url"),
        repr=False,
    )
    auth_principals: Annotated[tuple[AuthPrincipal, ...], NoDecode] = Field(
        alias="CONVERSATION_AUTH_PRINCIPALS",
        validation_alias=AliasChoices("CONVERSATION_AUTH_PRINCIPALS", "auth_principals"),
        min_length=1,
        max_length=32,
        repr=False,
    )
    log_level: Literal["DEBUG", "INFO", "WARNING", "ERROR"] = Field(
        default="INFO", alias="LOG_LEVEL", validation_alias=AliasChoices("LOG_LEVEL", "log_level")
    )
    managed_ai_bindings: Annotated[tuple[ManagedBinding, ...], NoDecode] = Field(
        default=(), alias="MANAGED_AI_BINDINGS",
        validation_alias=AliasChoices("MANAGED_AI_BINDINGS", "managed_ai_bindings"),
        max_length=64, repr=False,
    )
    managed_ai_base_url: str | None = Field(
        default=None, alias="MANAGED_AI_BASE_URL",
        validation_alias=AliasChoices("MANAGED_AI_BASE_URL", "managed_ai_base_url"), repr=False,
    )
    managed_ai_connect_seconds: TimeoutSeconds = Field(
        default=5.0, alias="MANAGED_AI_CONNECT_SECONDS",
        validation_alias=AliasChoices("MANAGED_AI_CONNECT_SECONDS", "managed_ai_connect_seconds"),
    )
    managed_ai_write_seconds: TimeoutSeconds = Field(
        default=10.0, alias="MANAGED_AI_WRITE_SECONDS",
        validation_alias=AliasChoices("MANAGED_AI_WRITE_SECONDS", "managed_ai_write_seconds"),
    )
    managed_ai_read_seconds: TimeoutSeconds = Field(
        default=100.0, alias="MANAGED_AI_READ_SECONDS",
        validation_alias=AliasChoices("MANAGED_AI_READ_SECONDS", "managed_ai_read_seconds"),
    )
    managed_ai_pool_seconds: TimeoutSeconds = Field(
        default=5.0, alias="MANAGED_AI_POOL_SECONDS",
        validation_alias=AliasChoices("MANAGED_AI_POOL_SECONDS", "managed_ai_pool_seconds"),
    )
    managed_ai_total_seconds: TimeoutSeconds = Field(
        default=120.0, alias="MANAGED_AI_TOTAL_SECONDS",
        validation_alias=AliasChoices("MANAGED_AI_TOTAL_SECONDS", "managed_ai_total_seconds"),
    )

    website_bindings: Annotated[tuple[WebsiteBinding, ...], NoDecode] = Field(
        default=(), alias="WEBSITE_BINDINGS",
        validation_alias=AliasChoices("WEBSITE_BINDINGS", "website_bindings"),
        max_length=64, repr=False,
    )
    website_worker_enabled: bool = Field(
        default=False, alias="WEBSITE_WORKER_ENABLED",
        validation_alias=AliasChoices("WEBSITE_WORKER_ENABLED", "website_worker_enabled"),
    )
    website_worker_principal_id: WorkerRef | None = Field(
        default=None, alias="WEBSITE_WORKER_PRINCIPAL_ID",
        validation_alias=AliasChoices("WEBSITE_WORKER_PRINCIPAL_ID", "website_worker_principal_id"),
    )
    website_worker_lease_seconds: int = Field(
        default=30, ge=5, le=300, alias="WEBSITE_WORKER_LEASE_SECONDS",
        validation_alias=AliasChoices("WEBSITE_WORKER_LEASE_SECONDS",
                                     "website_worker_lease_seconds"),
    )
    website_worker_poll_seconds: float = Field(
        default=1.0, ge=0.1, le=10, allow_inf_nan=False, alias="WEBSITE_WORKER_POLL_SECONDS",
        validation_alias=AliasChoices("WEBSITE_WORKER_POLL_SECONDS", "website_worker_poll_seconds"),
    )
    website_worker_health_port: int = Field(
        default=8001, ge=1, le=65535, alias="WEBSITE_WORKER_HEALTH_PORT",
        validation_alias=AliasChoices("WEBSITE_WORKER_HEALTH_PORT", "website_worker_health_port"),
    )

    @field_validator(
        "managed_ai_connect_seconds", "managed_ai_write_seconds", "managed_ai_read_seconds",
        "managed_ai_pool_seconds", "managed_ai_total_seconds", mode="before",
    )
    @classmethod
    def managed_timeout(cls, value: object) -> object:
        # Environment scalar values arrive as text; validate after numeric parsing.
        if isinstance(value, bool):
            raise ValueError("invalid managed AI configuration")
        if isinstance(value, str):
            try:
                return float(value)
            except ValueError:
                raise ValueError("invalid managed AI configuration") from None
        return value

    @field_validator("managed_ai_base_url")
    @classmethod
    def managed_origin(cls, value: str | None) -> str | None:
        if value is None:
            return None
        try:
            url = urlsplit(value)
            if (not value.isascii() or any(ord(c) <= 32 for c in value)
                    or url.scheme not in {"http", "https"} or not url.hostname
                    or url.username is not None or url.password is not None
                    or url.path not in {"", "/"} or url.query or url.fragment
                    or (url.port is not None and not 1 <= url.port <= 65535)):
                raise ValueError
        except (ValueError, TypeError):
            raise ValueError("invalid managed AI configuration") from None
        return value.rstrip("/")

    @field_validator("managed_ai_bindings", "website_bindings", mode="before")
    @classmethod
    def managed_registry(cls, value: object) -> object:
        if isinstance(value, str):
            if len(value) > 4194304:
                raise ValueError("invalid managed AI configuration")
            try:
                return json.loads(value)
            except (ValueError, TypeError):
                raise ValueError("invalid managed AI configuration") from None
        return value

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
        scopes = [entry.scope() for entry in self.managed_ai_bindings]
        if len(set(scopes)) != len(scopes):
            raise ValueError("invalid managed AI configuration")
        if any(entry.service_token.get_secret_value() in tokens
               for entry in self.managed_ai_bindings):
            raise ValueError("invalid managed AI configuration")
        identities = [(entry.owner_id, entry.integration_id) for entry in self.website_bindings]
        if len(set(identities)) != len(identities):
            raise ValueError("invalid website configuration")
        if self.website_worker_enabled:
            principal = next((entry for entry in self.auth_principals
                              if entry.principal_id == self.website_worker_principal_id), None)
            if (principal is None or principal.role not in {"FULL", "PROCESSING"}
                    or not self.website_bindings or self.managed_ai_base_url is None):
                raise ValueError("invalid website worker configuration")
            for entry in self.website_bindings:
                if not any(g.owner_id == entry.owner_id and g.integration_id == entry.integration_id
                           and g.channel == Channel.WEBSITE_WIDGET
                           and g.provider_namespace == "main-chota.website-widget.v1"
                           for g in principal.grants):
                    raise ValueError("invalid website worker configuration")
                if not any(b.owner_id == entry.owner_id and b.integration_id == entry.integration_id
                           and b.channel == Channel.WEBSITE_WIDGET and b.agent_id == entry.agent_id
                           for b in self.managed_ai_bindings):
                    raise ValueError("invalid website worker configuration")
        return self


def load_settings() -> Settings:
    """Do not let validation traceback render environment values at startup."""
    try:
        return Settings()
    except (ValidationError, SettingsError, ValueError, TypeError):
        raise RuntimeError("Invalid conversation-service configuration.") from None

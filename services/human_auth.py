"""Live user-service verification for the browser-only owner Inbox boundary."""

from __future__ import annotations

import json
import re
from typing import Any
from uuid import UUID

import httpx
from pydantic import BaseModel, ConfigDict, StrictBool

from core.config import Settings
from core.errors import DomainError
from schemas.contracts import ErrorCode
from schemas.handoff import HumanPrincipal

INBOX_ORIGIN = "https://agents.chota.by"
MAX_ME_BYTES = 16_384


class UserMe(BaseModel):
    model_config = ConfigDict(extra="ignore", strict=True)
    id: UUID
    status: str
    email_verified: StrictBool
    consent_required: StrictBool


def _unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("duplicate JSON key")
        result[key] = value
    return result


def _access_cookie(headers: list[tuple[bytes, bytes]]) -> str | None:
    cookie_headers = [value for key, value in headers if key.lower() == b"cookie"]
    if len(cookie_headers) != 1:
        return None
    try:
        raw = cookie_headers[0].decode("ascii")
    except UnicodeDecodeError:
        return None
    access_values: list[str] = []
    for part in raw.split(";"):
        key, separator, value = part.strip().partition("=")
        if not separator:
            continue
        if key == "access_token":
            access_values.append(value)
    if (
        len(access_values) != 1
        or re.fullmatch(r"[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+", access_values[0]) is None
    ):
        return None
    return access_values[0]


async def verify_user_me(access_token: str, settings: Settings) -> HumanPrincipal | None:
    if settings.user_service_base_url is None:
        return None
    timeout = httpx.Timeout(connect=2.0, read=3.0, write=2.0, pool=2.0)
    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            trust_env=False,
            follow_redirects=False,
        ) as client:
            async with client.stream(
                "GET",
                settings.user_service_base_url + "/api/v1/me",
                headers={"Cookie": f"access_token={access_token}", "Accept": "application/json"},
            ) as response:
                if response.status_code in {401, 403}:
                    return None
                if response.status_code != 200:
                    raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
                content_types = response.headers.get_list("content-type")
                encodings = response.headers.get_list("content-encoding")
                if (
                    len(content_types) != 1
                    or content_types[0].split(";", 1)[0].strip().lower() != "application/json"
                    or any(value.lower().strip() != "identity" for value in encodings)
                ):
                    raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
                body = bytearray()
                async for chunk in response.aiter_raw():
                    if len(body) + len(chunk) > MAX_ME_BYTES:
                        raise DomainError(ErrorCode.SERVICE_UNAVAILABLE)
                    body.extend(chunk)
        # The first pass rejects duplicate object keys. JSON-mode UUID decoding then
        # accepts the User service's normal UUID string while scalar fields stay strict.
        json.loads(bytes(body), object_pairs_hook=_unique_object)
        user = UserMe.model_validate_json(bytes(body))
    except DomainError:
        raise
    except Exception:
        raise DomainError(ErrorCode.SERVICE_UNAVAILABLE) from None
    if user.status != "active":
        return None
    return HumanPrincipal(
        account_id=user.id,
        email_verified=user.email_verified,
        consent_required=user.consent_required,
    )


async def authenticate_browser(
    headers: list[tuple[bytes, bytes]],
    method: str,
    settings: Settings,
) -> HumanPrincipal | None:
    access_token = _access_cookie(headers)
    if access_token is None:
        return None
    return await verify_user_me(access_token, settings)

"""Authenticate exactly one dedicated credential into a token-free Principal."""

from __future__ import annotations

import hmac
import re

from core.config import Settings
from schemas.contracts import Principal


def authenticate(headers: list[tuple[bytes, bytes]], settings: Settings) -> Principal | None:
    tokens = [value for key, value in headers if key.lower() == b"x-service-token"]
    valid = len(tokens) == 1 and re.fullmatch(rb"[A-Za-z0-9._~-]{32,256}", tokens[0]) is not None
    provided = tokens[0] if valid else b""
    matched = None
    # Check every configured credential, without short-circuiting by principal ID.
    for entry in settings.auth_principals:
        expected = entry.token.get_secret_value().encode("ascii")
        if hmac.compare_digest(provided, expected) and valid:
            matched = entry.principal()
    return matched

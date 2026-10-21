"""Authenticate exactly one dedicated credential into a token-free Principal."""

from __future__ import annotations

import hmac
import re

from core.config import Settings
from schemas.contracts import Principal


def authenticate(headers: list[tuple[bytes, bytes]], settings: Settings) -> Principal | None:
    tokens = [value for key, value in headers if key.lower() == b"x-service-token"]
    inbox_tokens = [value for key, value in headers if key.lower() == b"x-inbox-lifecycle-token"]
    if len(tokens) + len(inbox_tokens) != 1:
        return None
    is_cp_lifecycle = bool(inbox_tokens)
    raw = inbox_tokens[0] if is_cp_lifecycle else tokens[0]
    valid = re.fullmatch(rb"[A-Za-z0-9._~-]{32,256}", raw) is not None
    provided = raw if valid else b""
    matched = None
    direct_token = (
        settings.inbox_lifecycle_token if is_cp_lifecycle else settings.handoff_ai_lifecycle_token
    )
    if (
        direct_token is not None
        and valid
        and hmac.compare_digest(provided, direct_token.get_secret_value().encode("ascii"))
    ):
        matched = Principal(
            principal_id=(
                "control-plane-inbox-lifecycle" if is_cp_lifecycle else "ai-handoff-lifecycle"
            ),
            role="INBOX_LIFECYCLE",
            grants=(),
        )
    # Check every configured credential, without short-circuiting by principal ID.
    if not is_cp_lifecycle:
        for entry in settings.auth_principals:
            expected = entry.token.get_secret_value().encode("ascii")
            if hmac.compare_digest(provided, expected) and valid:
                matched = entry.principal()
    return matched

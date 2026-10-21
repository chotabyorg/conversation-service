"""Pure ASGI auth-first bounded body reader and sanitized request correlation."""

from __future__ import annotations

import json
import re
from uuid import uuid4

from starlette.types import ASGIApp, Message, Receive, Scope, Send

from api.security import authenticate
from core.errors import DomainError
from core.logging import logger, request_id_context
from schemas.contracts import ErrorCode
from services.human_auth import INBOX_ORIGIN, authenticate_browser

MAX_BODY_BYTES = 65536
STRICT_JSON_PREFIXES = (
    "/public/v1/inbox/",
    "/internal/v1/inbox-grants/",
    "/internal/v1/outbound-writers/",
    "/internal/v2/telegram/delivery/",
    "/internal/v3/managed-turns/",
)


def error_payload(code: ErrorCode, request_id: str) -> dict[str, object]:
    error = DomainError(code)
    return {"detail": {"code": code.value, "message": error.message, "request_id": request_id}}


def _fastapi_json_content_type(value: bytes) -> bool:
    media_type = value.split(b";", 1)[0].strip().lower()
    if media_type == b"application/json":
        return True
    if not media_type.startswith(b"application/"):
        return False
    return media_type.partition(b"/")[2].endswith(b"+json")


class BoundaryMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        headers = scope.get("headers", [])
        request_ids = [v for k, v in headers if k.lower() == b"x-request-id"]
        request_id = str(uuid4())
        if len(request_ids) == 1 and re.fullmatch(rb"[A-Za-z0-9._:-]{1,64}", request_ids[0]):
            request_id = request_ids[0].decode("ascii")
        state = scope.setdefault("state", {})
        state["request_id"] = request_id
        context_token = request_id_context.set(request_id)
        response_status = 500
        response_started = False

        async def safe_send(message: Message) -> None:
            nonlocal response_status, response_started
            if message["type"] == "http.response.start":
                response_started = True
                response_status = message["status"]
                response_headers = list(message.get("headers", []))
                response_headers.extend(
                    [
                        (b"x-request-id", request_id.encode("ascii")),
                        (b"cache-control", b"no-store"),
                    ]
                )
                message = {**message, "headers": response_headers}
            await send(message)

        async def reject(code: ErrorCode) -> None:
            error = DomainError(code)
            body = json.dumps(error_payload(code, request_id), separators=(",", ":")).encode()
            logger.warning("", extra={"stage": "request", "code": code.value})
            await safe_send(
                {
                    "type": "http.response.start",
                    "status": error.status_code,
                    "headers": [(b"content-type", b"application/json")],
                }
            )
            await safe_send({"type": "http.response.body", "body": body})

        def has_duplicate_or_overdeep_json(raw: bytes) -> bool:
            def unique(pairs: list[tuple[str, object]]) -> dict[str, object]:
                value: dict[str, object] = {}
                for key, item in pairs:
                    if key in value:
                        raise ValueError("duplicate key")
                    value[key] = item
                return value

            try:
                json.loads(raw, object_pairs_hook=unique)
            except ValueError as exc:
                return str(exc) == "duplicate key"
            except (UnicodeDecodeError, json.JSONDecodeError):
                return False
            except RecursionError:
                return True
            return False

        try:
            path = scope["path"]
            public_inbox = path == "/public/v1/inbox" or path.startswith("/public/v1/inbox/")
            if public_inbox:
                settings = getattr(scope["app"].state, "settings", None)
                if settings is None or not settings.handoff_cohort_integration_ids:
                    await reject(ErrorCode.NOT_FOUND)
                    return
                if scope["method"] not in {"GET", "HEAD"}:
                    origins = [v for k, v in headers if k.lower() == b"origin"]
                    if len(origins) != 1 or origins[0] != INBOX_ORIGIN.encode("ascii"):
                        await reject(ErrorCode.FORBIDDEN)
                        return
                human = await authenticate_browser(headers, scope["method"], settings)
                if human is None:
                    await reject(ErrorCode.UNAUTHENTICATED)
                    return
                if human.consent_required or (
                    settings.inbox_require_verified_email and not human.email_verified
                ):
                    await reject(ErrorCode.FORBIDDEN)
                    return
                state["human_principal"] = human
            elif path not in {"/health", "/ready"}:
                settings = getattr(scope["app"].state, "settings", None)
                principal = authenticate(headers, settings) if settings is not None else None
                if principal is None:
                    await reject(ErrorCode.UNAUTHENTICATED)
                    return
                voice_paths = {
                    "/internal/v1/voice/" + suffix
                    for suffix in (
                        "calls/start",
                        "calls/heartbeat",
                        "calls/end",
                        "utterances",
                        "responses/next",
                        "responses/begin",
                        "playback",
                    )
                }
                if principal.role == "VOICE" and (
                    path not in voice_paths or scope["method"] != "POST"
                ):
                    await reject(ErrorCode.NOT_FOUND)
                    return
                if principal.role == "VOICE_PROCESSING":
                    await reject(ErrorCode.NOT_FOUND)
                    return
                business_effect_paths = {
                    "/internal/v1/external-effect-eligibility",
                    "/internal/v1/external-effect-permits",
                }
                if principal.role == "BUSINESS_EFFECT" and (
                    path not in business_effect_paths or scope["method"] != "POST"
                ):
                    await reject(ErrorCode.NOT_FOUND)
                    return
                website_paths = {
                    "/internal/v1/website/sessions",
                    "/internal/v1/website/admissions",
                    "/internal/v1/website/results",
                                 "/internal/v1/website/responses/begin",
                    "/internal/v1/website/responses/complete",
                }
                telegram_paths = {
                    "/internal/v1/telegram/admissions",
                    "/internal/v1/telegram/results",
                    "/internal/v1/telegram/responses/work",
                    "/internal/v1/telegram/responses/begin",
                    "/internal/v1/telegram/responses/complete",
                    "/internal/v2/telegram/delivery/work",
                    "/internal/v2/telegram/delivery/begin",
                    "/internal/v2/telegram/delivery/complete",
                }
                if principal.role == "WEBSITE" and (
                    path not in website_paths or scope["method"] != "POST"
                ):
                    await reject(ErrorCode.NOT_FOUND)
                    return
                if principal.role == "TELEGRAM" and (
                    path not in telegram_paths or scope["method"] != "POST"
                ):
                    await reject(ErrorCode.NOT_FOUND)
                    return
                if path.startswith("/internal/v2/telegram/delivery/") and (
                    principal.role != "TELEGRAM" or scope["method"] != "POST"
                ):
                    await reject(ErrorCode.NOT_FOUND)
                    return
                lifecycle_paths = {
                    "/internal/v1/inbox-grants/provision",
                    "/internal/v1/inbox-grants/revoke",
                }
                writer_retire_path = "/internal/v1/outbound-writers/retire"
                if principal.principal_id == "delivery-writer-supervisor":
                    if path != writer_retire_path or scope["method"] != "POST":
                        await reject(ErrorCode.NOT_FOUND)
                        return
                elif path.startswith("/internal/v1/outbound-writers/"):
                    await reject(ErrorCode.NOT_FOUND)
                    return
                elif principal.role == "INBOX_LIFECYCLE":
                    if path not in lifecycle_paths or scope["method"] != "POST":
                        await reject(ErrorCode.NOT_FOUND)
                        return
                    if (
                        principal.principal_id == "control-plane-inbox-lifecycle"
                        and path not in lifecycle_paths
                    ):
                        await reject(ErrorCode.NOT_FOUND)
                        return
                    if (
                        principal.principal_id == "ai-handoff-lifecycle"
                        and path != "/internal/v1/inbox-grants/revoke"
                    ):
                        await reject(ErrorCode.NOT_FOUND)
                        return
                    if principal.principal_id not in {
                        "control-plane-inbox-lifecycle",
                        "ai-handoff-lifecycle",
                    }:
                        await reject(ErrorCode.NOT_FOUND)
                        return
                if principal.role == "TELEGRAM_PROCESSING":
                    await reject(ErrorCode.NOT_FOUND)
                    return
                if principal.role == "PROCESSING" and (
                    not path.startswith("/internal/v1/processing/turns/")
                    or not path.endswith("/evidence")
                    or scope["method"] != "GET"
                ):
                    await reject(ErrorCode.NOT_FOUND)
                    return
                state["principal"] = principal
            # Content-Length is merely an early upper-bound hint, never the actual cap.
            lengths = [v for k, v in headers if k.lower() == b"content-length"]
            if any(v.isdigit() and len(v) <= 20 and int(v) > MAX_BODY_BYTES for v in lengths):
                await reject(ErrorCode.PAYLOAD_TOO_LARGE)
                return
            body = bytearray()
            while True:
                chunk = await receive()
                if chunk["type"] == "http.disconnect":
                    return
                if chunk["type"] != "http.request":
                    continue
                data = chunk.get("body", b"")
                if len(body) + len(data) > MAX_BODY_BYTES:
                    await reject(ErrorCode.PAYLOAD_TOO_LARGE)
                    return
                body.extend(data)
                if not chunk.get("more_body", False):
                    break
            if (
                scope["path"].startswith(STRICT_JSON_PREFIXES)
                and (
                    not [v for k, v in headers if k.lower() == b"content-type"]
                    or any(
                        _fastapi_json_content_type(v)
                        for k, v in headers
                        if k.lower() == b"content-type"
                    )
                )
                and has_duplicate_or_overdeep_json(bytes(body))
            ):
                await reject(ErrorCode.VALIDATION_ERROR)
                return
            delivered = False

            async def bounded_receive() -> Message:
                nonlocal delivered
                if not delivered:
                    delivered = True
                    return {"type": "http.request", "body": bytes(body), "more_body": False}
                return await receive()

            await self.app(scope, bounded_receive, safe_send)
        except DomainError as exc:
            if response_started:
                raise
            await reject(exc.code)
        except Exception:
            if response_started:
                raise
            await reject(ErrorCode.INTERNAL_ERROR)
        finally:
            logger.info(
                "",
                extra={
                    "stage": "request",
                    "code": "REQUEST_COMPLETED",
                    "status_code": response_status,
                },
            )
            request_id_context.reset(context_token)

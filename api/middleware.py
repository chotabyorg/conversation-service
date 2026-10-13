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

MAX_BODY_BYTES = 65536


def error_payload(code: ErrorCode, request_id: str) -> dict[str, object]:
    error = DomainError(code)
    return {"detail": {"code": code.value, "message": error.message, "request_id": request_id}}


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

        try:
            if scope["path"] not in {"/health", "/ready"}:
                settings = getattr(scope["app"].state, "settings", None)
                principal = authenticate(headers, settings) if settings is not None else None
                if principal is None:
                    await reject(ErrorCode.UNAUTHENTICATED)
                    return
                path = scope["path"]
                website_paths = {"/internal/v1/website/admissions", "/internal/v1/website/results",
                                 "/internal/v1/website/responses/begin",
                                 "/internal/v1/website/responses/complete"}
                telegram_paths = {
                    "/internal/v1/telegram/admissions",
                    "/internal/v1/telegram/results",
                    "/internal/v1/telegram/responses/begin",
                    "/internal/v1/telegram/responses/complete",
                }
                if principal.role == "WEBSITE" and (path not in website_paths
                                                      or scope["method"] != "POST"):
                    await reject(ErrorCode.NOT_FOUND)
                    return
                if principal.role == "TELEGRAM" and (path not in telegram_paths
                                                       or scope["method"] != "POST"):
                    await reject(ErrorCode.NOT_FOUND)
                    return
                if principal.role == "TELEGRAM_PROCESSING":
                    await reject(ErrorCode.NOT_FOUND)
                    return
                if principal.role == "PROCESSING" and (not path.startswith(
                        "/internal/v1/processing/turns/") or not path.endswith("/evidence")
                        or scope["method"] != "GET"):
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
            delivered = False

            async def bounded_receive() -> Message:
                nonlocal delivered
                if not delivered:
                    delivered = True
                    return {"type": "http.request", "body": bytes(body), "more_body": False}
                return await receive()

            await self.app(scope, bounded_receive, safe_send)
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

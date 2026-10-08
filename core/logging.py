"""A strict structured-log whitelist, including server and SQL channels."""

from __future__ import annotations

import json
import logging
from contextvars import ContextVar
from datetime import UTC, datetime
from uuid import UUID

request_id_context: ContextVar[str] = ContextVar("request_id", default="")
_IDS = ("conversation_id", "message_id", "event_id", "turn_id", "delivery_attempt_id")
_STAGES = {"request", "startup", "shutdown", "journal", "turn", "authority", "delivery"}


class SafeJsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        # Never call getMessage(), formatException(), str(args) or repr(exc_info).
        payload: dict[str, str | int] = {
            "timestamp": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "service": "conversation-service",
        }
        stage = getattr(record, "stage", None)
        if isinstance(stage, str) and stage in _STAGES:
            payload["stage"] = stage
        code = getattr(record, "code", None)
        if isinstance(code, str) and code in _SAFE_CODES:
            payload["code"] = code
        request_id = request_id_context.get()
        try:
            canonical_id = UUID(request_id)
            if str(canonical_id) == request_id:
                payload["request_id"] = request_id
        except (ValueError, TypeError, AttributeError):
            pass
        for key in _IDS:
            value = getattr(record, key, None)
            if isinstance(value, UUID):
                payload[key] = str(value)
        status = getattr(record, "status_code", None)
        if isinstance(status, int) and 100 <= status <= 599:
            payload["status_code"] = status
        return json.dumps(payload, ensure_ascii=True, separators=(",", ":"))


from schemas.contracts import ErrorCode  # noqa: E402

_SAFE_CODES = {code.value for code in ErrorCode} | {"REQUEST_COMPLETED", "STARTED", "STOPPED"}
logger = logging.getLogger("conversation")


_ORIGINAL_HANDLE = logging.Handler.handle
_PROTECTED_CHANNELS = ("httpx", "httpcore", "uvicorn.access", "sqlalchemy", "asyncpg")


def safe_handle(handler: logging.Handler, record: logging.LogRecord) -> bool:
    # Runs for every sink, including independently attached descendants and handlers
    # installed by later dictConfig/basicConfig. Library wire traces are always dropped.
    if any(record.name == name or record.name.startswith(name + ".")
           for name in _PROTECTED_CHANNELS):
        return False
    record.msg, record.args = "", ()
    record.exc_info = None
    record.exc_text = None
    record.stack_info = None
    # Independent sinks may serialize arbitrary extras; retain only approved fields.
    extras = set(record.__dict__) - set(logging.makeLogRecord({}).__dict__)
    for key in extras - {"stage", "code", "status_code", *_IDS}:
        del record.__dict__[key]
    for key in _IDS:
        if key in record.__dict__ and not isinstance(record.__dict__[key], UUID):
            del record.__dict__[key]
    if getattr(record, "stage", None) not in _STAGES:
        record.__dict__.pop("stage", None)
    if getattr(record, "code", None) not in _SAFE_CODES:
        record.__dict__.pop("code", None)
    status = getattr(record, "status_code", None)
    if not isinstance(status, int) or not 100 <= status <= 599:
        record.__dict__.pop("status_code", None)
    handler.setFormatter(SafeJsonFormatter())
    return _ORIGINAL_HANDLE(handler, record)


def configure_logging(level: str) -> None:
    logging.Handler.handle = safe_handle  # type: ignore[method-assign,assignment]
    handler = logging.StreamHandler()
    handler.setFormatter(SafeJsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "sqlalchemy", "asyncpg",
                 "httpx", "httpcore"):
        channel = logging.getLogger(name)
        channel.handlers = []
        channel.propagate = True
        # SQL/access messages contain arbitrary parameters/query paths; suppress them.
        channel.setLevel(logging.CRITICAL + 1)

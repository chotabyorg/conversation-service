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
        if request_id:
            payload["request_id"] = request_id
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


def configure_logging(level: str) -> None:
    handler = logging.StreamHandler()
    handler.setFormatter(SafeJsonFormatter())
    root = logging.getLogger()
    root.handlers = [handler]
    root.setLevel(level)
    for name in ("uvicorn", "uvicorn.error", "uvicorn.access", "sqlalchemy", "asyncpg"):
        channel = logging.getLogger(name)
        channel.handlers = []
        channel.propagate = True
        # SQL/access messages contain arbitrary parameters/query paths; suppress them.
        channel.setLevel(logging.CRITICAL + 1)

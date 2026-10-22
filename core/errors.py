"""Fixed public errors; never render exception text or caller input."""

from schemas.contracts import ErrorCode

_SPECIAL: dict[ErrorCode, tuple[int, str]] = {
    ErrorCode.RATE_LIMITED: (429, "Request admission limit reached."),
    ErrorCode.UNAUTHENTICATED: (401, "Service authentication required."),
    ErrorCode.FORBIDDEN: (403, "Inbox access is not permitted."),
    ErrorCode.NOT_FOUND: (404, "Resource not found."),
    ErrorCode.PAYLOAD_TOO_LARGE: (413, "Request body exceeds the allowed size."),
    ErrorCode.VALIDATION_ERROR: (422, "Request validation failed."),
    ErrorCode.SERVICE_UNAVAILABLE: (503, "Service temporarily unavailable."),
    ErrorCode.INTERNAL_ERROR: (500, "Internal service error."),
}
_CONFLICT_MESSAGES = {
    ErrorCode.COMMAND_CONFLICT: "Command identity has different immutable content.",
    ErrorCode.REVISION_CONFLICT: "Current case or authority revision is stale.",
    ErrorCode.GRANT_CONFLICT: "Current Inbox grant is inactive or stale.",
    ErrorCode.HANDOFF_CONFLICT: "Handoff identity or current state conflicts.",
    ErrorCode.TELEGRAM_WRITER_LIVE: "An earlier Telegram transport writer is still active.",
    ErrorCode.WEBSITE_WRITER_LIVE: "An earlier Website transport writer is still unresolved.",
    ErrorCode.EVENT_CONFLICT: "Event identity has different immutable content.",
    ErrorCode.TURN_SELECTION_CONFLICT: "Turn selection identity has different content.",
    ErrorCode.ACTIVE_TURN_EXISTS: "An active turn already exists.",
    ErrorCode.INPUT_REVISION_CONFLICT: "Input revision is stale.",
    ErrorCode.SETTLING_REQUIRED: "Settling coordinator must freeze this input.",
    ErrorCode.AUTHORITY_REVISION_CONFLICT: "Authority revision is stale.",
    ErrorCode.CONTROL_MODE_CONFLICT: "Current control mode does not allow this operation.",
    ErrorCode.INVALID_INPUT_BOUNDARY: "Input boundary is unavailable.",
    ErrorCode.TURN_TERMINAL: "Turn is already terminal.",
    ErrorCode.LEASE_HELD: "Turn lease is held.",
    ErrorCode.LEASE_EXPIRED: "Turn lease has expired.",
    ErrorCode.STALE_CLAIM: "Turn claim is stale.",
    ErrorCode.TERMINAL_RESULT_CONFLICT: "Terminal result has different content.",
    ErrorCode.MANAGED_EXECUTION_STARTED: "Managed execution already started.",
    ErrorCode.NO_DRAFT: "A completed draft is required.",
    ErrorCode.DELIVERY_IDENTITY_CONFLICT: "Delivery identity has different content.",
    ErrorCode.DELIVERY_INTENT_EXISTS: "A delivery attempt already exists for this draft.",
    ErrorCode.DELIVERY_RECONCILIATION_CONFLICT: "Reconciliation identity has different content.",
    ErrorCode.DELIVERY_REVISION_CONFLICT: "Delivery revision is stale.",
    ErrorCode.INVALID_DELIVERY_TRANSITION: "Delivery state transition is invalid.",
    ErrorCode.DELIVERY_REFERENCE_CONFLICT: "Provider reference has different content.",
}


class DomainError(Exception):
    def __init__(self, code: ErrorCode, *, retry_after: int | None = None) -> None:
        self.code = ErrorCode(code)
        self.retry_after = retry_after
        self.status_code, self.message = _SPECIAL.get(
            self.code, (409, _CONFLICT_MESSAGES.get(self.code, "State conflict."))
        )
        super().__init__(self.code.value)

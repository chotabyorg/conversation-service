# Internal API contract, version 1

All business paths use `/internal/v1`. Exactly one dedicated `X-Service-Token`
authenticates a token-free Principal with server-provisioned exact grants. Missing,
wrong, duplicated or malformed credentials are uniformly 401 before parsing a
body. Scope mismatch and guessed UUID are uniformly 404. There is no public route,
gateway change or provider send. The unconsumed managed executor is described in
[managed execution](managed-execution.md). X-User headers provide no authority.

| Method/path | Request | Response and retry behavior |
| --- | --- | --- |
| POST `/inbound-events` | owner_id, integration_id, channel, provider_thread_id, provider_namespace, provider_event_id, text; contract_version=1, optional provider_occurred_at | InboundEventResult with Conversation/Event/Message and replayed; new 201, exact replay 200, changed immutable event EVENT_CONFLICT |
| GET `/conversations/{id}` | UUID | ConversationRead through granted scope |
| GET `/conversations/{id}/messages` | after_sequence integer 0..BIGINT_MAX default0; limit1..100 default50 | MessagePage, ascending sequence; next_after_sequence is last returned when another row exists, otherwise null; validates scope even for empty page |
| GET `/messages/{id}` | UUID | MessageRead through granted parent |
| POST `/conversations/{id}/turns` | selection_key UUID, input_first_sequence, input_last_sequence, expected_input_revision, expected_authority_revision | TurnResult; new 201, same-key exact selection replay 200 even after history/control/terminal change; changed payload TURN_SELECTION_CONFLICT |
| GET `/turns/{id}` | UUID | TurnRead without claim secret/digest/fingerprints |
| POST `/turns/{id}/claim` | worker_ref, lease_seconds1..300 | TurnClaimResult with raw claim_token only here; 200. Not idempotent: lost response recovers same Turn after lease expiry |
| POST `/turns/{id}/renew` | claim_epoch, claim_token, lease_seconds | TurnRead; live winning proof extends expiry without changing epoch/token; no secret echo |
| POST `/turns/{id}/execute` | claim_epoch, claim_token only | TurnResult; one-attempt managed execution; sameepoch unfinished duplicate409 MANAGED_EXECUTION_STARTED; recovered marked epoch completes EXECUTION_UNKNOWN without AI; winning terminal proof replays without AI |
| POST `/turns/{id}/complete` | claim_epoch, claim_token, outcome DRAFT_READY/NO_RESPONSE, draft_text optional, operation_refs=[] | TurnResult; first mutation needs live winning proof; exact terminal replay needs stored winning proof and identical result, even after expiry |
| POST `/turns/{id}/fail` | claim_epoch, claim_token, failure_code, operation_refs=[] | TurnResult with inspectable failure; same terminal replay/fencing contract |
| POST `/conversations/{id}/control` | expected_authority_revision, control_mode | ConversationRead; every accepted CAS increments revision, including same-mode; stale retry conflicts |
| POST `/conversations/{id}/delivery-attempts` | delivery_key UUID, source_turn_id UUID, expected_authority_revision | DeliveryResult; new GENERATED 201, exact creation replay 200; changed key DELIVERY_IDENTITY_CONFLICT, another key for same draft DELIVERY_INTENT_EXISTS |
| GET `/delivery-attempts/{id}` | UUID | DeliveryAttemptRead through granted parent |
| POST `/delivery-attempts/{id}/reconcile` | reconciliation_key UUID, expected_revision, status other than GENERATED; optional provider refs/time/error_code, expected_authority_revision | DeliveryReconcileResult; new/old exact evidence 200; old replay returns current attempt and original applied_revision; changed key payload conflicts |

All outputs occur after commit. Mutation successes are 200 unless a new immutable
resource is created. There is no 202 implying that work executed. No deletion,
Message edit/outbound append or implicit draft promotion exists.

`GET /health` returns `{status:"ok",service:"conversation-service"}`.
`GET /ready` returns ready200 only when DB query, migration `0002_managed_execution` and
required Conversation query succeed; otherwise not_ready503. These are the only
unauthenticated routes. `GET /internal/openapi.json` requires the service header.
Default `/docs`, `/redoc`, `/openapi.json` are disabled. The fetched schema provides
every exact DTO/property and X-Service-Token security requirement.

## Normalization and strict bounds

All API DTOs forbid extra fields; no caller fingerprint, raw provider payload,
arbitrary metadata, credential, prompt or reasoning field is accepted. Integers
are strict (boolean excluded), 0..9223372036854775807; positive fields start at1.
UUIDs serialize lowercase dashed. Actual streamed body cap is65536 bytes,
independent of Content-Length. Request IDs accept ASCII `[A-Za-z0-9._:-]{1,64}`;
otherwise the service generates UUID4 and includes X-Request-ID in the response.

| Field | Exact contract |
| --- | --- |
| owner_id | 1..255 Unicode characters, <=1020 UTF-8 bytes; reject controls/surrogates, whitespace-only and literal `*`; preserve permitted whitespace/case/composition |
| thread/event external ID | 1..512 characters and <=512 UTF-8 bytes; same controls/surrogate/whitespace rejection; preserve exact value |
| channel | ASCII case-insensitive TELEGRAM/INSTAGRAM/WEBSITE_WIDGET/WHATSAPP/VOICE; no trim/aliases |
| namespace | `[a-z0-9][a-z0-9._:-]{0,127}` exactly and must match one complete configured grant |
| text/draft | 1..16384 Unicode scalar characters, <=32768 UTF-8 bytes; NUL/surrogates rejected; whitespace/CRLF/composition preserved |
| provider_occurred_at | Optional RFC3339 string with explicit Z/numeric offset and <=6 fractional digits; numeric epoch, naive/date-only or excess precision rejected; UTC canonical `YYYY-MM-DDTHH:MM:SS.ffffffZ`; missing/null equivalent |
| worker_ref | ASCII `[A-Za-z0-9][A-Za-z0-9._:-]{0,63}`, diagnostic only |
| provider refs | Optional ASCII `[A-Za-z0-9][A-Za-z0-9._:-]{0,254}`; safe identifiers, no URL/body/receipt; known reference cannot change |
| operation_refs | <=32 `{origin:"AI_AGENT_SERVICE",operation_id:identifier,outcome:SUCCEEDED/FAILED/UNKNOWN/null}`; operation ID1..128 ASCII letters/digits/._:-; duplicate origin/ID rejected; canonical sorted order |
| claim_token | Exactly43 URL-safe ASCII characters, SecretStr request handling; bearer exposed only in successful claim response |

DRAFT_READY requires draft_text; NO_RESPONSE requires null. Input first must not
exceed last. Reconciliation expected_authority_revision is required exactly for
DISPATCH_REQUESTED and forbidden otherwise. error_code is allowed only for
FAILED/UNKNOWN/PARTIAL/INTERRUPTED. Failure codes are PROCESSING_ERROR, TIMEOUT,
INVALID_RESULT, EXECUTION_UNKNOWN; delivery errors are PROVIDER_REJECTED, TRANSPORT_ERROR, TIMEOUT,
INTERRUPTED, UNSPECIFIED. No arbitrary diagnostic error string is accepted.

## Fingerprints and errors

The server fingerprints validated selected fields using SHA-256 of sorted compact
UTF-8 JSON (ensure_ascii=false, allow_nan=false) and explicit contract_version1.
Inbound proof includes exact authorized scope, event ID, thread, TEXT/CUSTOMER/
INBOUND semantics, text and canonical provider time. Dedup identity excludes thread.
Provider times denoting one UTC microsecond are equivalent; composed/decomposed
Unicode remains distinct. Headers/accepted time/secrets are never fingerprinted.

Turn selection proof fixes key/input bounds/expected revisions; terminal proof
fixes status/result/operation refs separately from claim proof. Delivery creation
and complete reconciliation request fields have retained immutable proofs. Key
lookup precedes current revision/state checks for explicit replay semantics.

Every error uses
`{"detail":{"code":"UPPERCASE_CODE","message":"fixed safe message","request_id":"bounded ID"}}`.
Validation may add bounded fields with safe locations and code validation_error;
it never includes input/context, rejected values, tokens, SQL or exception text.

| HTTP | Codes |
| --- | --- |
| 401 | UNAUTHENTICATED |
| 404 | NOT_FOUND |
| 409 | EVENT_CONFLICT, TURN_SELECTION_CONFLICT, ACTIVE_TURN_EXISTS, INPUT_REVISION_CONFLICT, AUTHORITY_REVISION_CONFLICT, CONTROL_MODE_CONFLICT, INVALID_INPUT_BOUNDARY, TURN_TERMINAL, LEASE_HELD, LEASE_EXPIRED, STALE_CLAIM, TERMINAL_RESULT_CONFLICT, NO_DRAFT, DELIVERY_IDENTITY_CONFLICT, DELIVERY_INTENT_EXISTS, DELIVERY_RECONCILIATION_CONFLICT, DELIVERY_REVISION_CONFLICT, INVALID_DELIVERY_TRANSITION, DELIVERY_REFERENCE_CONFLICT |
| 413 | PAYLOAD_TOO_LARGE |
| 422 | VALIDATION_ERROR |
| 503 | SERVICE_UNAVAILABLE (bounded DB unavailability/timeout) |
| 500 | INTERNAL_ERROR (unexpected runtime/invariant failure) |

For control/delivery transitions and clock/replay details see
[states and transactions](states-and-transactions.md).

TurnRead additively exposes nullable managed_agent_id, managed_dispatch_epoch,
managed_dispatch_started_at and managed_request_fingerprint. The hash excludes
tokens. Marker metadata is execution association, not a provider receipt.
MANAGED_EXECUTION_STARTED is a fixed409 conflict and does not fail live work.

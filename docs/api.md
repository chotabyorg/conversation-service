# Internal API contract, version 1

All business paths use `/internal/v1`. Exactly one dedicated `X-Service-Token`
authenticates a token-free Principal with server-provisioned exact grants. Missing,
wrong, duplicated or malformed credentials are uniformly 401 before parsing a
body. Scope mismatch and guessed UUID are uniformly 404. There is no public route,
gateway change or provider send. The managed executor is described in
[managed execution](managed-execution.md). X-User headers provide no authority.

| Method/path | Request | Response and retry behavior |
| --- | --- | --- |
| POST `/inbound-events` | owner_id, integration_id, channel, provider_thread_id, provider_namespace, provider_event_id, text; contract_version=1, optional provider_occurred_at | InboundEventResult with Conversation/Event/Message and replayed; new 201, exact replay 200, changed immutable event EVENT_CONFLICT |
| GET `/conversations/{id}` | UUID | ConversationRead through granted scope |
| GET `/conversations/{id}/messages` | after_sequence integer 0..BIGINT_MAX default0; limit1..100 default50 | MessagePage, ascending sequence; next_after_sequence is last returned when another row exists, otherwise null; validates scope even for empty page |
| GET `/messages/{id}` | UUID | MessageRead through granted parent |
| POST `/conversations/{id}/turns` | selection_key UUID, input_first_sequence, input_last_sequence, expected_input_revision, expected_authority_revision | TurnResult; new 201, same-key exact selection replay 200 even after history/control/terminal change; changed payload TURN_SELECTION_CONFLICT; policy-bound inbound requires the settling coordinator (`SETTLING_REQUIRED`) |
| GET `/turns/{id}` | UUID | TurnRead without claim secret/digest/fingerprints |
| POST `/turns/{id}/claim` | worker_ref, lease_seconds1..300 | TurnClaimResult with raw claim_token only here; 200. Not idempotent: lost response recovers same Turn after lease expiry |
| POST `/turns/{id}/renew` | claim_epoch, claim_token, lease_seconds | TurnRead; live winning proof extends expiry without changing epoch/token; no secret echo |
| POST `/turns/{id}/execute` | claim_epoch, claim_token only | TurnResult; one-attempt managed execution; sameepoch unfinished duplicate409 MANAGED_EXECUTION_STARTED; recovered marked epoch completes EXECUTION_UNKNOWN without AI; winning terminal proof replays without AI |
| POST `/turns/{id}/complete` | claim_epoch, claim_token, outcome DRAFT_READY/NO_RESPONSE, draft_text optional, operation_refs=[] | TurnResult; first mutation needs live winning proof; exact terminal replay needs stored winning proof and identical result, even after expiry |
| POST `/turns/{id}/fail` | claim_epoch, claim_token, failure_code, operation_refs=[] | TurnResult with inspectable failure; same terminal replay/fencing contract |
| POST `/conversations/{id}/control` | expected_authority_revision, control_mode | ConversationRead; every accepted CAS increments revision, including same-mode; stale retry conflicts |
| POST `/external-effect-eligibility` | strict opaque effect context | Observational 200 with eligible/reason and authority/input floors; never grants dispatch authority |
| POST `/external-effect-permits` | strict context plus opaque Business operation identity and frozen command fingerprint | Immutable permit 200 after positive commit; exact full-request replay returns original permit; a fresh denial is 409 |
| POST `/conversations/{id}/delivery-attempts` | delivery_key UUID, source_turn_id UUID, expected_authority_revision | DeliveryResult; new GENERATED 201, exact creation replay 200; changed key DELIVERY_IDENTITY_CONFLICT, another key for same draft DELIVERY_INTENT_EXISTS |
| GET `/delivery-attempts/{id}` | UUID | DeliveryAttemptRead through granted parent |
| POST `/delivery-attempts/{id}/reconcile` | reconciliation_key UUID, expected_revision, status other than GENERATED; optional provider refs/time/error_code, expected_authority_revision | DeliveryReconcileResult; new/old exact evidence 200; old replay returns current attempt and original applied_revision; changed key payload conflicts |

All outputs occur after commit. Mutation successes are 200 unless a new immutable
resource is created. There is no 202 implying that work executed. No deletion,
Message edit or generic outbound append exists. Website response completion adds
canonical assistant history only under the [explicit write policy](history-projection.md).

`GET /health` returns `{status:"ok",service:"conversation-service"}`.
`GET /ready` returns ready200 only when DB query, migration `0009_voice_partial_drafts` and
required Conversation, permit, Website and Telegram journal queries succeed; otherwise not_ready503. These are the only
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
DISPATCH_REQUESTED and forbidden otherwise. Settling evidence is an optional
paired `expected_settling_revision` / `result_settling_revision`; the latter cannot
be less than the former. `SUPERSEDED` is terminal communication evidence for a
stale draft and never authorizes a provider send. error_code is allowed only for
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
| 409 | EVENT_CONFLICT, TURN_SELECTION_CONFLICT, ACTIVE_TURN_EXISTS, INPUT_REVISION_CONFLICT, SETTLING_REQUIRED, AUTHORITY_REVISION_CONFLICT, CONTROL_MODE_CONFLICT, INVALID_INPUT_BOUNDARY, TURN_TERMINAL, LEASE_HELD, LEASE_EXPIRED, STALE_CLAIM, TERMINAL_RESULT_CONFLICT, NO_DRAFT, DELIVERY_IDENTITY_CONFLICT, DELIVERY_INTENT_EXISTS, DELIVERY_RECONCILIATION_CONFLICT, DELIVERY_REVISION_CONFLICT, INVALID_DELIVERY_TRANSITION, DELIVERY_REFERENCE_CONFLICT |
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

## Goal4A narrow Website adapter

See [Website canary contract and lifecycle](website-canary.md) for frozen routing,
FIFO coordination and socket-only evidence. New private routes are POST
`/internal/v1/website/admissions`, `/results`, `/responses/begin`,
`/responses/complete` under that same prefix, plus protected GET
`/internal/v1/processing/turns/{turn_id}/evidence`. They use explicit WEBSITE or
PROCESSING credentials and exact current bindings. Existing foundation routes and
managed v1 contracts remain compatible for FULL principals.

| Method/path | Request | Response and retry behavior |
| --- | --- | --- |
| POST `/website/admissions` | trusted Website identity, text, desired route | `{route,admission_id,replayed}`; MANAGED commits the canonical inbound event/message; LEGACY retains routing only |
| POST `/website/results` | five identity fields | retained execution state and draft; polling owns no worker cancellation |
| POST `/website/responses/begin` | five identity fields plus `response_attempt_id`; request has no `admission_id` | `{action:SEND|REPLAY_COMPLETED|DENY,response_attempt_id,replay_source_attempt_id,draft_text,send,replayed,denial_reason}` with the action truth table below |
| POST `/website/responses/complete` | same request identity plus `WRITE_COMPLETED|WRITE_FAILED|UNKNOWN` | `{response_attempt_id,outcome,replayed}`; only an exact authorized attempt may complete or provide late evidence |

Current v2 `response_attempt_id` is
`uuid5(NAMESPACE_URL, "main-chota.website-widget-response.v2:" + str(admission_id))`;
the public `message_id` remains unchanged. Historical v1 UUID4 attempts remain
scoped to their original admission and delivery. `SEND` has
`send=true` and no replay source or denial. `REPLAY_COMPLETED` has `send=false`,
`replayed=true`, and an actual same-admission successful source ID. `DENY` has
`send=false` and a closed denial reason. Every v2 begin checks for eligible
historical success before returning a persisted denial, including denial rows
created earlier. It leaves that denied row unchanged and creates no new write.

## Telegram provider adapter and Goal5 coordination

The private `/internal/v1/telegram/*` POST routes accept only exact TELEGRAM grants
under namespace `main-chota.telegram.v1`, the configured `{owner_id,
integration_id, agent_id}` binding, and TELEGRAM or FULL principals. The gateway's
owner/integration/agent values come from authenticated Control Plane polling.
Conversation has an independent new-admission integration cohort; both cohorts
default empty. Keep sticky bindings after rollback so retained MANAGED admissions
remain addressable.

| Method/path | Request | Response and retry behavior |
| --- | --- | --- |
| POST `/telegram/admissions` | trusted provider identity, private-chat `chat_id`, positive `message_id`, `sender_id`, `bot_id`, nonnegative `update_id`, text, desired route | `{route,admission_id,replayed,bot_id,coordination_mode}`; MANAGED atomically creates the canonical Event/Message; exact replay returns the retained admission; conflicting immutable payload returns EVENT_CONFLICT |
| POST `/telegram/results` | trusted identity; `bot_id` may be omitted on request | echoes retained identity and returns `admission_id`, resolved `bot_id`, `coordination_mode`, nullable `turn_id`, execution `state`, exhaustive `delivery_state`, and `progress_disposition` |
| POST `/telegram/responses/work` | owner, integration, agent, and required positive `bot_id` | finite page of at most 100 fully correlated work items with admission/Turn/attempt IDs, draft, bot, and `work_kind=SEND_CANDIDATE|RECOVER_ONLY` |
| POST `/telegram/responses/begin` | Telegram identity plus stable `response_attempt_id`; `bot_id` is optional; request does not accept `admission_id` | echoes resolved identity, `admission_id`, bot, attempt, draft, `work_kind`, `send`, and `replayed`; only an eligible SEND_CANDIDATE can return `send=true` |
| POST `/telegram/responses/complete` | same request identity plus `PROVIDER_ACCEPTED` with provider message ID, `FAILED` with `PROVIDER_REJECTED`, or `UNKNOWN` with `TRANSPORT_ERROR|TIMEOUT` | echoes attempt/admission/bot and retained outcome; accepts exact authorized late facts; provider acceptance appends one canonical AGENT Message; no DELIVERED/READ claim |

The first MANAGED admission binds one immutable bot per owner/integration. A
different bot cannot create new MANAGED work. Historical mixed-bot MANAGED
admissions migrate to a persisted `CONFLICT` account instead of selecting an
arbitrary bot; conflict blocks new permission but does not erase exact prior
recovery authority. `RECOVER_ONLY` identifies an already-authorized unresolved
attempt/admission/bot. Begin returns `send=false`; gateway recovery must not call
the provider. Correct late evidence remains recordable, and a wrong bot cannot
trigger even the UNKNOWN transition.

`coordination_mode` is `IMMEDIATE|SETTLING`; historical/policy-free admissions
are IMMEDIATE. `progress_disposition` is `WAIT|TERMINAL|AUTHORITY_FENCED|ACCOUNT_CONFLICT`.
It is observational, not a provider receipt: an IMMEDIATE waiter releases only
after a fully correlated non-WAIT result, while SETTLING does not wait.
`delivery_state` may be `NOT_STARTED`, `GENERATED`, `DISPATCH_REQUESTED`,
`SUPERSEDED`, `PROVIDER_ACCEPTED`, `DELIVERED`, `READ`, `FAILED`, `UNKNOWN`,
`PARTIAL`, or `INTERRUPTED`; PARTIAL and INTERRUPTED are not success. Revocation,
missing runtime configuration, and HTTP status alone do not synthesize terminal
progress.

The provider event key is `b{bot_id}c{chat_id}m{message_id}`; `update_id`,
sender identity and text are included in the immutable fingerprint. The canonical
thread is the decimal private `chat_id`, scoped by the generic tuple
`(owner_id,integration_id,TELEGRAM,provider_thread_id)`. Group/non-text/anonymous
messages stay on their preexisting unsupported/legacy path. See
[Telegram canary](telegram-canary.md) for rollback and send semantics.

## Business external-effect authority

The private `BUSINESS_EFFECT` role can POST only `/external-effect-eligibility`
and `/external-effect-permits`; see [external-effect authority](external-effect-authority.md)
for exact DTOs, grant scope, closed denial reasons and the transaction boundary.
The role cannot use foundation, execution, Website, Telegram, Voice or OpenAPI
routes. Eligibility is a read-only observation. Permit authority exists only after
Conversation commits the immutable opaque permit while holding the same parent
lock used by customer ingress and authority changes. Existing exact permit replay
has precedence over later authority/input changes; fresh permits require the
latest complete canonical customer input and reject every split settling chunk.
This private route does not add a Gateway or public client API.

## Goal5 durable settling and supersede

`TURN_SETTLING_POLICIES` is optional private configuration. A channel-level policy
may be overridden by an integration-specific policy. New canonical inbound
Messages join a Conversation-scoped durable burst under the Conversation lock.
The burst freezes its first policy/version and original maximum cutoff; every
eligible member before its stored ready time extends quiet time, even if policy
configuration is later removed or replaced. At the exact ready time or later,
the old burst closes and a new input uses current policy or IMMEDIATE mode.
Membership is independent of message adjacency, and generic/assistant Messages
do not extend its lifetime.

Telegram and Website workers run the shared Conversation-owned scheduler before
their immediate compatibility path. Only a due, closed burst can be selected;
one Turn receives its maximal serializer-valid contiguous prefix of at most 256
members. Residual members retain the same burst and cutoff through restart.
Membership and all TurnInputMessage ownership commit atomically. `POST
/conversations/{id}/turns` returns `SETTLING_REQUIRED` if asked to bypass a
policy-bound selection. Input ownership is shared across transports and all
retained Turn ranges, including terminal Turns.

Before a Telegram or Website provider/socket write, `responses/begin` locks the
Conversation then the completed Turn and atomically records `DISPATCH_REQUESTED`
or `SUPERSEDED`. The response includes `send`; false means no new write is
authorized. Website responses also return `denial_reason` as `SUPERSEDED` or
`DELIVERY_NOT_RETRYABLE`. Website v2 `response_attempt_id` is derived from the
canonical `admission_id`; historical v1 UUID4/UUID5 identities remain valid only
for their exact retained admission and delivery. Every v2 lookup checks for an
eligible same-admission historical `WRITE_COMPLETED` source before honoring a
persisted denial. It returns `REPLAY_COMPLETED` with the real source ID while
leaving the denied requested row unchanged. A prior failed/unknown/denied
identity alone never grants another send. Only an undispatched delivery can
create a new `SEND`; only an authorized confirmed write may be replayed, subject
to retained mode/control/authority rules. A stale draft retains its Turn and
`operation_refs`, and its delivery cannot be reopened. An unfinished write is
recorded as `UNKNOWN` on retry and is never blindly repeated. See
[durable turn settling](turn-settling.md) for exact deadline, selection,
restart, fairness, and issue #18 behavior.

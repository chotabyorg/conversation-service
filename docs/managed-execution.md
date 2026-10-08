# Managed AgentTurn execution v1

`POST /internal/v1/turns/{turn_id}/execute` accepts only existing claim_epoch and
SecretStr claim_token. Authentication is the existing auth-before-body boundary;
UUID access resolves through a complete trusted parent grant. Response is
TurnResult. The separate opt-in Goal4A Website worker uses this service primitive;
[Website canary](website-canary.md) defines its scheduling/recovery boundary.
There is no public gateway route or provider delivery in Conversation.

Conversation owns Messages, sequence, selection, stable Turn, claim/epoch, draft
and execution uncertainty. AI owns Agent configuration, prompts, KB, model context
budgeting, reasoning, tools and future operation receipts. Execute accepts no
prompt/model/tools/owner/agent/URL/history override. Draft generation appends no
Message, changes no communication counter and creates no delivery journal row.

## Private local configuration

MANAGED_AI_BINDINGS defaults empty. Enabled JSON array has 1..64 exact
`{owner_id,integration_id,channel,agent_id,service_token}` records. Owner is exact
opaque CP text; integration/agent are UUIDs and channel is uppercase canonical enum.
Duplicate Conversation scope is invalid. Dedicated token is 32..256 ASCII
`A-Za-z0-9._~-`, cannot equal an inbound Conversation token and is hidden from repr.
AI independently validates its full grant and the authoritative agent owner UUID.
No CP fetch, registry service, production token or GitOps provisioning exists.

MANAGED_AI_BASE_URL is optional without a production default. It permits only an
HTTP(S) origin, without credentials/path/query/fragment. Missing binding/URL returns
fixed 503 before marker/network. Connect/write/read/pool/total defaults are
5/10/100/5/120 seconds; all finite positive settings<=240. Total is independent
of component timeouts. Choose AI/CS deadlines together: AI90s work and ordinary
cleanup targets normally fit the CS120s coordination cutoff. Retained final AI
resource joins can exceed those targets; CS may time out while AI is still joining
its owner. CS preserves EXECUTION_UNKNOWN/the durable marker and never resends.
Claim an appropriate lease (e.g. 300s); execution does not secretly renew it.
The outgoing HTTP port retains native TCP stream objects immediately on acquisition,
before TLS/protocol setup can lose them. First and repeated caller cancellation
signal only the active operation CancelScope and join the same request-local HTTP
owner; they never cancel its teardown task. Native operation/TLS closes have a
shielded two-second failure-detection budget. Final cleanup runs after the operation
scope exits, finishes abort on each captured actual transport (including an
already-closing transport with buffered writes), and joins physical socket release
before returning or propagating the original cancellation. Failed close certainty
maps to EXECUTION_UNKNOWN; cleanup does not issue another request. Its private native HTTP/1 ABI is
HTTPX0.28.1/httpcore1.0.9/AnyIO4.15.1 on asyncio; HTTP/2, custom transports/backends
and other versions are unsupported. Final native joins have no universal OS bound.
`.env.example` has commented synthetic local placeholders.

## Request and result

AI endpoint is fixed `POST /internal/v1/managed-turns/execute`. Request contains
contract_version=1, conversation_id, turn_id, owner_id, integration_id, agent_id,
channel, input_revision, authority_revision, input_first_sequence,
input_last_sequence, context_last_sequence, messages and selected_input_message_ids.
Every Message has id, sequence, actor, direction, content_type=TEXT and exact text.
Entire frozen prefix is strictly 1..context_last with unique IDs and <=256 Messages.
Selected IDs equal the inclusive first/last range's IDs in canonical order;
non-suffix/multiple/any-actor selection never duplicates or moves content. Extras
are rejected; counters are strict bounded BIGINTs. Text retains Goal1 bounds.
Total text<=524288 UTF8 bytes; encoded request<=1048576 bytes. Oversized full
projection fails before dispatch. Snapshot loads at most 257 rows; Conversation
never truncates/splits history. AI owns later model context budgeting.

Result echoes version/conversation/turn/agent and all frozen counters exactly.
Status is DRAFT_READY/NO_RESPONSE/FAILED/UNKNOWN; draft exists only for DRAFT_READY,
error_code only for FAILED/UNKNOWN. Closed causal codes are in independently
mirrored `schemas/managed.py`. Operation_refs defaults [], with <=32 unique sorted
existing opaque AI references and explicit nullable outcome. Goal3 AI may retain
durable references; Conversation stores no raw Business receipt or ledger. No reasoning, secret, provider delivery or raw
business receipt belongs in results.

Client streams at most 262144 response bytes, requires JSON media and identity/no
encoding, validates every echo/shape and disables environment proxy, redirects
and all transport/status retries. Only exact recognized AI boundary status/code/
fixed-message envelopes 401/404/413/415/422 establish no execution. Proxy text,
5xx/redirect/malformed/truncated/oversized result is uncertainty. Port failures and
application logs retain no body/token/URL/exception text.

## One attempt and fencing

Each phase owns a short independent PostgreSQL transaction. Lock order is
Conversation then fresh Turn; lease clock is sampled afterward. Snapshot validates
proof and copies authorized immutable frozen DTO values. Preparation/encoding runs
outside locks. Another transaction revalidates proof/lease and stores four fields:
managed_agent_id, managed_dispatch_epoch, managed_dispatch_started_at and
managed_request_fingerprint. All commit before the single HTTP call. Uncertain
marker commit means no send. Fingerprint covers canonical v1 request plus namespace
`main-chota.conversation.v1`, excluding tokens.

Same-epoch unfinished marker returns 409 MANAGED_EXECUTION_STARTED without changing
live work. Expired marked Turn recovers on the same UUID with a new epoch; execute
saves FAILED/EXECUTION_UNKNOWN without a second execution request. Goal4A Website
recovery may read genuine references from the protected AI evidence endpoint,
rechecking current proof/live lease around that read. Marker is never cleared or reassigned.
Terminal winning proof returns retained result/replayed=true without HTTP even
after expiry. Displaced proof stays stale; late work cannot overwrite a winner.

Valid DRAFT_READY/NO_RESPONSE uses existing completion. Known model/execution timeout
maps TIMEOUT, context/result limit maps INVALID_RESULT, other known no-tool failure
maps PROCESSING_ERROR. AI UNKNOWN and untrusted transport/result map EXECUTION_UNKNOWN.
Finish uses original proof and existing terminal fingerprint. Held work can finish
after input/control changes without delivery authority. Expiry without recovery
rejects a new finish as LEASE_EXPIRED.

Cancellation after marker joins bounded best-effort original-proof DB finalization
and re-raises cancellation. Stale/expired/failed finalization leaves marker. Crash
between marker and HTTP conservatively sacrifices availability: recovery is UNKNOWN
even if nothing was sent. Result commit uncertainty permits only the same exact DB
terminal request/proof retry; executor never repeats HTTP. EXECUTION_UNKNOWN means
effects may have happened, not proof of failed mutation. No exactly-once business/
cross-Turn guarantee or replacement-Turn retry exists. Goal3 AI owns receipts and
reconciliation; read-only late-reference queries never alter terminal fingerprints.

## Additive schema and checks

0002_managed_execution adds nullable markers with null-safe shape, epoch/status/
time/lowercase-digest CHECK and extends failure CHECK. 0001 stays unchanged. Goal1
rows start with null markers and retain fingerprints/replay. Readiness requires
exactly 0003_website_canary, including the new Website journals. Downgrade locks the table and refuses any marker/EXECUTION_UNKNOWN row
before removing columns. Test downgrade only on an empty explicit disposable DB.
No production migration is applied here.

Goal1 source on 0002 preserves data but is unready and cannot parse EXECUTION_UNKNOWN.
Use a compatible Goal2 image or forward fix; erase neither marker nor uncertainty
to make old source/schema serve again.

Service tests require explicit disposable PG16 URL and exact endpoint guards. The
separate mandatory `python -m pytest tests/managed_cross_service/acceptance_http.py`
gate requires explicit AI candidate/runtime and separate disposable PG inputs,
runs real apps/client over HTTP with fake model/KB/MCP only and fails missing inputs.
The standard service suite remains runnable independently. Goal4A real-process
crash tests require their explicit Website candidate/runtime inputs; see
`tests/website_cross_service/README.md`. Author checks,
independent review and fresh verification remain separate evidence.

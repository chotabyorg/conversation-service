# Main Chota Platform — Conversation Service

Candidate Voice source integration is described in [Voice contract](docs/voice-contract.md).
It is stacked above Goal5 and disabled by default. The separate native entry point
is `python voice_worker.py`; it requires explicit VOICE grants, bindings and an
enabled worker. This does not activate a phone reservation or production route.

Durable provider-neutral communication journal and processing primitives for Main
Chota Platform. PostgreSQL owns accepted inbound events, canonical Messages,
Conversation identity, stable AgentTurns, delivery evidence and control authority.
A protected managed executor calls `ai-agent-service` for a claimed Turn. The
Website and Telegram workers consume only their explicitly admitted channel work;
Conversation itself sends nothing to a messaging or business provider.

Goal 1 is tracked in [issue #1](https://github.com/chotabyorg/conversation-service/issues/1),
linked to the [Project #3 Foundation initiative](https://github.com/orgs/chotabyorg/projects/3/views/1?pane=issue&itemId=263003543).
The [Main Conversation ADR](https://github.com/chotabyorg/gitops/blob/05867486c8de6292f6b5679391e84c19d3421557/docs/main-chota-conversation-foundation.md)
defines this boundary. Foundation remains an umbrella for later stages; Goal 1
alone does not complete it.

Goal 2 is tracked in [issue #3](https://github.com/chotabyorg/conversation-service/issues/3).
See [managed execution](docs/managed-execution.md) for the independent versioned AI contract,
one-attempt semantics and private local binding configuration. Goal4A adds the
separate immediate Website coordinator described in [Website canary](docs/website-canary.md).
Goal4B adds a channel-isolated Telegram admission and immediate worker described in
[Telegram canary](docs/telegram-canary.md); its cohort defaults empty.

Managed request fingerprints resolve retained hashes by exact wire profile:
pre-stream, explicit `false/2000`, or Voice-v2-only `true/2000`. Current
Conversation already writes the explicit text and Voice-v2 profiles. Update the
AI reader first so it accepts both current bodies and historical omitted fields;
then this Conversation reader can restore historical bodies from their stored
hashes. New freezes keep the same serialized bytes. The [history projection
contract](docs/history-projection.md) describes the locked checks. Existing rows
need no migration or rewrite.

**Source implementation; production rollout requires separate evidence.** There
is no public KrakenD route or provider webhook. The manual publisher builds only
immutable images and has no deployment/promotion job. Existing Telegram, Instagram, Website, WhatsApp and AI
callers continue their existing paths.

This is separate from paused fashion/e-commerce **Chota 2.0**
(`chotabyorg/chota`, Project #6); none of its Conversation code is a dependency or
implementation template. Conversation coordination is separate from Business
operation coordination. AI Business V2 PR #43 was OPEN/DRAFT/unmerged at the
historical Goal1 discovery. The Goal4A runtime baseline includes accepted newer
Business work; this service preserves that boundary and stores only opaque refs.

## Foundation contracts

Conversation identity is the exact tuple
`(owner_id, integration_id, channel, provider_thread_id)`. Owner is the opaque Main
Control Plane owner string; integration is a trusted Control Plane UUID. Only UUID
format and ASCII channel case normalize. Owner/thread whitespace, case and Unicode
composition remain exact; no customer/channel merge occurs. Namespace authorizes
event ingestion within an integration and does not partition that Conversation's
history. Different provider accounts needing isolated history need distinct
integrations. No provider credentials are stored.

Server-generated canonical SHA-256 fingerprints distinguish exact replay from
conflicting content under `(owner,integration,channel,namespace,event_id)`. The
dedup key excludes thread, so moving a repeated event to a new thread conflicts.
Event receipt, Conversation resolution, Message and local sequence allocation
commit atomically. Exact replay preserves stable IDs and does not allocate another
Message or revision.

The Conversation row serializes its own mutations; unrelated Conversations can
progress independently. Unique identity/event/Message indexes and the active-Turn
partial unique index defend the invariants at PostgreSQL level. `PENDING` and
`CLAIMED` both occupy the one active logical Turn slot. Generic ingress primitives
do not schedule work. Explicit Website and Telegram admissions use separate durable
workers. Optional Goal5 settling policies create immutable, Conversation-local
bursts; the persisted deadline and selected input ownership survive worker restarts.
Policy-free admissions retain their immediate path. Before a provider or socket
write, the adapter records a locked send decision; stale SETTLING drafts become
`SUPERSEDED`, while uncertain prior writes remain recover-only and are never blindly
reissued. See [durable turn settling](docs/turn-settling.md).

A claim fences a stable Turn using a monotonically increasing epoch and a random
256-bit bearer whose SHA-256 digest alone is persisted. Expired recovery rotates
proof on the same Turn UUID. Lease decisions use the DB clock after acquiring
locks; stale or expired claimants cannot renew/finish. Exact terminal replay is
accepted only for the winning proof and identical result, even after expiry.

Completed draft text is an internal candidate response until eligible Website write
evidence creates canonical history; see [history policy](docs/history-projection.md).
DeliveryAttempt references that immutable draft Turn. One journal attempt per
draft and retained reconciliation keys prevent duplicate/reverted evidence.
`UNKNOWN`, `FAILED`, `PARTIAL` and `INTERRUPTED` remain distinct. Control CAS
increments authority revision even for the same target mode; stale authority is
rejected at creation/dispatch journaling. No endpoint grants permission to send.

See [API contracts](docs/api.md), [states and transactions](docs/states-and-transactions.md),
[architecture](docs/architecture.md) and [evidence provenance](docs/evidence.md).

## Local development

Python 3.11, PostgreSQL 16, FastAPI/Pydantic 2, async SQLAlchemy 2 and Alembic are
required. Runtime and dev dependency closures use exact `==` pins in requirements
files; pyproject configures checks only. No Redis, broker, provider, Control Plane,
LLM or external business API is needed for the service suite; managed HTTP tests
use explicit local fakes.

```bash
python3.11 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements-dev.txt
cp .env.example .env
```

Replace every placeholder in untracked `.env`. Generate a URL-safe random local
database password and an independent service token (32..256 ASCII characters from
`A-Za-z0-9._~-`); do not reuse Main polling or AI chat credentials. For native
startup DATABASE_URL must be `postgresql+asyncpg` and point to the local instance.
Percent-encode any database password characters reserved by a URL. Compose's
DATABASE_PASSWORD is local orchestration configuration and is ignored by Settings.
The native application and migration commands below read `.env` in the current
directory. [Goal4A Website canary](docs/website-canary.md) documents the private admission API,
separate `python worker.py` process, narrow roles, evidence recovery and rollout
gates. [Goal4B Telegram canary](docs/telegram-canary.md) documents its exact binding,
rollback and delivery evidence. Empty cohorts and disabled workers preserve the dark default.

Migrations read only DATABASE_URL, without variable interpolation or
loading authentication configuration. An exported DATABASE_URL takes precedence,
including an empty or invalid value, which fails validation instead of falling back.

```bash
docker compose up -d postgres
python -m alembic upgrade head
python -m uvicorn main:app --host 127.0.0.1 --port 55585 --no-access-log
# In separate processes, after configuring the private worker bindings:
python worker.py
python telegram_worker.py
```

Alternatively migrate and run the container using the same isolated database:

```bash
docker compose build conversation
docker compose run --rm conversation alembic upgrade head
docker compose up -d conversation
```

Compose binds only `127.0.0.1:55585` (HTTP) and `127.0.0.1:55595` (PostgreSQL),
using its own internal network rather than the existing microservices network.
The application image runs as UID 10001 and Compose uses a read-only filesystem.
Migrations are explicit; startup never substitutes `create_all`. `/ready` succeeds
only against migration `0009_voice_partial_drafts` and the required table queries. `/health`
checks process liveness. Both return bounded public status only.

## Internal authentication

Programmatic `Settings` construction accepts the field names `database_url`,
`auth_principals`, `log_level` and their uppercase environment aliases. Explicit
constructor values take precedence over environment, then `.env`, then defaults;
invalid explicit values fail validation. When both constructor spellings of a
field are supplied, its uppercase alias takes precedence.

`CONVERSATION_AUTH_PRINCIPALS` is private server-provisioned JSON with 1..32 entries:

```json
[{"principal_id":"local-worker","token":"<dedicated-random-token>","grants":[{"owner_id":"local-owner","integration_id":"00000000-0000-4000-8000-000000000001","channel":"WEBSITE_WIDGET","provider_namespace":"local.widget"}]}]
```

Each principal has 1..64 complete, exact grants. Empty/invalid configuration,
duplicate credentials/IDs/grants and wildcard owner are rejected at startup.
Grant overlap between distinct workers is intentional. The registry is trusted
bootstrap configuration of Control Plane bindings; it is not an integration
registration API. Future production provisioning requires separate integration
work. Current CP does not yet implement WHATSAPP despite the reserved generic
identity value here.

Registry roles are `FULL` (existing default), `WEBSITE`, `PROCESSING`, `TELEGRAM`,
`TELEGRAM_PROCESSING`, `VOICE`, `VOICE_PROCESSING` and `BUSINESS_EFFECT`; narrow
roles cannot call foundation/control/execution/OpenAPI routes. Every business/OpenAPI
request requires exactly one `X-Service-Token`; malformed,
duplicated, wrong and missing tokens return the same 401. Authentication precedes
body parsing and DB work. `X-User-Id`/`X-User-Role` cannot expand access. Ingestion
must match one complete four-field grant; entity UUID access must join through a
granted Conversation owner/integration/channel. Nonexistent and out-of-scope IDs
both return 404.

Default `/docs`, `/redoc` and `/openapi.json` routes are disabled. Fetch the schema
from `GET /internal/openapi.json` with the service header using an HTTP client that
reads the credential from private configuration. Avoid embedding credentials in
shell commands/history. Bodies are limited to 65536 actual streamed bytes, even
without an accurate Content-Length. Errors and logs omit rejected values, text,
token/claim proof, DSN, SQL and exception detail. Optional request IDs are bounded
and sanitized; server access/query and SQL parameter logs are suppressed.

Managed failures emit a diagnostic only after result validation and correlation:
`MANAGED_FAILED_<error_code>` or `MANAGED_UNKNOWN_<error_code>`, using the closed
ManagedTurn error enum and canonical Turn UUID. Exact known 401/404 envelopes emit
`MANAGED_PREEXECUTION_UNAUTHENTICATED` / `MANAGED_PREEXECUTION_AGENT_NOT_FOUND`.
These finite log codes do not change terminal mappings or retry behavior and do
not contain response bodies, prompts, credentials or exception details. They
describe observed upstream results, not proof that terminal persistence succeeded.

## Quality and disposable testing

Use a separately created disposable PostgreSQL 16 database. Tests require explicit
`TEST_DATABASE_URL` and `TEST_DATABASE_DISPOSABLE=yes`, never fall back to runtime
DATABASE_URL, and reset only their designated test database. Do not set these to a
shared, dev or production database. They exercise independent sessions/connections
and real lock waits; SQLite cannot establish the constraints or race behavior.

```bash
python -m pytest
python -m ruff check .
python -m mypy
python -m pip check
docker build -t conversation-service:local .
```

The quality workflow runs PostgreSQL migrations, migrated service readiness,
tests, lint, typecheck and a local image build with `contents: read` permissions.
It has no deployment credentials or publishing step. Implementation checks are
distinct from the fresh independent review/verification required by Goal 1; see
the evidence record for what actually ran.

## Goal4A, Goal4B and Goal5

[Website canary source](docs/website-canary.md) adds immediate FIFO execution and
protected evidence reads while retaining legacy Website traffic unless exact
configured admission selects it. Production rollout/smoke require independent
release and runtime verification. Website v2 uses its retained admission identity
for a stable response-attempt ID and preserves historical per-attempt write evidence;
the public message ID is unchanged. Goal4B binds one immutable Telegram bot per
owner/integration. Migration derives `BOUND` from one historical MANAGED bot and
`CONFLICT` from mixed-bot history, preserving exact recovery evidence without
granting new sends under conflict. Goal5 policy is optional and defaults off.
Full demo cutover additionally requires stable frontend message IDs, independent
review of the [history policy](docs/history-projection.md), and a separately
approved routing gate. A Business effect permit is an additional private
Conversation authority boundary for opaque AI Business operations; it does not
store business plans, parameters, receipts or operation status. The exact contract
and conservative input-completeness behavior are in
[external-effect authority](docs/external-effect-authority.md). Settling remains a
FIFO of separate temporal bursts. Cross-burst catch-up grouping is deferred to a
bounded follow-up before broad tester rollout; no larger quiet window is used to
mask accumulated backlog.

Migration `0006_turn_settling` creates durable burst and provider-account state,
records response send-permission decisions, and backfills input revisions without
assigning settling policy retroactively. It refuses downgrade while retained
burst ownership, supersede or send-permission evidence, settling fences, multiple
response attempts, or non-reconstructible ownership/account history depends on the
new schema. See the migration details in [durable turn settling](docs/turn-settling.md).
Migration `0009_voice_partial_drafts` adds Voice partial-draft storage and per-lease
response ordinals. Migration `0008_business_effect_permits` adds the authority input floor and
append-only opaque effect permits. Existing nonzero authority epochs are backfilled
to their current message tail, so their historical customer inputs cannot be
reinterpreted as fresh business authority after upgrade.

## Telegram Handoff and owner Inbox

The Handoff MVP adds a durable human-assistance case, owner-only Inbox commands,
trusted CP/AI grant lifecycle proofs, canonical HUMAN messages, and one ordered
Telegram writer journal for HUMAN and managed AGENT output. Its route and strict
DTO details are in [API contracts](docs/api.md); the accepted cross-service
contract is mirrored in the Gateway API documentation.

The feature is dark by default. `TELEGRAM_HANDOFF_INTEGRATION_IDS` must contain
the exact enrolled integration UUIDs and stay within the existing sticky
`MANAGED_AI_BINDINGS` and Telegram identity grants. A browser-facing surface uses
the original live `access_token` cookie: Conversation calls the configured
`USER_SERVICE_BASE_URL/api/v1/me` and accepts only an active account. Invalid
sessions return 401; user-service outages, malformed responses, or configuration
failures return 503 so an outage does not look like logout. Inbox operations are
owner-only; `X-User-Id`, `X-User-Role`, FULL service credentials, and Telegram
principal identity never stand in for a human session. Email verification is
required by default and may only be relaxed by explicit private configuration.

Grant provisioning is CP-owned and lifecycle operations require independent
`INBOX_LIFECYCLE_TOKEN` and `HANDOFF_LIFECYCLE_TOKEN` credentials. Conversation
obtains fresh exact CP integration and AI agent ownership proofs, checks the
account UUID mapping, and persists generation high-water marks before granting
Inbox authority. CP/AI lifecycle changes hold the exact binding pending until a
matching durable revoke acknowledgement returns. Revoke advances the shared
Conversation authority/input floor in the same transaction that invalidates
new grants, AGENT sends, and unacquired HUMAN sends. Existing external-effect
permits remain immutable under Goal5 semantics. After an AI-only revoke, CP's
cached grant may still be active; CP must complete its deactivate/activate
lifecycle before a new grant enrollment can succeed.

`POST /public/v1/inbox/cases/{case_id}/claim`, `/messages`, and
`/return-to-agent` use stable command UUIDs and expected case, authority, and
assignment revisions. The transaction stores command receipts and canonical
HUMAN Messages before delivery. A public receipt means queued, not delivered;
PENDING/UNKNOWN facts remain visible. A HUMAN send is authorized once by its
exact scope, grant generation, case assignment, authority revision, and FIFO
ordinal. Return moves the case to RETURNED, snapshots the latest input sequence
as the resume floor, retires never-acquired HUMAN intents, and leaves LIVE
provider writers as FIFO barriers until their original task reports positive
`TASK_JOINED` evidence.

After a Telegram integration scope has enrolled Handoff, the generic FULL
`/conversations/{id}/control` route is disabled for that scope, even after a
later revoke. Owners return control only through the Inbox case command, which
updates the resume floor and retires queued intents atomically. New generic
`/conversations/{id}/turns` selections and `/conversations/{id}/delivery-attempts`
creations are also rejected for enrolled scopes; exact historical replays still
return their original receipt. Before the first grant activates, provision locks
the scope and ordered roots and refuses any pending or claimed AgentTurn until
the legacy coordinator has resolved it. Generic
`/delivery-attempts/{id}/reconcile` also rejects Telegram v2-tagged intents;
legacy attempts retain their reconciliation path and do not acquire v2 FIFO
ordinals during migration. Provider results and writer retirement for v2 intents
must come from the original supplier's v2 completion identity or the dedicated
positive supervisor retirement protocol described in [the API](docs/api.md).

Managed Handoff turns use strict v3 request/result contracts while existing
v1/v2 paths stay intact. The v3 request keeps the frozen canonical message
window. `communication_facts` has a 64-item bound and preserves a delivery fact
for every included HUMAN/SYSTEM outbound; if more than 64 are required, the turn
terminalizes as `INVALID_RESULT` before calling AI. Telegram v2 outbound intent
UUIDs are stable UUIDv5 values derived from
the immutable intent ID. A provider call happens only after the first `begin`
returns `send=true`; replay/recovery never authorizes a second send. `UNKNOWN`
is retained without blind retry. A LIVE writer is never released by elapsed time
or another poller. The original supplier's positive task-joined receipt or an
independently verified supervisor termination proof ends it. New inbound customer messages received during HUMAN mode or at
or below the Return floor are retained and reported `AUTHORITY_FENCED`, not left
blocking ordered update ingestion.

Migration `0012_shared_handoff` adds the case, command receipt, grant
generation, retained v3 snapshot, delivery ordering, writer lifecycle, and Return
floor state. Migration `0013_writer_retirement` adds immutable supervisor evidence
on the original intent. Readiness requires `0013_writer_retirement`. Downgrade is refused while any
case, lifecycle/command receipt, active or historical grant generation, retained
Return floor, v3 Turn snapshot/dispatch identity, or HUMAN delivery intent/evidence
would be lost. Rollback therefore disables the exact cohort and worker while
keeping migration history and sticky bindings available for safe evidence replay.

The retirement port is disabled unless all three private settings
`WRITER_RETIREMENT_TOKEN`, `WRITER_SUPERVISOR_TOKEN` and
`WRITER_SUPERVISOR_BASE_URL` are configured. Credentials must be distinct from
each other and existing service, AI and lifecycle credentials. This repository
defines and verifies the proof contract; it does not yet ship a production
supervisor proof provider. Keep the production Handoff cohort empty until a
provider can prove termination/fencing of the exact acquired supplier/process
incarnation. Heartbeat expiry, a missing pod, a new poller or an owner click are
not positive evidence. Lost legacy identities without such proof stay blocked.

## Text writer lifecycle evidence

[Writer attestation](docs/writer-attestation.md) documents the dedicated scoped
parent producer APIs, immutable closure fences, exact restart-safe recovery
commands and `python retirement_worker.py` entrypoint. The additive readiness head
is `0019_writer_attestation`. Registration enforcement defaults off; retained
closures remain fencing facts across configuration rollback. These are lifecycle
observations on the existing delivery journal, independent of provider outcomes.

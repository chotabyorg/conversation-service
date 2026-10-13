# Main Chota Platform — Conversation Service

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
immediate workers; debounce and supersede remain outside these policies.

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
# In a separate process, after configuring the private Telegram binding:
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
only against migration `0006_turn_settling` and the required table queries. `/health`
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

Registry roles are `FULL` (existing default), `WEBSITE`, `PROCESSING`, `TELEGRAM`
and `TELEGRAM_PROCESSING`; narrow
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

## Goal4A and handoff

[Website canary source](docs/website-canary.md) adds immediate FIFO execution and
protected evidence reads while retaining legacy Website traffic unless exact
configured admission selects it. Production rollout/smoke require independent
release and runtime verification. Full demo cutover additionally requires stable
frontend message IDs, independent review of the [history policy](docs/history-projection.md)
and a separately approved routing gate. Goal4B Telegram and Goal5 settling remain
unactivated until their independent review and release gates pass. See
[durable turn settling](docs/turn-settling.md) for Goal5 policy and delivery rules.

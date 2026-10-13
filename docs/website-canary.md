# Goal4A Website managed canary source

Goal4A is tracked in [Conversation issue5](https://github.com/chotabyorg/conversation-service/issues/5)
and [Main GitOps issue111](https://github.com/chotabyorg/gitops/issues/111). Source readiness,
reviewed candidate, image publication, GitOps desired state, ArgoCD and actual
production runtime are separate evidence. This document does not claim rollout.

## Authority and admission

Only trusted Website ingress can call the four `/internal/v1/website` POST routes.
Every request uses a dedicated `X-Service-Token`, complete owner/integration/channel/
namespace grant and configured agent binding. The public widget key never enters
Conversation. Namespace is fixed `main-chota.website-widget.v1`; channel is
`WEBSITE_WIDGET`. `owner_id`, `integration_id`, `agent_id` come from Control Plane;
`session_id` and `message_id` are stable UUIDs supplied by the technical client.

`/admissions` accepts those five identity fields, exact `text` and
`desired_route=MANAGED|LEGACY`. It returns `{route,admission_id,replayed}` after a
positive transaction commit. The unique scoped message journal excludes session
from identity. Its frozen SHA256 fingerprint includes session, scope, agent and
text, excludes desired route and receipt time. Changed content/session conflicts409.
A transaction advisory lock on the scoped identity serializes rollout/rollback
arrivals; the database unique key remains the final fence. Hash collisions only
serialize unrelated requests. MANAGED decision, Conversation, InboundEvent and
canonical Message commit atomically. LEGACY reserves only immutable routing
identity/fingerprint and does not append canonical customer text.

Retained route wins forever. A gateway rollback must retain guarded integrations,
Conversation connectivity/grants, worker and database. Any identified guarded
request must arbitrate through admission, including a desired LEGACY request.
Uncertain commit or unavailable Conversation cannot fall back to legacy.
Images predating sticky arbitration are unsupported rollback candidates.

`/results` accepts the five identity fields and returns
`{state:PENDING|DRAFT_READY|NO_RESPONSE|FAILED|UNKNOWN,draft_text:null|string}`.
Retained LEGACY, absent or out-of-scope resources return404. Polling and HTTP request
lifetime never own/cancel worker execution. FAILED and UNKNOWN remain distinct.
No internal Conversation IDs belong in the public Website error response.

## Immediate FIFO worker

Run the same image separately with `python worker.py`. Probe port defaults8001;
`/health` tracks owned process task liveness and `/ready` requires enabled processing,
exact migrated schema, validated processing scope and a polling/renewal heartbeat.
The worker waits for schema `0004_website_history`, never migrates on startup, owns
and joins execution/renewal/resources at shutdown, and uses PostgreSQL exclusively
as queue authority.

`WEBSITE_BINDINGS` is a max64 exact mapping list
`[{owner_id,integration_id,agent_id}]`. Duplicate owner/integration is rejected.
Default empty bindings/disabled worker admit no Website work. Enable with
`WEBSITE_WORKER_ENABLED=true`, `WEBSITE_WORKER_PRINCIPAL_ID` matching a provisioned
PROCESSING principal, exact `main-chota.website-widget.v1` grants, existing
`MANAGED_AI_BINDINGS` and `MANAGED_AI_BASE_URL`. Lease defaults30 seconds (5..300),
poll1 second (0.1..10), probe port8001. No production IDs, tokens or sessions are
compiled into source.

Positive MANAGED admissions are selected only under these authorized scopes.
Lock Conversation, reuse existing immediate active Turn, otherwise select earliest
unselected inbound Message. Its UUID is the stable selection key and a constrained
message/Turn association is unique. Freeze context through that Message, excluding
later queued messages. Save original creation revision/fingerprint; retries do not
reconstruct it against later state. Commit Turn before claim. Renew a live claim
independently while executing; lease recovery rotates proof on the same Turn.
Unmarked recovery may dispatch once. Marked recovery never calls execute again.
Pre-dispatch binding/projection failures terminalize the same Turn with a known
bounded failure. A terminal Turn releases FIFO progression to the next Message.

Goal4A.1 adds [canonical assistant history and frozen v2 windows](history-projection.md).
New immediate Website Turns freeze a bounded contiguous absolute canonical suffix
through their selected Message; retained and generic v1 Turns keep the full prefix.
That describes the pre-Goal5 immediate path. The stacked Goal5 source can apply an
opt-in durable policy before immediate selection and atomically fence/supersede a
stale draft; it is not enabled by default. Stable frontend message IDs and an
independently approved routing gate remain required for activation.

## Evidence recovery and transport honesty

The existing AI protected evidence-read endpoint carries only opaque
`operation_refs`. Current winning claim/live lease are checked before and after
recovery I/O. Marker agent must still match exact current binding. Missing/error/
404 evidence never proves no effect. A lost AI completion/draft is
`EXECUTION_UNKNOWN`, potentially permanently, with genuine retained refs when
available. No fresh Turn, model call, provider retry or Business ledger is created.

Protected PROCESSING/FULL `GET /internal/v1/processing/turns/{turn_id}/evidence`
reads late refs for a scoped terminal marked Website Turn. It does not enrich or
rewrite immutable terminal fingerprints. API needs its distinct processing
credential and read-only AI binding/egress when this operational query is enabled.

`/responses/begin` accepts result identity plus a stable `response_attempt_id` UUID
derived from the canonical `message_id`, so exact client retries retain the same
transport identity across Gateway restarts. It returns the retained draft, `send`,
`replayed`, and a denial reason when send is false. It commits the retained draft
authority check, one generic Generated→Dispatch Requested delivery journal and
per-response intent. A replay may resend only after definitive `WRITE_COMPLETED`
evidence while that Turn remains current. Failed, unknown, and superseded attempts
never become sendable again; UNKNOWN is never reset to Dispatch Requested.

`/responses/complete` accepts the same fields plus local outcome
`WRITE_COMPLETED|WRITE_FAILED|UNKNOWN`, returns `{response_attempt_id,outcome,replayed}`.
Each response outcome is immutable except authorized late evidence may resolve
UNKNOWN. Generic delivery transitions once to UNKNOWN
with immutable reconciliation evidence; local socket completion never means browser
acknowledgement, DELIVERED or READ. Unconfirmed intents after crash remain honest.
Safe response replay can create fresh HTTP evidence with zero AI/business execution.
The first newly committed WRITE_COMPLETED also appends one canonical AGENT Message
from the retained logical draft, atomically with the outcome under the parent lock.
It advances sequence only; input_revision retains its customer-input meaning.
WRITE_FAILED/UNKNOWN alone never promote. A later successful attempt may promote
once, including for a retained v1 draft; old completion replay performs no backfill.

## Release and logging

Auth registry roles are FULL (default for existing foundation compatibility),
WEBSITE (only the four POST routes), PROCESSING (only operational evidence GET).
Narrow credentials cannot fetch internal OpenAPI, claim, execute, complete or
change authority. Unknown/out-of-scope identities remain404. Private configuration
and error boundaries never render caller input, secret values or exception repr.

Safe logging is installed before serving/clients. Every configured sink uses a
structured whitelist, removes raw exceptions and arbitrary extras, and suppresses
HTTPX/httpcore descendants and access/SQL records even after logging reconfiguration.
Worker entry explicitly disables uvicorn access logs. Publishers/release operators
must also prove Website/CP/live credential-observing sink protections before a real
widget key is used. Source tests are not deployed logging proof.

Manual `.github/workflows/publish.yml` publishes only full-source-SHA GHCR images
from main, with immutable action pins and no GitOps mutation/promotion/deploy job.
Initial merge does not publish/deploy automatically. Migration is additive and
serialized outside worker startup. Downgrade refuses any retained Website
admission, including LEGACY reservations. Production routing rollback is config-only
and does not downgrade the schema.

Production remains gated by exact candidate independent source/release review,
Main cluster proof, complete sealed dark overlay, deployed logging proof, actual
widget integration binding and separately proven no-business-mutation smoke safety.

### Planned worker shutdown

The Website coordinator stops admitting new claims when lifespan shutdown begins.
Selection already in progress may leave a durable PENDING Turn; it does not claim
that Turn after stop. A claim started before stop remains owned work and follows
its existing execution/lease/result path. Readiness becomes false immediately.
Idle and schema-readiness polling wake without waiting for their next interval.

The current owned execution retains lease renewal and may finish/persist during
`min(MANAGED_AI_TOTAL_SECONDS + 10, 130)` seconds of graceful drain. At the current
120-second client budget this is 130 seconds. After the deadline the existing
cancellation path preserves UNKNOWN/dispatch evidence; it never retries AI work.
The coordinator and its owned tasks are joined before the DB is disposed.

This is a graceful-wait limit, not a guarantee that arbitrary OS/DB/network
cleanup always finishes before SIGKILL. The production worker's 150-second grace
leaves 20 seconds for cancellation/finalization and process exit; real-process
verification remains required. Larger client budgets are not supported merely by
this cap and need a coordinated grace/transport/UX review. AI and its TLS proxy
must remain available while the worker drains; worker drain cannot compensate
for an upstream dependency being terminated early. Goal4B and other routing
policies are unchanged.

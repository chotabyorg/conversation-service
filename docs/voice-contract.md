# Candidate Voice/Conversation contract v1

Source implementation authorized by the user on 2026-10-15. This is a review
candidate above Goal5 `fe4fa7b3d809efdf3121b4e8c300905c708e5197`, not a declaration
that Goal5 or a production Voice protocol has been accepted. ManagedTurn v1/v2,
OperationReference, Business/CRM semantics and existing channel routes are unchanged.

## Ownership and trusted routing

The path is PBX → Voice → Conversation → managed AI → Conversation → Voice → TTS.
Voice owns audio, acoustic finals, playback and interruption. Conversation owns
canonical Messages, AgentTurns, agent binding, send permission and delivery journals.
Voice never calls AI, sends prompts/history, or holds a second durable history.

The gateway receives an injected trusted `call_id → owner/integration/agent`
resolver. The platform must derive that binding from the called DID and an active,
ready VOICE integration. Caller audio, caller ID and browser-supplied agent IDs
are not routing authority. AudioSocket's UUID alone contains no DID: production
PBX metadata registration and trusted pool resolution remain activation gates.
The local acceptance fixture uses synthetic trusted bindings, not the real pool.

`RESERVED` never grants eligibility. There is no reservation activation API here.
Conversation verifies the exact complete VOICE grant, namespace
`main-chota.voice.v1`, configured managed binding/agent and explicit canary cohort.
Both the cohort and Voice worker are disabled by default. Configuration registries
follow the existing private bootstrap model; automated provisioning must come from
the platform rather than a separate gateway customer/number registry.

## Private API

All paths below are POST under `/internal/v1/voice`, require exactly one
`X-Service-Token`, forbid extra fields and inherit the auth-first 64 KiB body cap.
The `VOICE` principal can use only these routes. `VOICE_PROCESSING` is worker-only
and has no HTTP access. Tokens, text, phone numbers and bodies are never logged.
The OpenAPI DTOs in `schemas/voice.py` are the exact request/response schema.

Every request includes `call_id` and `media_session_id`, both UUIDs. A fresh media
owner creates a new session UUID; another process cannot take over an existing
call by repeating its call UUID. A process restart cannot resume PBX audio.

| Suffix | Additional input | Result |
| --- | --- | --- |
| `calls/start` | owner_id, integration_id, agent_id | conversation_id, ACTIVE/ENDED/EXPIRED, replayed |
| `calls/heartbeat` | none | same call state; renews only an unexpired active lease |
| `utterances` | sequence starting at 1, final text | canonical message_id and replayed |
| `responses/next` | none | next eligible candidate turn_id, or null; no speech text/permission |
| `responses/begin` | turn_id | stable response_id, send, text only when send=true, replayed |
| `playback` | response_id, event_sequence, state, generated_bytes, transport_accepted_bytes, terminal | accepted event_sequence and replayed |
| `calls/end` | DISCONNECTED/TIMEOUT/CANCELLED/FAILED reason | retained ended call and replayed |

The gateway validates complete response correlation and limits each HTTP operation
to three seconds by default and each response to 64 KiB. No automatic redirect,
proxy environment, request retry, legacy fallback or direct AI path is used.
A transport/protocol failure stops the local call. A lost permission response
never causes a second send. HTTP success only follows a positive database commit.

## Admission and execution

Call creation freezes the agent and media session. Heartbeats run every second;
the default database-clock lease is 15 seconds (configurable 5..60), with a hard
one-hour call lifetime. Expiry cannot be renewed or restarted. The worker records
expired calls as TIMEOUT and unresolved output as UNKNOWN. Exact input/evidence
replay remains available after closure; no new input or output is authorized.

Each final acoustic utterance becomes one CUSTOMER/INBOUND Message. Sequence is
strictly contiguous and at most 10,000 per call. The same sequence and text replay;
changed content conflicts. No synthetic history or interim transcript is stored.
Admission increments the existing input and settling revisions atomically under
the Conversation lock, and returns before AI execution or playback completes.

Voice uses immediate one-final selection, without a second text settling timer.
Voice `TURN_SETTLING_POLICIES` are rejected in this candidate. Selection reuses
Goal5 global input ownership, the one-active-Turn database constraint, immutable
v2 projection and same-Turn lease/dispatch markers. Call/agent/input identity is
checked again before AI dispatch. A closed unmarked Turn fails without AI; a
marked lost execution recovers as EXECUTION_UNKNOWN without re-execution.

## Output permission and playback facts

Under the same Conversation lock as admission, `responses/begin` checks the live
call, canary/binding, control authority and the exact frozen settling revision.
It records DISPATCH_REQUESTED with one send permission or SUPERSEDED with no
permission. Every repeat returns send=false and no text. There is at most one
unfinished authorized output per call. No network-wide atomicity is claimed:
permission is a decision at commit time, not proof of playback. Newer input after
that decision is handled by local barge-in and subsequent transport facts.

Voice polls responses independently of admitting input; awaiting AI or speech
must not prevent the caller's next final from reaching Conversation. Loss of the
Conversation heartbeat stops active playback and releases media resources.
Heartbeat also rejects HUMAN control, so a takeover stops the gateway. Removing
the configured cohort prevents even a previously selected unmarked Turn from
dispatching to AI; marked executions retain the existing UNKNOWN recovery path.

Playback has ordered, immutable, idempotent event sequences and monotone PCM byte
counters (8 MiB maximum per output). Facts remain recordable after call end or
authority change only for that already authorized response and media session.

| Voice fact | Meaning and generic delivery mapping |
| --- | --- |
| STARTED / PARTIAL, terminal=false | Transport progress journal only; delivery stays unresolved |
| COMPLETED | Positive, equal generated/accepted counts; PROVIDER_ACCEPTED, never DELIVERED/READ |
| INTERRUPTED | Barge-in/known interruption, distinct from completion |
| NOT_STARTED | No accepted bytes; FAILED |
| FAILED | FAILED with no accepted bytes, PARTIAL if some bytes were accepted |
| UNKNOWN | Uncertain transport outcome; never resend automatically |

**Proposed Voice history policy for review:** only terminal COMPLETED creates one
canonical AGENT/OUTBOUND Message from the retained full draft. It means all PCM
was accepted by the transport, not that a human heard it. Interrupted, failed and
unknown output creates no full assistant Message; partial text cannot be inferred
from PCM byte counts. The immutable draft, operation_refs and exact playback facts
remain retained. No Business effect is rolled back by superseding communication.

## Native verification and activation

Use Python 3.11 and a dedicated disposable PostgreSQL 16 database. Run migrations,
the full suite, Ruff and mypy as documented in README. The explicit two-repository
acceptance uses real local AudioSocket and HTTP connections and a deterministic
managed AI HTTP fake:

```powershell
$env:TEST_DATABASE_URL = '<private loopback disposable PostgreSQL16 URL>'
$env:TEST_DATABASE_DISPOSABLE = 'yes'
$env:VOICE_CANDIDATE_ROOT = '<absolute Voice candidate checkout>'
python -m pytest tests/voice_cross_service/acceptance_http.py -q
```

It exercises complete playback, interruption/barge-in and call closure. It does
not establish live provider quality, PBX routing readiness, heard speech, real AI
Business safety or production readiness. All endpoints in this test are loopback;
the fixture denies external sockets. No Docker is required locally.

On native Windows, 16 existing buffered-close regressions in
`tests/test_managed_http_resources.py` cannot establish their Linux TCP backpressure
precondition: Winsock loopback accepts the entire bounded request before close.
Do not weaken those assertions. Run them in the full Linux CI; for the remaining
native suite, explicitly deselect the three parametrized buffered-close functions
in that module. Its three partial-TLS cases run with Windows' selector loop.
The Voice and cross-service acceptance tests require no platform deselection.

Before activation: accept Goal5, independently review this contract/implementation
and Voice history policy, pin both revisions, prove trusted DID routing and active
integration readiness, provision dedicated grants and agree runtime rollout.
Only then coordinate one real inbound test on `+375173820015`. No other number,
pool route, production migration, credential, image publication or GitOps change
is authorized by this source implementation.

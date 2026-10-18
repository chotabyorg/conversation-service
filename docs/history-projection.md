# Goal4A.1 canonical Website history

PostgreSQL Conversation Messages are the only history authority. Browser transcripts,
Redis, legacy AI Sessions, model summaries and transport presentation are not inputs.
Message order is commit order serialized by the authoritative Conversation row.
Sequences and UUIDs are never renumbered or replaced.

A completed AI draft remains a candidate until a **new durable WRITE_COMPLETED**
physical Website response attempt commits. In that transaction, under the Conversation
lock, append the exact retained logical draft as AGENT/OUTBOUND/TEXT if its source
Turn has no Message. Advance last_message_sequence once; do not change input_revision.
The source Turn is unique and has a composite FK to the same Conversation.
WRITE_FAILED, UNKNOWN and unfinished intents alone create no assistant history.
A fresh successful physical retry may promote that same draft once, without AI
execution. Replaying an already retained completion creates no Message or backfill.
This policy also applies to new successful attempts on retained v1 Website drafts;
their frozen request/result/fingerprints and old evidence remain unchanged.

WRITE_COMPLETED proves a local socket write only. Generic DeliveryAttempt remains
UNKNOWN; DELIVERED/READ/browser acknowledgement are never synthesized. Late success
appends at the current canonical end. It cannot enter an earlier frozen Turn.
Draft generation, canonical AGENT append and result replay create no admission,
customer event or new AgentTurn.

## Contract and deterministic budget

Existing/generic Turns retain managed v1, including omission=1 and the exact full
prefix1..context_last_sequence. Only new immediate Website Turns use v2.
The execute endpoint and Message fields are unchanged; v2 derives window start from
messages[0].sequence. It requires a contiguous absolute canonical window through
context_last_sequence, containing the complete inclusive selected input interval.
Selected IDs must match that interval in order. Results echo the requested version.
OperationReference, strict v1 managed evidence values and execution namespace
main-chota.conversation.v1 are unchanged. Conversation interprets no confirmations
and owns no Business authority; a frozen Business proposal origin can be outside
the current history window.

At creation, under the Conversation lock, freeze the existing input/context bounds
(context ends at the selected Website Message), version, window start, admission's
authoritative agent and exact canonical request SHA256. This small hash detects
altered rows/identity before dispatch; no second full body is stored. A PostgreSQL
trigger refuses version conversion of any retained Turn and changes to retained v2
identity, bounds and freeze fields. Changed
configured agents cannot replace admission authority.

1. Read at most257 whole canonical Messages descending through the fixed context
   bound, restore ascending order and verify contiguity. Older rows outside this
   candidate are ineligible under the256-message limit.
2. Preserve the complete selected interval. Drop the oldest prior whole unit:
   a CUSTOMER plus following AGENT rows before the next boundary. At history
   boundaries use whole singletons for AGENT/HUMAN/SYSTEM or an incomplete prior
   unit. Never drop selected input, create holes, truncate text or summarize.
3. Validate the exact projected request after each removal: <=256 Messages,
   <=524288 aggregate UTF8 text bytes, <=1048576 sorted compact UTF8 request bytes
   (ensure_ascii=false, including JSON escaping and all identity fields).
   Selected-only input that cannot fit fails before dispatch.
4. Persist the successful window and fingerprint in the creation transaction.
   Reconstruct the exact rows and validate the fingerprint on snapshot and again
   under the final dispatch lock. Retry/restart does not re-run selection/budgeting.
   Existing dispatch marker fingerprints remain authoritative after marking;
   marked recovery remains UNKNOWN without another AI execution.

Managed request fingerprints also pin the wire profile. `PRE_STREAM` omits
`stream_partials` and `max_partial_chars`; `EXT_FALSE_2000` includes `false` and
`2000`; Voice v2 may also use `EXT_TRUE_2000` with `true` and `2000`. Snapshot and
final-lock checks reconstruct these complete candidates from the same immutable
request core, select only the candidate whose full digest equals the stored
projection fingerprint, and reject no-match or inconsistent projection/dispatch
hashes before marking or HTTP. The selected request object's runtime stream value
matches its serialized body. New freezes write the extension values explicitly.
This exact-hash compatibility needs no migration, timestamp choice or data rewrite.
For a cross-service rollout, update AI first, then Conversation; the AI reader
accepts both historical and explicit wire shapes during that interval.

Response cap remains262144 bytes. AI owns model context budgeting after this
communication projection. Changed settings, new admissions and late delivery
evidence never change frozen windows. Scope resolves through the principal's actual
Conversation owner/integration/channel before history reads. Website result identity
also requires the retained exact session/agent. Widget keys are not persisted here.

## Migration and rollback

0004_website_history is additive. Old Turns default to v1/start1 with no freeze hash
or agent; old Messages have no source_agent_turn_id. Existing evidence, drafts and
fingerprints are never converted. API and worker readiness require this exact head
and query the new columns. Downgrade refuses any retained v2 Turn or source-Turn AGENT
Message, including history from a v1 draft. Production rollback is configuration-only
or a compatible forward fix, never destructive schema downgrade.

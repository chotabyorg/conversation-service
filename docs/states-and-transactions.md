# Durable states and transactions

Every service operation owns a fresh independent AsyncSession and a short READ
COMMITTED transaction. Lock order is **Conversation → Turn/Delivery → reconciliation**.
All entity access filters through one actual grant's authorized parent triple.
UUID-only preliminary lookup never authorizes access. Locked reads must refresh
current ORM values after a wait; old identity-map snapshots must not decide leases,
authority or revisions. There are no AI/provider calls inside a transaction.

Identity strings use `COLLATE "C"`. UUID PKs, UTC TIMESTAMPTZ, nonnegative BIGINT
counters, bounded string CHECKs, composite parent FKs and named uniqueness provide
durable defenses. All delete relations RESTRICT/NO ACTION; there is no deletion
API. State strings use explicit CHECKs instead of opaque provider state/PG enums.

## Atomic ingestion and order

1. Authenticate and match one full owner/integration/channel/namespace grant.
2. Validate selected immutable fields and compute server fingerprint. Existing
   event replay returns its original Message; different content conflicts.
3. INSERT Conversation identity ON CONFLICT DO NOTHING, then select/lock the
   winner in a separate statement. Read Committed's INSERT snapshot alone may
   not see an uncommitted conflicting winner.
4. INSERT scoped event ON CONFLICT DO NOTHING. If another worker won, read its
   committed fingerprint/source Message in a subsequent statement; replay or
   rollback conflict, including any newly created candidate Conversation.
5. Only a new event increments the locked Conversation's sequence/input revision
   and creates its unique CUSTOMER/INBOUND Message. Sample DB acceptance time
   once after locking; event/Message/Conversation use that sample.
6. Commit identity/event/counters/Message together before returning success.

`uq_conversation_identity`, `uq_inbound_event_scope`, `uq_message_source_event`
and `uq_message_sequence` protect creation/dedup/order. The event key excludes
thread, so conflicting changed-thread replay cannot leave a phantom Conversation.
Rollback/replay allocates no sequence. Provider timestamp is evidence only. Reads
sort by local sequence and use after_sequence keyset pagination. Each Conversation
has its own serializer; no global/advisory/in-memory counter or lock is used.

## AgentTurn selection and lease

The caller chooses a stable selection_key and inclusive first/last input sequence
with expected current input and authority revisions. Under the parent lock,
retained key lookup precedes checking current history/control/active work: exact
creation replay returns the original Turn (including terminal); changed request
under that key conflicts. New work requires AGENT control, exact snapshots and
valid same-Conversation Message endpoints. Context freezes at current last sequence.
This foundation currently contains only inbound Messages in the selected range.

`uq_turn_selection` retains logical creation identity. Composite Message FKs
retain same-Conversation boundaries. `uq_turn_one_active` is a partial UNIQUE
index on conversation_id WHERE status IN ('PENDING','CLAIMED'). PENDING holds the
active slot; lease expiration never releases it or creates a new Turn.

| State | Meaning/allowed mutation |
| --- | --- |
| PENDING | Stable selected work, epoch0 without proof/start/result; may claim |
| CLAIMED | Current proof, worker, first-start and expiry; may live renew/finish/fail; expired may recover same UUID |
| COMPLETED | Immutable DRAFT_READY+draft or NO_RESPONSE+null; active slot released; winning exact terminal replay only |
| FAILED | Immutable safe failure_code and refs; inspectable terminal; active slot released; winning exact terminal replay only |

Claim locks parent then Turn, samples `clock_timestamp()` afterward, requires
current AGENT control and either PENDING or expired CLAIMED. It increments epoch,
generates secrets.token_urlsafe(32), stores only SHA-256 digest and returns the
43-character bearer only once in that successful response. Recovery retains ID,
input/context/authority snapshots and first started_at. Lost claim response waits
for expiry; claims are not advertised as idempotent.

Renew/first completion/failure require stored epoch and constant-time token-digest
match while `clock < lease_expires_at`, using the post-lock DB clock. A claimant
whose lease expired fails even without takeover. Renewal uses max(old expiry,
clock+requested seconds) and retains proof. Control/input changes do not erase a
processing result; they remain explicit snapshots for future policy.

On a terminal row, only winning stored epoch/token plus the identical terminal
fingerprint may replay successfully, including after expiry. Different claimant
is STALE_CLAIM; changed status/result is TERMINAL_RESULT_CONFLICT. This acknowledges
an already committed result and gives no active work ownership. Terminal cannot
be reclaimed. No token/digest appears in reads or finish/fail output/logging.

## Control authority

AGENT, HUMAN_PENDING and HUMAN are persisted skeleton states, without handoff UI
or workflow. A parent-locked compare-and-set requires exact expected revision.
Every accepted transition increments authority revision once, even same-mode
fencing, and updates time. Stale revision always conflicts; concurrent old-revision
requests have one winner. New claim requires AGENT. Existing held work can finish
without granting stale outbound authority.

## Delivery evidence

Delivery creation locks parent then source Turn. Stable delivery_key lookup is
first: exact replay returns current attempt, changed create payload conflicts.
New creation requires immutable COMPLETED/DRAFT_READY, current AGENT, and expected
authority matching current Conversation and Turn snapshot. It creates GENERATED
revision0. `uq_delivery_source_turn` allows only one attempt per immutable draft;
UNKNOWN cannot generate another attempt. No Message/counter is appended.

Each reconciliation key has its own immutable row, submitted refs/time/error,
fingerprint, expected revision and original result revision. Lookup occurs before
current revision/state checks. Exact old replay returns **current attempt snapshot
plus original applied_revision**, never reverts state; changed old-key payload
conflicts. New keys require exact current revision and legal transitions, then
state/milestones/revision/evidence row commit atomically under locks.

| From | Allowed different new states |
| --- | --- |
| GENERATED | DISPATCH_REQUESTED |
| DISPATCH_REQUESTED | PROVIDER_ACCEPTED, DELIVERED, READ, FAILED, UNKNOWN, PARTIAL, INTERRUPTED |
| PROVIDER_ACCEPTED | DELIVERED, READ, FAILED, UNKNOWN, PARTIAL, INTERRUPTED |
| UNKNOWN | PROVIDER_ACCEPTED, DELIVERED, READ, FAILED, PARTIAL, INTERRUPTED |
| PARTIAL | DELIVERED, READ, FAILED, UNKNOWN, INTERRUPTED |
| INTERRUPTED | DELIVERED, READ, FAILED, UNKNOWN, PARTIAL |
| DELIVERED | READ |
| READ / FAILED | No different target |

Same-state evidence enrichment is permitted with a new key/revision, excluding
GENERATED as a reconciliation target. Entering/reaffirming DISPATCH_REQUESTED
requires AGENT and expected authority equal to attempt snapshot/current parent;
other facts about prior attempts remain journalable after authority changes.
This is evidence journaling, not a send permit.

UNKNOWN means insufficient evidence and may resolve; FAILED means conclusive
failure and cannot silently rewrite to later positive evidence. PARTIAL and
INTERRUPTED are distinct. Proven DELIVERED/READ cannot regress. PROVIDER_ACCEPTED
does not synthesize delivered evidence. Jump to READ sets missing delivered/read
server-known times because read entails delivery; other unsupported milestones
remain null. These are server acceptance times, not invented provider timestamps.

Known provider operation/message ID cannot be replaced. Null may be enriched;
omitted/null update keeps existing known reference. Positive current state clears
safe error_code; prior negative evidence remains in reconciliation history.

Future provider adapters/retries, outbound Message promotion, input freshness,
supersede, settling and managed business safety require separate contracts. Goal 1
retains evidence for them without implementing them.

## Managed processing uncertainty

Goal2 adds four same-Turn nullable marker columns and EXECUTION_UNKNOWN failure
without altering retained creation/terminal proofs or the delivery state machine.
See [managed execution](managed-execution.md) for the snapshot→committed marker→
one HTTP→fenced finish algorithm. AI work holds no DB session or row lock. Marker
and result decisions use the DB clock after the final lock, as claims already do.
Sameepoch duplicate conflicts without terminalizing the live worker. Recovery of
an expired marked Turn increments epoch normally, then records EXECUTION_UNKNOWN
without sending another request. Late displaced work remains STALE_CLAIM.

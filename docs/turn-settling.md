# Goal5 durable turn settling

## Policy and persisted burst lifetime

`TURN_SETTLING_POLICIES` is optional service configuration. A policy has a
version, channel, optional integration override, quiet window, and maximum wait.
There is no implicit timeout. With no matching policy, the existing immediate
one-message path remains available.

Admission resolves policy once and, in the same PostgreSQL transaction that
accepts the event, creates or extends a Conversation-local `SettlingBurst`.
The burst freezes its policy version, quiet and maximum-wait values, opening
time, maximum cutoff, last accepted time, tail sequence, and ready time. Each
member `Message` points to that burst and retains the same policy snapshot.
Replays do not change the canonical Message, burst, timestamps, or settling
revision.

For a burst opened at database time `T0`:

```text
max_cutoff_at = T0 + max_wait
ready_at      = min(T0 + quiet_window, max_cutoff_at)
```

A following accepted input joins the open burst only if it arrives strictly
before the persisted `ready_at`. It advances the burst tail and sets
`ready_at = min(accepted_at + quiet_window, max_cutoff_at)`. An input at equality
or later closes that burst and resolves current policy for a new one. Policy
replacement or removal cannot rewrite an open burst's membership, deadline, or
cutoff; it only applies when a new burst opens. Policy-free Messages remain
IMMEDIATE and are not merged into a burst.

## Migration 0006 and retained history

Upgrade backfills the input revision of each historical CUSTOMER/INBOUND Message
in Conversation order and initializes settling revisions to zero. Existing rows
receive no settling policy or burst membership, so upgrade does not reinterpret
past traffic as debounced work. Historical Website response attempts retain
their original authorization as `send_authorized=true`. For Telegram, the
migration reconstructs authorization only for the first retained response
attempt of each non-GENERATED delivery; an old attempt after that point is not
promoted into new send authority. The account backfill creates `BOUND` only when
all retained MANAGED admissions for an owner/integration name one bot; mixed bot
history becomes `CONFLICT` with no selected bot.

Downgrade takes exclusive locks over the affected journals and refuses whenever
the old schema could not represent retained facts. This includes burst membership
or policy snapshots, nonzero settling revisions, `SUPERSEDED` delivery evidence,
send-permission decisions, settling fences, multiple response attempts for one
delivery, input ownership that cannot be reconstructed exactly, multi-input
immediate Turn mappings, or a Telegram account row that differs from the
historical MANAGED admissions. A refusal leaves the upgraded schema and data in
place; do not remove that evidence manually to force a downgrade.

## Selection, ownership, and restart

The scheduler reads the retained burst deadline after restart; `Message.created_at`
is not scheduling authority. At equality, it closes a due burst at its stored
ready time and selects the oldest unconsumed contiguous prefix, up to 256
canonical inputs. A larger burst's residual keeps the same burst ID, policy,
cutoff, tail history, and ready time across Turns and worker restarts. A following
burst never joins a residual. The selected Turn freezes context through its last
input sequence. No synthetic concatenated Message is created.

Input is globally consumed when it has an explicit `TurnInputMessage` owner or
falls in any retained `AgentTurn` frozen input range, for every Turn status.
Exact selection replay precedes ownership conflict checks. Direct creation,
immediate selection, settling selection, and Website selection use the same
alias-safe consumed predicate. All ownership rows commit atomically with the
Turn. A failure on a later assignment rolls back the Turn and every earlier
assignment; the actual `pk_turn_input_messages` violation maps to
`TURN_SELECTION_CONFLICT`.

The Conversation row lock serializes admission against selection. Under that
lock, the scheduler rechecks authority and the active-Turn slot, validates each
member, freezes projection, and inserts the Turn plus all ownership rows in one
transaction. The partial unique index on active Turn status remains the final
one-active-Turn guard. Generic or malformed oldest inputs remain a FIFO/mode
barrier inside that Conversation; a later input cannot bypass them.

Candidate Conversations are discovered in bounded keyset pages of 100, ordered
by oldest due burst time and Conversation ID. Each candidate is revalidated in
its own short transaction under the Conversation-first lock. Typed candidate
validation/projection failures roll back before paging continues, so a finite
malformed prefix cannot starve a valid tail. Candidates are ranked before
account or delivery eligibility is applied. Unknown database or programming
failures remain visible.

## Telegram provider-account and dispatch recovery

Telegram keeps one immutable provider-account binding per owner/integration.
The first accepted MANAGED admission binds its bot. New MANAGED admissions must
match that binding. Migration reconstructs a single `BOUND` account from
single-bot history and a `CONFLICT` account from mixed-bot history; it never
chooses an arbitrary bot. `CONFLICT` blocks new work but preserves exact
previously authorized provider recovery and late evidence.

New provider permission and recovery are separate. A `SEND_CANDIDATE` requires a
current BOUND account, complete Turn-member identity, and current control and
authority. `RECOVER_ONLY` identifies the exact prior authorized attempt,
admission, and bot, survives authority change or historical account conflict,
and cannot send. Begin may record the existing interrupted-to-UNKNOWN transition
and returns `send=false`; the gateway then stops. A wrong runtime bot cannot
mutate recovery. A correct late provider fact may still resolve the exact
attempt. Marked AI execution recovery also stays on the same Turn and preserves
the existing mark-before-new-account/preparation ordering.

`POST /telegram/results` is observational. It reports coordination mode,
canonical Turn/account correlation, exhaustive delivery state, and
`progress_disposition`: `WAIT`, `TERMINAL`, `AUTHORITY_FENCED`, or
`ACCOUNT_CONFLICT`. Only a fully correlated non-WAIT result releases an
IMMEDIATE gateway waiter. SETTLING does not wait for results. A revoked
authority, missing configuration, or HTTP status alone never fabricates
terminal/provider evidence. Authorized unresolved dispatch remains WAIT even
after revocation; PARTIAL and INTERRUPTED stay distinct from success.

## Supersede and Website response history

Each canonical inbound increments `Conversation.settling_revision`, including
IMMEDIATE input. Before provider/socket dispatch, the adapter locks Conversation,
completed Turn, then delivery and checks current AGENT control, authority, and
the frozen settling revision. It commits either `DISPATCH_REQUESTED` with
`send=true` or terminal `SUPERSEDED` with `send=false`. A new inbound shares the
Conversation lock, so only one side of this decision can win. Superseding
affects communication only; the Turn and opaque `operation_refs` remain
retained, and Conversation never rolls back AI-owned Business effects.

Website v2 derives a stable response-attempt ID from the retained admission ID;
it never rewrites the public `message_id`. Historical UUID4 attempts remain
retained with their original send permissions. Before returning a persisted v2
denial, begin looks for an authorized completed source from the same admission
and delivery. A completed source returns `REPLAY_COMPLETED` with its exact
source-attempt ID and no new socket write. A denied row remains denied. Late
completion records evidence for that exact authorized old attempt and does not
authorize a retry. An unfinished external write becomes `UNKNOWN` when recovered
and is never blindly repeated.

## Focused evidence

| Invariant | Conversation test evidence |
| --- | --- |
| Quiet reset, equality, max wait, and settings snapshots | `tests/test_settling.py` and `tests/test_goal5_burst_recovery.py` |
| 257+/600 members, original cutoff, byte split, fresh-process residual | `tests/test_goal5_burst_recovery.py` |
| 131 malformed candidates, immutable schema, atomic second-owner rollback | `tests/test_goal5_burst_recovery.py` |
| Exact duplicate owner PK name and translation | `tests/test_turns.py::test_actual_turn_input_primary_key_conflict_is_translated_after_rollback` |
| Historical Website attempts, denied-v2 late success, scoped IDs | `tests/test_goal5_website_recovery.py` |
| Stale undispatched Website v2 attempt persists SUPERSEDED fence | `tests/test_website.py::test_stale_settling_v2_begin_persists_superseded_fence` |
| Telegram account, progress, RECOVER_ONLY and marked-AI recovery | `tests/test_goal5_telegram_recovery.py` |

No Voice adapter, partial transcript, or gateway debounce is introduced. The
existing ManagedTurn v1/v2 and `OperationReference` contracts remain unchanged;
Business-operation policy remains with AI/Business Runtime.

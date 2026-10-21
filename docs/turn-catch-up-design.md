# Bounded queued catch-up: proposed Conversation design

Status: proposed; independent architecture review is required before implementation.
Tracks [Conversation #14](https://github.com/chotabyorg/conversation-service/issues/14).
Baseline: Conversation `301c5aa3818ecc42d01a9a0d3bbea7d245e20925` above main
`bdb6e138a7e58bd38c189446574be45fe3e05627`; AI compatibility candidate is #82.
This document changes no runtime, wire contract, migration or channel enrollment.

## Product behavior

While T1 is running, M2, M3 and M4 can arrive in different temporal bursts. If no
canonical assistant/HUMAN/SYSTEM boundary intervenes and the inputs are otherwise
eligible, the next Turn should own all three original messages in order. The
customer receives one current response. The original temporal bursts remain
immutable evidence; catch-up is an additional selection decision, not admission
debouncing and not a change to the quiet window.

The current scheduler in `services/settling.py` stops at the first burst ID.
`_validate_settled_members` and `services/effects.py::_input_complete` deliberately
reject multiple burst IDs. Simply removing that equality check is unsafe:
`_split_floor` also correlates split evidence through the Turn's single burst ID,
and delivery freshness currently uses the selection-time settling revision even
when older queued work does not own the latest accepted input.

## Authority and ownership

Conversation owns grouping, canonical ordering, Turn input assignment, authority
and communication send decisions. AI retains the one BusinessOperation ledger,
proposal semantics and provider recovery. The existing managed-v2 projection can
carry the selected canonical inputs; a new wire profile is not required for this
selection feature. No canonical Message is concatenated, replaced or deleted.

Ingress, selection, authority transitions and fresh effect/send decisions remain
serialized under the Conversation row lock. The existing active-Turn unique index
and `TurnInputMessage` uniqueness remain the final database fences. Retained
terminal results and already-committed permit replay keep their precedence.

## Eligibility and boundaries

Initially enable catch-up only for explicitly configured managed SETTLING text
bindings. IMMEDIATE and Voice paths retain their current behavior. A group starts
at the oldest unconsumed input above the current authority input floor. It may
include consecutive canonical CUSTOMER/INBOUND messages from due, closed original
bursts with identical retained settling policy snapshots and the same admitted
agent/channel/provider-account identity.

Stop at the first non-customer canonical message, sequence gap, unadmitted input,
IMMEDIATE input, policy change, different agent, invalid account, authority floor,
not-yet-due burst, or projection bound. A malformed oldest input stays a FIFO
barrier in its own Conversation; keyset discovery must still progress to other
Conversations. Never group inputs from different Conversations or merge customer
identities across channels.

The selector must not postpone a due burst to wait for a later burst. Reuse the
existing persisted deadlines, 256-message/request-byte limits and bounded keyset
pages. New arrivals create later work and cannot rewrite a committed selection.

## Durable representation

Propose additive Conversation-owned selection evidence:

- `CatchUpSelection`: one immutable row per newly selected opt-in SETTLING text
  Turn, including single-burst selections and bounded prefixes, with selection version,
  Conversation/Turn identity, authority revision and authority input floor,
  selection-time accepted-input/settling revisions, selected range, observed
  latest-customer tail, a canonical membership digest, and a frozen admitted
  identity proof/digest as defined below.
- `CatchUpMember`: ordered immutable references to original bursts, including the
  exact selected first/last sequences and the closed burst's tail/policy snapshot.
  Composite foreign keys fence Conversation, Turn and original burst identity.
- Existing `TurnInputMessage` rows remain the sole per-message input ownership.
  The Turn's existing first-burst reference can remain for legacy inspection, but
  new safety checks must use the explicit membership when a catch-up row exists.

Create the Turn, membership, projection and all input ownership in one transaction.
Recompute and validate exact membership under the lock before commit. A failed
projection/assignment rolls back all new rows. A restart reclaims the same Turn
and reads its retained selection; it never infers a new group from today's clock.
Database immutability and downgrade refusal must cover the new evidence.

### Admitted identity proof

The current admission tables have exact canonical-message links and scope foreign
keys, but do not have database immutability triggers. Do not assume that their
current contents alone prove a historical selection. Freeze a bounded canonical
identity-proof value in `CatchUpSelection`, with one ordered entry per selected
`TurnInputMessage`: message ID/sequence/ordinal, admission ID and fingerprint,
route, owner/integration/agent/channel/namespace and channel identity fields.
Hash this value with the membership proof; do not include customer text, tokens
or credentials. Its length is bounded by the same 256-input limit.

For Website, join `WebsiteAdmission.canonical_message_id=Message.id` and equal
Conversation ID, require MANAGED, exact scope/agent/namespace, and
`session_id=UUID(Conversation.provider_thread_id)`. Retain message/session IDs.
For Telegram, use the corresponding exact `TelegramAdmission` join, require
MANAGED and exact scope/agent/namespace; retain bot/chat/sender/message/update/event
and thread IDs. Validate the existing private-chat/event-ID relationships and
require the same BOUND `TelegramProviderAccount` bot ID for all selected inputs.
Retain that account decision in the proof. A later CONFLICT fences fresh work;
it cannot rewrite the selected identity.

Recompute these joins and compare the entire frozen proof before projection,
each fresh dispatch fence, fresh effect eligibility/permit, and each communication
send/replay decision under the parent lock. Missing, changed or mismatched evidence
fails closed. Include the supplied/current agent in the comparison. Terminal result,
committed permit, and exact original delivery-evidence recovery retain precedence;
none of these comparisons permits a new provider call on UNKNOWN. Every fresh send
entry point, including generic delivery reconciliation, must enforce the same proof
or reject catch-up Turns; channel APIs cannot be bypassed through a FULL principal.

## Bounds and incomplete residuals

Freeze only the largest representable contiguous prefix, at most 256 canonical
messages and within current serialized/UTF-8 limits. Retain the snapshot of the
original burst tail, including when the prefix cuts inside a burst. Remaining
inputs keep their original admission and ownership state for a later bounded Turn.

A prefix missing any required original-burst member, or not owning the latest
accepted CUSTOMER input, remains external-effect-ineligible. It cannot acquire
permission later merely because another Turn consumed the residual or a visible
projection changed. Each selected burst must be complete within this exact Turn
for fresh effects. The current input revision and latest accepted CUSTOMER proof
remain mandatory on every fresh permit.

For catch-up communication, a selected prefix that does not own the current latest
CUSTOMER tail must not emit another delayed customer-facing answer. Retain its
terminal draft/operation references and record a superseded communication decision.
Do not fabricate an old settling revision to force that result. Check the explicit
catch-up completeness/current-tail proof at the existing locked send boundary.

Continuous arrivals must not starve execution: due work may run while stale/incomplete
responses remain suppressed. The oldest-ready Conversation ordering is preserved.
This design guarantees bounded work and no stale response cascade; a response under
an unending input stream is not promised without a stable eligible input boundary.

## Effect and recovery proof

Extend `_input_complete` to validate the exact catch-up membership, all canonical
selected inputs and complete ownership of every listed original burst. Preserve
the existing single-burst/IMMEDIATE paths for historical Turns without a selection
row. A missing, damaged or contradictory catch-up record fails closed.

Extend `_split_floor` to find a candidate associated with any catch-up member burst,
including a non-first burst. Otherwise a split in the second member could escape
the existing stale-confirmation barrier. Do not reinterpret a split as complete
because another Turn later consumed its residual.

Selection and ingress share the existing parent lock. If ingress wins, a fresh
permit must see the new revision/latest input. If a permit commits first, its exact
original operation may finish; it grants no later ordinal or replacement effect.
UNKNOWN stays recover-only. Communication suppression never rolls back an effect.

## Handoff interaction

Handoff uses the same authority row. A change to HUMAN_PENDING/HUMAN fences new AI
selection, fresh permits and send decisions. Group membership cannot cross the
authority input floor established by handoff/Return. Return to AI requires a fresh
post-Return input; it must not schedule old queued inputs as new agent work.
Earlier HUMAN conversation remains context, not fresh confirmation authority.
The old Handoff source must be reconciled with these rules before enrollment.

## Migration and release

Keep the feature disabled by default; old Turns have no catch-up row and preserve
their existing identity. Add only selection evidence, constraints and immutability
guards. Roll out readers/validators before enabling new selection. Enable one
explicit text cohort only after the current pair and deterministic safety gates
pass. Stop new catch-up creation and drain active work before any compatible
rollback. An old binary cannot safely process newly grouped work; downgrade must
refuse while non-representable evidence remains. Never delete evidence to force it.

## Required deterministic acceptance

1. Hold T1 with an explicit barrier, admit M2/M3/M4 in separate fake-clock windows,
   release T1 and prove exactly one next Turn owns all three in canonical order.
2. Compare original burst/membership evidence before/after and after worker restart;
   repeat select/claim/execute and prove stable identity and no second dispatch.
3. Race two workers, selection/ingress and permit/ingress with barriers. Prove one
   active Turn, exact ownership and the existing committed-permit precedence.
4. Exercise byte bounds, exactly 256 and more-than-256 inputs, and a split inside
   a non-first member burst. Every incomplete/stale prefix stays effect-ineligible
   across restart, later residual consumption and pending-proposal TTL expiry.
5. Exercise canonical assistant/HUMAN/SYSTEM boundaries, IMMEDIATE inputs, policy
   changes, authority floor, provider-account conflict and malformed oldest input.
6. Under continued arrivals, verify bounded work, unchanged deadlines and progress
   of another Conversation; no broader quiet windows and no latency claims.
7. Exercise send-versus-ingress/handoff ordering, retained delivery UNKNOWN, late
   evidence and superseded drafts without blind resend or loss of operation refs.
   After a permit commits, change admission/identity evidence in an explicitly
   corrupted fixture: the exact original permit replay retains precedence, while
   fresh permits, a replacement request and new communication sends fail closed.
   Separately hold an original provider writer after its single call has started,
   change identity evidence, and complete/replay its original evidence. Only that
   exact attempt may record completion; restart/recovery must make zero additional
   provider calls. Preserve the separate committed-before-ingress first-dispatch
   case from item 3; do not reinterpret a committed permit as replacement authority.
8. Run the current Business bridge and supported Website/Telegram HTTP suites on
   exact paired revisions, then independent technical/business reviews of changes
   and evidence. Hosted channel acceptance remains a separate paid-QA gate.

## Architecture review decisions

Before implementation, independently confirm the additive membership/identity model, the
non-first-burst split proof, incomplete-prefix communication behavior, the authority
floor interaction, and reader/enablement/rollback ordering. In particular, review
the decision to cover all new opt-in SETTLING selections, including single-burst
bounded prefixes, to make the incomplete-input proof uniform without rewriting
historical Turns. The admitted-identity refinement follows the independent review
of revision `05ae43c4f26b64e37749ea87be4d6f34d4ad8c38`.

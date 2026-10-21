# Shared Conversation Handoff and owner Inbox reconciliation

Status: design candidate, no enrollment or production acceptance. This follows
the current text/Business compatibility work and bounded catch-up PR #28. It
replaces the obsolete integration assumptions of source PR #17; it does not
authorize merging that old head unchanged.

## Reconciled source baseline

| Service | Current main inspected on 2026-10-21 | Old source reference |
| --- | --- | --- |
| Conversation | `bdb6e138a7e58bd38c189446574be45fe3e05627` | #17 `081c5b2040139285eab703c8411b09a5065e4013` |
| AI | `f570d1e3c6e23075b899379e0e23895df512947f` | #59 `4f535432ad023f1675d81195c62a7d7bd33c5c13` |
| Control Plane | `e477f04bd5e8d72d60e6f0250f3830b44b4ca44b` | #25 `d684db9e7bbd9b3425fa1ad6d4bb65710b38a7bb` |
| Telegram gateway | `a94256b1ce71b21e5337ca3fa98a6e0dc64d83ce` | #16 `f0d21f318f58cc29515d85036a7c46fb445503be` |
| Website gateway | `df3b2002bb3c7e48d111098fe7db1b60257850fe` | current managed HTTP response path |
| Chota site | `9db8a21e8231a9df2a907649515ba0b180b85b32` | #69 `787b547a040c2885c7b96beb5b56baf695bb4acb` |
| API gateway | `145efad491d02b6a0932e2210cc806be582fa539` | #42 `def39a00414fffc497e642117a3583ddb9c2e286` |

Conversation integration must include the final accepted #23/#27/#28 heads.
AI integration must include the final accepted #82 head and current Business
runtime. Re-fetch all relevant mains, reconcile and repeat exact-head checks
before every source merge and release. Source, image digest, desired GitOps
revision and observed runtime revision are distinct acceptance facts.

The old Conversation source adds about ten thousand lines across forty files.
Its migration `0009_telegram_handoff_inbox -> 0008_business_effect_permits`
collides with retained Voice migrations. Reimplement an additive migration after
`0011_turn_catch_up`, keeping one Alembic head, all current Voice fields, immutable
catch-up evidence, wire-profile readers, authority floors and permit journals.
Never rewrite deployed migration history to imitate the old branch.

## One authority and one operator lifecycle

Conversation owns control, authority revision, input floor and canonical history.
HandoffCase is a projection of this lifecycle, not another authority service.
One active case per Conversation; completed RETURNED cases remain inspectable.
The owner-only MVP supports list, paginated history, reason, claim, HUMAN reply
and Return to AI. No departments, SLA, teams, routing rules or automatic reassign.

The authenticated account must be the integration owner and AI agent owner.
An "operator" in this MVP is that same owner account acting on a claimed case.
Do not imply multi-user staff access until its own authorization model exists.
Claim records assignment revision; it does not itself authorize an external send.

The lifecycle is:

1. A current managed Turn produces typed HUMAN_HANDOFF control with a stable
   request identity, closed reason and opaque committed operation references.
   Conversation validates its claimed execution, current authority and selected
   latest input proof, then advances authority/input floor and opens the case.
2. New AI dispatch, fresh Business permits and new AI send acquisitions stop.
   Previously committed Business permits and original writer evidence retain
   their established recovery precedence; no permit is revoked or reissued.
3. If an older acquired channel writer is still LIVE, authority is HUMAN_PENDING.
   The case is visible and claimable, but human send waits for its positive end.
   Once drained, authority becomes HUMAN through the same durable lifecycle.
4. Claim and send commands use stable UUIDs plus authority/case/assignment CAS.
   Exact replay returns the original immutable receipt before new eligibility.
   Canonical HUMAN text and its outbound intent commit together exactly once.
5. The channel adapter acquires the intent through the common Conversation send
   journal and returns provider evidence for its original attempt.
6. Return requires HUMAN authority and no live human/AI writer. It retires only
   unacquired human intents, records RETURNED, advances the shared authority and
   sets the input floor to the last accepted sequence. A new CUSTOMER input is
   required before AI may run. Pre-Return history remains context only.

Uncertain Business outcomes remain in the existing BusinessOperation ledger.
The AI may request BUSINESS_UNCERTAIN handoff when safe recovery cannot resolve
the operation; Conversation sees only opaque committed refs. Handoff never
creates a second ledger or a provider retry. An old incomplete catch-up prefix
cannot make a new handoff/send decision merely because residual input was later
consumed. Current latest-input/identity validation still applies.

## Shared outbound journal, channel-specific evidence

Retain the old source's useful model: one ordered intent stream per Conversation,
explicit AGENT_TURN or OUTBOUND_MESSAGE source, immutable text/identity/ordinal,
stable attempt ID and writer supplier/process incarnation. Add Website to this
same model; do not create another Inbox or authority table for its replies.

Telegram's positive sendMessage response proves provider acceptance, not delivery
or reading. Website's positive browser-write completion proves only that write.
Disconnect, timeout or process loss remains UNKNOWN. A different poller cannot
blindly resend it or end a LIVE writer based on elapsed time. Only the matching
original task's positive join/end evidence, or another explicitly designed
authoritative adapter mechanism, releases that writer barrier. Permanent writer
loss must have an operationally reviewable recovery path before production;
silently aging it out is not that path.

Generic FULL delivery/control routes must not bypass enrolled channel checks or
make a case disagree with Conversation authority. Existing exact outcomes and
original late evidence must remain replayable after authority/identity changes.
Retain additive receipts rather than mutating earlier observed evidence.

Website currently returns a reply inside a customer's outstanding HTTP request;
that alone cannot deliver a later owner reply to an idle browser. Add an explicit
bounded poll or stream transport for the same canonical session and outbound
intent journal, with stable message/attempt IDs and browser deduplication. Verify
the existing trusted session-delivery authorization before exposing any history;
a public widget key plus an arbitrary client-supplied session ID must not grant
access to another session. Reconcile this narrow transport requirement with the
separately owned security work; it does not authorize a platform auth redesign.

## Grants and transaction order

Reconcile the existing CP binding and AI agent lifecycle proofs, generations,
stable revoke commands and exact ACKs. No client-supplied owner/header/UUID alone
grants Inbox access. Verify account, owner, integration, channel, agent and both
proof generations as one scope. Preserve revocation high-water marks across
restart; a late provision cannot reactivate an obsolete binding or agent owner.

Do supplier/CP/AI calls outside database transactions. Lock a grant scope before
Conversation roots when both need mutation; multiple roots lock in UUID order,
then Turn/Case, Delivery and receipts. Audit every enrolled admission/selection/
send/revoke path for reverse lock order. Sample the database clock after locks.
Revocation fences new permits and sends using the existing authority/input floor;
it neither discards UNKNOWN outcomes nor pretends an acquired writer ended.

## Managed history and compatibility

Preserve current v1/v2 wire fingerprints and old replay. Reconcile the old typed
v3 control result with current AI/Conversation execution, cancellation/cleanup,
partials, Business confirmation and permit acquisition paths. The v3 reader must
be deployed on AI before Conversation can create v3 work. Keep enrollment empty
until all readers, channel adapters and owner routes are ready.

HUMAN/SYSTEM canonical rows and bounded communication facts let AI distinguish a
draft from accepted, failed or UNKNOWN communication. Return history must not
turn an old human/customer utterance into fresh confirmation authority. Use the
same Business runtime and operation references; do not normalize v3 into a fake
new v2 Turn or fabricate hashes to get past a strict sidecar check.

Catch-up selection evidence remains unchanged and belongs to Conversation.
Future v3 Turns require an explicitly reviewed extension of its version/contract
constraint and validators. A missing record must fail closed. Voice remains on
its accepted contract until the common v3 reader includes its actual stream
profile and playback semantics; text rollout cannot silently change Voice bytes.

## Voice and transcript continuation

Voice retains phone/STT/TTS/partials/playback ownership while using the same
Conversation, Business runtime and HandoffCase. Claim is not proof that a phone
bridge or operator media path connected. Transfer request, positive connection,
failure and UNKNOWN need adapter evidence under the same case/authority. Do not
introduce a Voice-specific handoff authority. Stop/finish acoustic playback with
positive provider evidence before authorizing an operator media writer.

Canonical final CUSTOMER utterances and positively accepted AGENT speech remain
the text history. Interrupted/partial/UNKNOWN playback retains its draft and
playback facts separately; PCM byte counts cannot prove which words were heard.
An Inbox transcript is a projection of those canonical events and delivery facts,
with timing, speaker and call identity. A summary is a derived, replaceable view,
never the source of booking/confirmation authority.

Proposed retention design, not yet applied: configure text/transcript retention
per owner (suggested initial product default 90 days); raw audio off by default,
with a separately enabled bounded TTL if later required. Store optional audio in
encrypted object storage referenced by Conversation-owned metadata, never debug
logs. Product history access uses the same owner scope. Deletion/expiry requires
an explicit dependency design: transcript text/audio/derived summaries and
operational logs may have different lifetimes; immutable permit/Business receipt
identity must not be rewritten. Until a reviewed purge/tombstone mechanism exists,
do not claim that an automated 90-day deletion policy is installed.

## Implementation and acceptance slices

1. Reconcile the shared schema, authority, grant lifecycle and history contracts;
   independently review compatibility and rollout/rollback before enrollment.
2. Telegram plus owner Inbox end to end: AI handoff, visible case, owner claim,
   one HUMAN message to the same client, Return, one new AI-owned request.
3. Website adapter and the same Inbox flow; verify reconnect, original attempt
   recovery and that Telegram/Website identities remain isolated.
4. Voice common case/transfer evidence, current text and Voice regressions,
   canonical transcript persistence and owner history projection.
5. Only after actual ChotaCRM stability, delegate the requested separate Bitrix
   design analysis. Implement provider parity in the existing Business contracts.

Required deterministic barriers: Handoff versus AI dispatch/permit/send; owner
send versus Return/revocation; paused original provider task versus a new poller;
late original evidence after Return; duplicated/conflicting commands; account,
agent, binding and provider identity changes; restart after each commit boundary;
catch-up non-first-burst split and post-Return input floor. A prior committed
Business permit may finish exactly its operation but cannot authorize any new
ordinal or communication. No concurrent AI/human writer may cross the barrier.

Run native PostgreSQL/fake integrations first, then exact paired Linux CI,
independent technical/product review, immutable image publication and reviewed
functional release. Hosted Website/Telegram and Voice calls remain separate
acceptance gates. Current lifetime-budget admission is STOP_ALL_PAID_TESTS;
no live model/widget/TTS/phone traffic is authorized while accounting is incomplete.

Each acceptance checkpoint records exact source/tree/checks, images and observed
runtime, scenario evidence and deliberately deferred demo scope. Instagram,
WhatsApp, the separate security audit and infrastructure redesign remain outside
this track. Functional release changes must avoid the other agent's work.

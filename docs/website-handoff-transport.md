# Website transport for the common Inbox

Design candidate above Conversation `70c77811f6199797e8d6670a26a19ead48ee3fc4`.
No Website Handoff enrollment, public endpoint or production setting is enabled
by this document. The shared authority and writer protocol remains defined in
[common Handoff reconciliation](common-handoff-reconciliation.md).

## Existing path and required boundary

Website gateway `df3b2002bb3c7e48d111098fe7db1b60257850fe` serves a managed
response inside the customer's outstanding `POST /v1/chat`. Conversation owns
admission, immutable v2 Turn projection and the response journal. The browser
widget lives in `chota-site/public/widget.js`; it retains client-generated
session/message UUIDs and exact pending requests. Neither side currently has a
customer session credential. `X-Widget-Key` identifies a public integration, not
the visitor who owns an existing session. Adding history or owner-reply polling
to that authorization would disclose another visitor's conversation.

Introduce one narrow anonymous session capability for the new transport. This
is a delivery boundary, not a replacement account system or the separately owned
platform security audit. Legacy v1/v2 requests and retained identities remain
unchanged. They cannot be promoted to a private mailbox by knowing their UUID.

## Session capability

1. A new public `POST /v1/sessions` accepts only the widget key, not a requested
   existing session ID. Gateway revalidates the integration with Control Plane
   and derives owner/integration/agent from that trusted response.
2. Gateway requests a fresh session from a dedicated private Conversation port.
   Conversation generates the cryptographic UUID and 256-bit random bearer
   capability and atomically reserves that new canonical root and credential.
   An existing root or credential collision fails; there is no attach/takeover
   branch. The credential retains only the digest, exact scope, issue/expiry
   times and the common grant epoch with both supplier proof generations at
   issuance (`cp_binding_generation`, `agent_lifecycle_generation`). Their
   independent revocation high-water marks remain durable in the same shared
   `InboxGrantScope`; do not create a second Website lifecycle store. The raw
   secret is returned once, never logged or placed
   in a URL. A lost create response may create another empty session, not recover
   access to the first session's history.
3. Browser keeps session ID and capability together in tab sessionStorage under
   the full integration/widget identity. When storage is unavailable, keep both
   in memory. Reload cannot invent a credential for an old UUID. Expiry or loss
   starts a new session; it does not expose old history.
4. New mailbox/chat requests carry `X-Widget-Session-Token`. Gateway validates the
   current widget binding and passes the capability through a restricted private
   Website port. Conversation verifies its digest in constant time and the exact
   owner/integration/channel/agent/session before any content read, admission or
   send acquisition. In that same scope-before-root transaction, require the
   common grant to be active at the credential's grant epoch and both retained
   proof generations to match the current shared grant and exceed their
   independent retained revocation high-water marks. Both current supplier
   proofs must validate through the common lifecycle path. Restart, rolled-back
   static config, re-enrollment or one fresh supplier proof cannot revive an
   old capability. Possessing the public key alone is insufficient.
   Capability checks do not grant HUMAN authority, an AI permit or owner access.
5. Expiry/revocation blocks fresh work. The matching original adapter may still
   submit late evidence for an already acquired attempt through its internal
   principal. Evidence replay cannot disclose text to an unauthenticated browser.
   Revoked session bindings and the common grant high-water marks survive restart;
   revocation and re-enrollment never reset or delete those floors. A fresh grant
   epoch requires a fresh session credential, not automatic promotion of an old
   credential. A credential-only issuance/check helper is not content access
   authorization and cannot be exposed until these shared checks are composed.

The additive session table is a credential binding, not a second history or
authority table. Creation and enrollment must retain grant-scope before root
lock order. Use database-clock expiry after locks. Explicit request/response
size and polling bounds are part of the closed DTOs. Revocation does not erase
immutable delivery/Business evidence. No automated transcript purge is implied.

## One authority, case and outbound stream

Extend the existing Inbox grant/case/command channel constraints to exactly
`TELEGRAM | WEBSITE_WIDGET` with a new additive migration after `0013`.
Owner identity, claim/command UUIDs, CAS, grant high-water marks and Return rules
stay shared. Keep the existing Telegram cohort field backward compatible; add
an explicit Website cohort and one channel-aware scope helper. Integration UUID
membership alone must never select another channel or agent.

Extend CP binding proof and AI lifecycle scope validation for Website using the
same lifecycle protocol and generation checks. Do not add a Website grant manager.
Only a capability-bound session under a live reviewed Website grant can select
managed v3. Existing legacy/v2 sessions keep their current wire profile and
cannot expose Handoff tools or owner replies. Reader-first deployment is required.

Refactor the existing generalized Telegram delivery implementation into common
ordered-intent validation plus thin channel adapters; do not copy its authority
checks into a parallel Website ledger. HUMAN creation remains the existing
atomic Message + DeliveryAttempt + Inbox command receipt transaction. The same
stream carries AGENT_TURN and OUTBOUND_MESSAGE sources. Channel-specific identity
and evidence validation remain explicit. Telegram bot IDs have no Website meaning.

## Idle delivery and honest evidence

The new browser path polls independently of sending customer messages. Polling
selects at most one bounded eligible intent for its authenticated session. The
gateway acquires that exact intent in the common journal before a socket write,
with stable source/intent/ordinal/attempt plus supplier/process incarnation.
The frame carries a stable opaque public delivery ID for browser deduplication;
canonical Conversation, Turn, intent, attempt and supplier/process IDs stay
private. The ID is retained with the original frame, not regenerated on retry.
No new
Turn, CRM operation, proposal or permit is created to deliver a HUMAN reply.

| Event | Retained meaning |
| --- | --- |
| No eligible intent | Empty poll; no writer acquired |
| Positive begin | One authorized write for that exact original attempt |
| HTTP write completed | Transport acceptance only; not read or human delivery |
| Confirmed failure of the original attempt | FAILED only with positive failure evidence from that attempt; never infer success |
| Disconnect or uncertain completion | UNKNOWN without positive original failure evidence; never infer failure or success |
| Retried poll after an uncertain acquired write | Recovery metadata only; no blind second send |
| Exact replay of an already completed public response | Same retained frame identity/content; browser deduplicates it |
| Positive task join or trusted retirement proof | Ends only the original writer; does not change the provider outcome or resend |

Define a closed idle-poll DTO before implementation. Keep PENDING, DRAFT_READY,
NO_RESPONSE, FAILED and UNKNOWN distinct in the existing private result path;
the new transport must not collapse them into failure or a resend instruction.
Wrong/expired credentials, wrong scope and unknown retained identities preserve
the existing indistinguishable 404 behavior. Public recovery responses expose
only bounded delivery state and the opaque public delivery ID; no internal IDs,
canonical pending text, credential digests or execution metadata.

An authenticated history read must not become an alternative delivery path for
unacquired or UNKNOWN outbound intents. Visitor-facing history includes its own
accepted inbound text and previously accepted outbound frames. Pending/UNKNOWN
HUMAN text stays visible to the owner with its honest delivery state. If the
browser positively retained a frame before an ACK was lost, its stable local
identity prevents a second visible bubble; this does not prove the old gateway
writer ended. A future browser ACK extension needs its own closed receipt
contract and cannot silently turn a timeout into a new send permission.

Return waits for the same positive writer barrier as Telegram, retires only
unacquired HUMAN intents and requires a new CUSTOMER input for AI. New input,
Handoff, Return and provider completion races preserve original replay priority,
authority/input floors and catch-up proof. A reload or second browser tab cannot
take over a LIVE writer. Permanent gateway loss uses the shared retirement
verifier; the trusted production proof source remains an enrollment gate for
both channels. No heartbeat, missing pod or browser ACK substitutes for it.

## Implementation slices and acceptance

1. Session-capability DTO/table/private API and gateway issuance/validation,
   default off. Prove no old-session attach, wrong capability/scope/generation,
   expiry, malformed/oversized requests, lost create response and restart.
2. Shared channel-aware Inbox/proof contract and additive migration. Prove old
   Telegram/v2 fixtures and fingerprints unchanged, exact grant replays/revokes,
   owner isolation and transaction lock order.
3. Website v3 worker/history and common ordered intent adapter, then bounded
   idle poll transport and browser dedup/recovery. Prove no network call while
   database locks are held and positive cleanup on cancellation/shutdown.
4. Real HTTP/PG acceptance: capability session -> AI -> HUMAN -> owner Inbox ->
   claim -> one human reply in the idle browser -> Return -> new AI input.
   Include process loss after acquire, late original evidence, conflicting
   commands, two tabs, reconnect, stale input and Telegram/Website isolation.
5. Repeat the actual ChotaCRM matrix, bounded catch-up, existing Voice profiles,
   UI interaction/responsive checks and exact image/GitOps/runtime verification.
   Paid live-model/widget/phone QA remains denied by the project lifetime gate.

This contract is not Website source/integration readiness. It identifies the
required authorization and transport work before implementation and review.

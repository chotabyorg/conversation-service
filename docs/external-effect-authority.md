# External-effect authority bridge

Conversation provides a private, opaque permit boundary for AI Business Runtime.
It owns only the Conversation, Turn, input-completeness and authority evidence
needed to decide whether one operation may begin. AI remains the owner of
Business plans, confirmation meaning, operation status, provider commands,
receipts and reconciliation. Conversation performs no provider or CRM call and
stores no business parameters or outcome.

## Authentication and DTOs

The `BUSINESS_EFFECT` role can `POST` only to
`/internal/v1/external-effect-eligibility` and
`/internal/v1/external-effect-permits`. Its `CONVERSATION_AUTH_PRINCIPALS` entry
uses one or more existing complete owner/integration/channel/namespace grants.
Scope joins use the complete owner/integration/channel tuple; caller-supplied
`X-User-Id` or `X-User-Role` has no effect. `FULL` retains administrative
compatibility. Website, Telegram, Voice and processing roles cannot call these
routes.

Both requests require the same strict context:

```json
{
  "contract_version": 1,
  "conversation_id": "uuid",
  "turn_id": "uuid",
  "origin_turn_id": "uuid",
  "owner_id": "exact-owner",
  "integration_id": "uuid",
  "channel": "TELEGRAM",
  "agent_id": "uuid",
  "authority_revision": 0,
  "managed_request_fingerprint": "64 lowercase hexadecimal characters"
}
```

The fingerprint is Conversation's existing canonical hash for the immutable
managed request: SHA-256 of sorted compact UTF-8 JSON for
`{execution_namespace, request}`. It is different from AI Business Runtime's
local digest and does not change either ManagedTurn contract. The permit request
adds `operation_namespace="main-chota.ai-business.v1"`, the opaque `operation_id`
UUID, and a lowercase SHA-256 `operation_fingerprint` over the frozen provider
command. No customer text, business arguments, provider URL, credentials or
receipt is accepted.

Eligibility is observational and returns HTTP 200 with
`{contract_version,request_fingerprint,eligible,reason,authority_revision,
authority_input_floor_sequence,split_input_floor_sequence}`. A valid but
ineligible request has `eligible=false` and one closed reason: `AUTHORITY_CHANGED`,
`EXECUTION_INACTIVE`, `INPUT_BEFORE_AUTHORITY`, `INPUT_INCOMPLETE`,
`INPUT_SUPERSEDED`, `ORIGIN_STALE`, `ORIGIN_INCOMPLETE` or `INTERVENING_SPLIT`.
`ELIGIBLE` is used only with `eligible=true`. The request fingerprint is SHA-256
of the full canonical sidecar DTO; it is not authorization evidence.

Permit success returns HTTP 200 with
`{contract_version,request_fingerprint,permit_id,replayed}`. A fresh denial is
HTTP 409 with `{detail:{code:"EXTERNAL_EFFECT_DENIED",reason}}`. A valid scope
with a contradictory frozen managed marker returns
`{detail:{code:"EXTERNAL_EFFECT_CONTEXT_CONFLICT"}}`; operation rebinding or the
per-Turn cap returns `{detail:{code:"PERMIT_CONFLICT"}}`. Missing, malformed,
wrong or duplicated service credentials return 401 before body parsing. A
nonexistent or out-of-grant entity returns 404. Callers treat any undocumented
response or ambiguous transport/commit outcome as unavailable authority.

## Decision and transaction boundary

The parent Conversation row is locked for input acceptance, authority changes,
eligibility and permit issuance. The current and origin Turns are then locked in
stable UUID order. Under those locks the service checks the stored managed agent
and request fingerprint, exact Conversation scope, current AGENT authority,
current claimed execution and lease, and the origin's same-epoch provenance.

Every authority revision increment also writes
`authority_input_floor_sequence=last_message_sequence` in the same transaction,
including same-mode control changes and Voice `close_call`. Migration 0008 keeps
the floor at zero for epoch zero and backfills every existing nonzero epoch to its
current message tail. Thus pre-migration customer input cannot be reinterpreted as
fresh authority; new post-migration input can.

Current input must be complete and current in two independent ways. Its frozen
`input_revision` must equal the Conversation revision, and its selected Turn must
own the newest canonical inbound CUSTOMER Message, whose own `input_revision` is
the current revision. This catches queued older Turns whose selection-time
revision was stamped after newer messages had already arrived. Every selected
customer input must also be later than the authority floor. AGENT history does
not increment customer freshness.

A settling Turn is complete only when its burst is closed, it selects at least
one customer input, and it owns every canonical customer member of that burst.
Queries examine the full durable membership and ownership set without the
ManagedTurn 256-message projection limit. Residual chunks and projection-limited
prefixes remain read-only, including after restart. Policy-free Turns keep their
existing selection path but still need exact customer ownership and newest-input
proof before a business effect.

For an origin/current pair, the service finds the latest closed burst between
their input tails that has any selected Turn owning only part of its canonical
customer membership. Such an intervening split invalidates the old proposal even
when its origin and a later confirmation Turn are individually complete. A fresh
complete Turn after the split can follow normal AI capability policy.

Exact replay of an already committed `(operation_namespace, operation_id)` permit
is checked before current lease, authority, freshness, completeness or split
conditions. It returns the original permit only when the complete request and
operation fingerprints match; changed content conflicts, and a foreign-scope
identity remains 404. Fresh permits are immutable, capped at 32 per Turn, and
commit before the positive response. That commit is the authority linearization
point: an authority/input fence committed first prevents a new permit; a permit
committed first survives a later authority change for that same opaque operation.
Conversation does not wait for provider completion or revoke an issued permit.

## Queue limitation

Turns selected from distinct temporal bursts continue to drain FIFO as separate
Turns. They are not coalesced into a catch-up Turn. Track the bounded follow-up in
[conversation-service issue #14](https://github.com/chotabyorg/conversation-service/issues/14);
its design and independent review are required before broad tester rollout. The
follow-up must preserve complete-input denial for stale or residual bursts.

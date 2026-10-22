# Website session capability foundation

This private source slice is disabled by default. It does not expose a public
route, enable Website Handoff, change existing Website v2 admission or claim
visitor authorization for old UUID-only sessions. No model or provider call is
part of credential issuance.

The trusted Website gateway will resolve the current integration with Control
Plane before calling `POST /internal/v1/website/sessions`. The private request
contains only owner, integration, agent and the trusted binding generation.
Conversation generates both the fresh UUID and 256-bit secret, atomically creates
an empty canonical root and its exact credential binding, and returns the secret
once with `Cache-Control: no-store`. A lost response means a new empty session;
there is no endpoint to attach to or recover an existing root. Only SHA256 of the
secret is retained. Credential collision never authorizes old history.

`WEBSITE_SESSION_CAPABILITIES_ENABLED` defaults false. Enabling this private port
requires every `WEBSITE_BINDINGS` entry to pin `capability_generation`; old configs
remain valid while disabled. A generation comes from trusted server deployment /
Control Plane binding coordination, never from the visitor. An old-generation
credential is unusable after that server binding advances. Removing a binding
or disabling the port also denies access. The gateway's dynamic CP validation is
still required in the transport implementation; this source slice is not proof
of live CP freshness.

`WEBSITE_SESSION_TTL_SECONDS` is server controlled, 60-86400 seconds (default 24h).
The DB clock sets issue/expiry and is checked after the canonical root lock.
`locked_credential` checks the credential, current server generation, agent, session,
digest, revocation and expiry inside the caller's transaction. All credential
revocation writers must acquire this same root lock. Future Handoff paths acquire
the common Inbox grant scope first, then this root, and keep the transaction
through the protected content read/admit/send. A standalone token check followed
by another transaction must never authorize content.

The migration is additive; rollback keeps this schema and credential evidence.
Downgrade refuses to delete any retained credential binding. No parallel history,
authority, Inbox grant manager, delivery ledger or BusinessOperation is created.

Remaining transport work includes browser tab storage and header propagation,
CP-backed generation lifecycle, authenticated canonical history, shared Website
Inbox enrollment and writer barriers, bounded idle delivery and original-intent
recovery. Those must ship together behind their own empty cohort; do not enable
Handoff merely because this issuance port exists.

Credential validation alone grants no content access. The transport must also check
the shared active grant epoch, both supplier proofs and their durable revocation
floors before any content read/admit/send; this helper is not that combined gate.

## Common grant credential gate

`locked_access` composes visitor authentication with the existing `InboxGrantScope`,
shared provision receipt and current CP/AI proofs. It requires a caller-owned
transaction and retains the common scope lock before the canonical root lock
through protected use. It does not expose a public content or delivery endpoint.
Content access selects an existing scope with `FOR SHARE`, allowing distinct
visitors' proof requests and protected use to proceed concurrently. It never
inserts a scope. Issuance and lifecycle mutation retain `FOR UPDATE`, so revoke
still waits for protected use and subsequent readers observe the new epoch.
Issuance under an active common grant records its grant generation, AI lifecycle
generation and original provision command UUID; the existing binding generation
records CP's generation. CP proof reuses that retained command identity.

Wrong visitors are rejected before supplier I/O. A valid visitor still needs both
fresh proofs, matching account/agent identity, the exact retained provision receipt,
and generations strictly above both shared revocation floors. Supplier outage
fails closed. Revoke and reenrollment cannot promote an old credential into a new
epoch, including after restart or stale configuration rollback. Foundation
credentials issued without a grant retain null epoch fields and cannot use this
content gate after later enrollment. The migration does not backfill authority.

The schema can read Website cases and lifecycle receipts, but fresh Website common
provision/revoke remains rejected before supplier calls. Enabling it requires the
remaining legacy Website fencing, common managed v3 execution, idle delivery and
writer retirement work. No new lifecycle store or delivery mechanism is introduced.
Migration 0015 refuses downgrade while any Website credential or common Website
scope, lifecycle receipt, case or command receipt remains. It locks writers before
checking retained evidence; an empty schema can still downgrade and upgrade.
Before enabling issuance, prepare a compatible rollback reader or forward fix.

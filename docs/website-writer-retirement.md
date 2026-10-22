# Website recovery through the common writer journal

The existing supervisor-only `POST /internal/v1/outbound-writers/retire` accepts
the retained Telegram command unchanged and a closed Website command with
`contract_version: 1` and `channel: WEBSITE_WIDGET`. Website identifies the exact
owner/integration/agent/binding, canonical Conversation/session, outbound
descriptor, supplier/process incarnation, expected delivery revision, command
UUID and immutable proof reference. It carries neither visitor credentials nor
poll leases. The response remains the existing immutable `WriterRetired` receipt.

Only the separately authenticated retirement supervisor reaches this route.
Website locks an already retained exact grant scope, then its root and original
intent. It requires the retained provision-backed session capability and validates
every descriptor field through the same Website delivery loader. It never creates
a scope, root, capability, Handoff case or send permission. This evidence path
remains usable after credential expiry, grant revocation or cohort configuration
rollback; it does not authorize fresh channel execution under those conditions.

The same external termination-proof read occurs without a database checkout or
Conversation lock. The Website proof must echo every command field and its
canonical fingerprint and positively establish fencing and task termination.
False or mismatched evidence fails closed. A fresh transaction reacquires the
same scope/root/intent, checks the original writer and delivery revision again,
and commits the existing retirement receipt. Original completion during the proof
read wins; stale retirement cannot override its revision or join.

An unresolved dispatch becomes UNKNOWN; retiring a writer never establishes
provider acceptance, delivery or a safe resend. An already observed outcome is
preserved. The next ordered HUMAN reply or Return can proceed after that positive
writer retirement. The original late transport evidence may refine the original
outcome, while the writer remains RETIRED and its retirement receipt unchanged.
Exact command replay precedes a new supplier read and survives a database-pool
restart; conflicting command reuse remains a conflict.

Native verification uses actual common provisioning, PostgreSQL, Website private
routes, Handoff/Inbox and the original-writer completion path. The termination
supplier is a fake. This source does not implement or activate a production
attestor. A production source must bind the process incarnation to a trusted
operating-system identity and positively prove that its writers ended; missing
heartbeats, an expired lease, a pod lookup or a browser acknowledgement are not
such evidence. Lost legacy Website writers without trusted incarnation bindings
still require a separately proved cohort drain. Enrollment remains blocked until
those production proofs and coordinated release acceptance exist.

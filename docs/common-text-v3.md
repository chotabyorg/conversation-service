# One managed v3 executor for text channels

Website and Telegram share the existing managed v3 executor, exact dispatch
snapshot, typed operation references, control evidence and Handoff case creation.
The canonical root's channel selects its existing Inbox grant scope and managed
binding. Claims retain scope-before-root ordering. Recovery verifies the immutable
snapshot's owner, integration and channel as well as its Turn, agent and hashes.
Telegram wire values, fingerprints and lock hooks remain unchanged.

The Website path carries WEBSITE_WIDGET through the request, original-dispatch
evidence and Handoff case. It preserves UNKNOWN references and same-body recovery;
a revoked grant may resolve the original dispatch but cannot create fresh HUMAN
authority. No second Business runtime, operation ledger or Handoff manager exists.

This slice does not enable Website enrollment or select a Website v3 Turn in
production. The private PG tests seed the common enrollment and selected Turn to
exercise the real claim/executor/case transactions. Credential-bearing ingress,
common selection, bounded idle transport and trusted writer termination are still
required before the cohort can open. All external model ports in these tests are
local fakes.

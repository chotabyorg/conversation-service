# Website enrollment in the common Inbox

`WEBSITE_HANDOFF_INTEGRATION_IDS` is a separate, empty-by-default rollout cohort.
It selects Website integrations for the existing Inbox lifecycle, not another
grant manager. The Telegram cohort name, wire errors and request fingerprints
remain unchanged. Owner authentication still uses User Service and the existing
account checks. A Website-only cohort can expose the same owner Inbox routes.

Configuration requires pinned Website capabilities, the configured Website
worker and matching managed binding, an explicit Website settling policy, User
Service and both lifecycle suppliers. A new provision command also checks its
exact owner/integration/agent/binding generation against that current server
configuration before asking either supplier for proof. Exact accepted provision
replay retains precedence over fresh policy and proof checks while the cohort
is selected. Empty/default configuration still rejects first enrollment.

Provision uses the existing current CP/AI proofs, independent retained lifecycle
floors, scope lock, ordered root locks and immutable receipt. It refuses active
legacy Turns, stranded HUMAN roots and unresolved legacy Website HTTP writers.
For a legacy WebsiteResponseAttempt, UNKNOWN remains unresolved even if
finished_at is populated. Only the original positive WRITE_COMPLETED or
WRITE_FAILED evidence releases that legacy barrier. There is no timeout or
blind retry. Permanent loss without such evidence needs a separately reviewed
positive termination recovery; this change does not invent it.

New credential sessions retain the real PROVISION receipt and grant epoch.
They enter the same v3 selector, Business runtime, HandoffCase, ordered delivery
journal and Return command as Telegram. A LIVE common Website writer produces
WEBSITE_WRITER_LIVE; the Telegram error stays unchanged. UNKNOWN plus a positive
original TASK_JOINED may release a writer while preserving its uncertain send
outcome. Browser ACKs and elapsed poll leases do not establish that fact.

An authenticated supplier can revoke an exact retained Website scope after a
cohort configuration rollback. This creates no scope for an unknown integration
and cannot reactivate an old credential. Future fresh provision still requires
the selected cohort and matching current configuration.

Native acceptance exercises actual PostgreSQL provisioning, credential issuance,
input/worker selection, HUMAN authority, owner-only Inbox visibility, claim,
idempotent HUMAN reply, private writer acquisition, UNKNOWN/join, Return and a
new v3 input without a seeded grant. Supplier proofs, User authentication, model
responses and socket evidence are explicitly synthetic. This does not prove
Gateway-to-browser or live provider acceptance.

Production enrollment remains prohibited until the matching Gateway/Widget
releases, intended-peer TLS and privacy evidence, positive legacy writer
retirement, the common trusted termination proof source, full actual-CRM/common
Website integration and permitted live QA have been accepted. No setting,
cohort, daemon or production route is enabled by this source change.

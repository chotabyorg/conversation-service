# Durable Website session creation admission

The private create DTO now requires client_ip derived by the trusted Website
gateway from its verified ingress. Browsers cannot choose an existing session,
an owner/agent binding or this trusted client context. Existing legacy Website
v1/v2 admission/result contracts remain unchanged. The new capability transport
has no production consumers or enrollment yet.

Conversation serializes creation by integration after the common grant scope lock.
Within that transaction it checks the database clock, reserves GCRA admission and
inserts the fresh root and capability. Limits are 5/minute per integration+IP with
burst 2, 60/minute per integration with burst 10, and 1,000 unexpired credentials per
integration. Quota includes revoked credentials until expiry. Denial returns 429
with bounded Retry-After and creates no root, capability or new client bucket.

PostgreSQL stores only scoped bucket digests and theoretical arrival timestamps.
No raw IP, text or credential is copied. Digests are pseudonymous rate keys, not
an anonymity guarantee. Idle debt-free buckets older than two minutes are pruned
on the next admitted creation; the index bounds per-integration lookup. IPv4-mapped
IPv6 shares the canonical IPv4 bucket. Multiple workers and process restart do not
reset limits. Agent/grant epoch changes retain the integration's quota.

Creation collision or failed persistence rolls back all reservations. This is an
atomic anonymous admission control, not a BusinessOperation or Handoff ledger.
Migration0017 starts empty and refuses retained admission rows on downgrade;
use a compatible reader or forward fix after use.

Gateway public HTTPS and trusted client-IP validation, verified private TLS,
durable poll admission, original socket/task writer ownership, browser integration
and the shared trusted retirement proof source remain required before enrollment.
This change enables no production cohort, paid experiment or public route.

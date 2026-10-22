# Durable Website poll admission

The private credential API reserves at most one poll for a canonical visitor root,
using the PostgreSQL clock after the existing scope/root locks. A GCRA counter
allows 30 starts/minute with burst2; its debt survives end, process restart and
supplier changes. Each grant lasts at most20s. The mutable current reservation and
rate debt contain no text, IP or raw credential, and are not a delivery or Business
operation ledger. Migration0018 starts empty and refuses downgrade with retained
poll admission; preserve a compatible reader.

`/sessions/polls/begin` accepts a private request UUID and supplier/process IDs.
Only the first success returns a fresh server-generated poll UUID/deadline.
Replay of the retained request returns RECOVER_ONLY with no executable grant.
Another in-flight request is429 with bounded Retry-After. All starts require the
current visitor credential, common grant and both fresh supplier proofs.

Delivery work and begin require that exact unended/unexpired poll and its original
supplier/process, checked under the same root lock as content selection/writer
acquisition. An old poll cannot resume after its deadline, acquire a writer or end
a newer poll. Private original poll end is metadata-only and stays possible after
revoke/config removal with the original credential/supplier/process identity.

**Poll end or expiry never ends a delivery writer.** A new poll may discover only
recovery metadata for the original LIVE/UNKNOWN writer; the shared delivery ledger
still blocks a second send. Original delivery completion is independent of poll
expiry/end and retains its original attempt/supplier/process evidence. Only the
existing positive writer join/trusted retirement mechanism ends that writer.

No public poll/socket adapter, production cohort or enrollment is introduced here.
The Gateway must implement its own bounded wait, cancellation and owned socket
write, then preserve original completion even after the poll resource is released.
The private Gateway DTOs must be upgraded before using these poll-bound ports.

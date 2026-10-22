# Private Website idle delivery adapter

The trusted WEBSITE caller uses three closed POST ports under
`/internal/v1/website/sessions/delivery`: `work`, `begin` and `complete`.
`work` requires the visitor credential, exact current binding, retained common
grant epoch and fresh Control Plane/AI proofs. It yields zero or one ordered
candidate. Discovery does not acquire a writer or authorize a socket write.

`begin` repeats that authorization before acquiring the exact common
DeliveryAttempt. Source/intent/ordinal/attempt identity stays private. A fixed
one-way UUID derivation from the retained intent gives the stable opaque public
delivery ID; retry never generates a new ID or new intent. The source text stays
in the canonical immutable Message or completed Turn rather than another mailbox.
The Gateway must expose a frame only after positive begin, and deduplicate that
stable public ID in the browser.

Both discovery and the complete prospective begin response are measured as
serialized UTF-8 JSON, including escaping and metadata, against 65,536 bytes.
An oversized frame returns bounded FRAME_TOO_LARGE metadata without text or a
writer. Under the same root lock, both discovery and direct begin record a
terminal SUPERSEDED reconciliation and return FRAME_TOO_LARGE metadata. The
immutable original text remains available to the owner; it is never truncated,
sent, or marked accepted. Repeating begin cannot acquire that intent, and the
next ordered intent can proceed. An owner may send a shorter new message.
A LIVE writer anywhere in the root blocks a new acquisition, including a
writer with a later or legacy missing ordering ordinal. Ordered unacquired
predecessors remain barriers too. No lease, timeout or browser ACK ends a writer.

`complete` records only the original acquired writer's outcome and optional
positive TASK_JOINED evidence. It requires the retained visitor secret and full
private attempt and supplier/process identity. It can accept late evidence after
credential expiry, revocation or settings removal; it returns metadata only and
cannot read pending text or grant another send. WRITE_COMPLETED means HTTP
transport acceptance, stored as PROVIDER_ACCEPTED in the common ledger; it never
means read or human delivery. WRITE_FAILED requires positive original rejection;
disconnects and uncertain writes remain UNKNOWN. Positive task join changes only
writer termination, independently from that outcome. Recovery responses contain
no text and are at most 4,096 bytes.

These private ports do not implement the public Gateway, long polling, visitor
history, rate admission, intended-peer TLS or the actual socket writer. The
Gateway must prove original-task completion/join and never synthesize it after
restart. Permanent process/node loss uses the shared trusted retirement proof,
which is still a rollout gate. Website provisioning stays disabled and cohorts
stay empty. No paid model traffic or production migration is enabled here.

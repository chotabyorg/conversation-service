# Credential Website history

The private `POST /internal/v1/website/sessions/history` port authenticates the
current visitor capability and common grant, using fresh CP/AI proofs before any
content read. It accepts only the exact Website scope plus an optional opaque UUID
cursor. This source adds no public Gateway route or production enrollment.

Content comes from canonical Messages. Accepted customer admissions are visible;
AGENT/HUMAN output becomes visible only after original positive transport evidence
in the common DeliveryAttempt ledger. Pending, failed and UNKNOWN output is hidden.
Transport acceptance does not claim that a person read the message.

An additive append-only index retains only canonical message/intent references,
an ordinal and an opaque public ID. It copies no text and is not another operation
ledger. Under the canonical root lock, each read materializes at most 100 newly
eligible messages. A late original acceptance appends a visibility position even
when the canonical message sequence precedes the caller's cursor. The owner Inbox
continues using the canonical timeline. This projection's ordering is retained
visibility order, not an assertion about physical delivery time.

Public outbound IDs equal the idle delivery frame ID, allowing browser deduplication
across polling, history and reload. Customer records include the caller's original
message UUID for optimistic reconciliation. Internal root, Turn, Message and intent
IDs are not exposed. Cursors resolve only inside the currently authenticated root.
No process-local cache or caller-provided signing key is involved.

Pages contain at most 50 messages and 262144 bytes of complete UTF-8 JSON, including
escaping and continuation metadata. A non-null next_cursor means continue; callers
may retain the last returned public message ID as their incremental checkpoint even
when next_cursor is null. Oversized first content returns FRAME_TOO_LARGE with the
original cursor, no text and no advancement. Content is never truncated or skipped.

Migration 0016 creates an empty index; it promotes no credential, grant or cohort.
Downgrade refuses retained positions to preserve cursors. Roll back application
code using a compatible reader or forward fix after first use.

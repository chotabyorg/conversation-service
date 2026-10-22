# Private credential input

`POST /internal/v1/website/sessions/admissions` accepts input only after the
existing visitor/common-grant gate validates the exact credential, retained epoch,
both fresh supplier proofs and revocation floors. Scope and canonical root locks
remain held until the journal transaction commits. A credential check never
authorizes an append in a later transaction.

The adapter reuses WebsiteAdmission, InboundEvent, Message, input/settling revisions
and canonical membership freezing. The UUID-only and credential adapters call the
same journal append helper. Raw visitor secrets enter neither fingerprints nor
receipts. Duplicate message UUIDs serialize and replay the exact original receipt;
changed content or another session conflicts. Every retry must still pass current
credential and grant checks. Supplier outage fails closed without appending input.

Input while HUMAN remains canonical context. Post-Return input starts a distinct
authority burst even while an older quiet window remains open. This does not
select a Turn or authorize a reply.

The endpoint is private and keeps the existing feature disabled by default.
Production Website grant enrollment, public gateway/header/rate/peer validation,
common v3 selection, idle delivery and trusted writer retirement are still gates.

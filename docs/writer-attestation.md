# Text writer attestation

This additive lifecycle journal supports the existing common text delivery journal.
It creates no Conversation, authority, delivery, grant, Business operation or send
permission. Migration `0019_writer_attestation` adds append-only registrations,
positive witnesses and exact retirement commands. A separate singleton
`writer_recovery_scan` holds mutable traversal position only; it has no authority,
outcome, permit or delivery fields. Migration creates no historical evidence, and
downgrade refuses retained registrations. Software rollback must retain a reader
that honors CLOSED evidence even when `WRITER_ATTESTATION_REQUIRED=false`.

## Private credentials and contracts

`WRITER_PRODUCERS` is JSON with up to 32 dedicated parents:
`[{"principal_id":"telegram-parent","token":"<independent random secret>","grants":[{"owner_id":"exact owner","integration_id":"UUID","agent_id":"UUID","channel":"TELEGRAM"}]}]`.
Channel is exactly `TELEGRAM` or `WEBSITE_WIDGET`; all four grant fields match.
The token is accepted only as `X-Writer-Producer-Token` on register/witness POSTs.
Ordinary gateway, FULL, owner, proof-reader and retirement tokens cannot upload.
A parent retains this secret; the gateway child gets only local IPC capability.
Duplicate/overlapping network credentials fail startup.

`POST /internal/v1/writer-attestations/register`:
`{contract_version:1,registration_id:UUID,descriptor:...}`. Telegram descriptor is
the exact existing `TelegramBeginRequest`, including version 2 and work kind,
without text. Website descriptor is `WebsiteRegistrationIdentity`: exact
owner/integration/agent/binding_generation, version 1, channel WEBSITE_WIDGET,
session_id, supplier/process IDs, and the existing WebsiteDeliveryDescriptor
fields. It excludes conversation_id, session_token, poll credentials and text.
Conversation resolves only the existing root/capability and stores canonical
conversation_id internally. Existing gateway work/begin schemas are unchanged.
Several supplier/process contenders may register for the same existing GENERATED
intent. No registration reserves or acquires send. New provenance after acquisition
is rejected. Repeated registration_id accepts only its original descriptor/producer.

`POST /internal/v1/writer-attestations/witness`:
`{contract_version:1,registration_id:UUID,proof_ref:UUID,proof_kind:PROCESS_TERMINATED|TASKS_FENCED_AND_JOINED,fenced:true,tasks_ended:true,observed_at:UTC}`.
Booleans are strict. Only the original scoped producer may close its registration.
One exact immutable witness is retained; replay must use the same complete payload,
including timestamp/proof_ref. If a task witness already closed the slot, a later
process exit must reuse that retained observation rather than replace it.
Both routes return `{contract_version:1,registration_id,state:OPEN|CLOSED,replayed:boolean}`.
Witness insertion and actual begin use the same scope/root locks. Begin after
closure never sends; begin committed before closure remains LIVE until original
completion or existing proof-based retirement. CLOSED checks remain active when
the enforcement flag is disabled. With enforcement enabled, missing registration
returns no send. Replay of begin never authorizes an additional write.

`POST /internal/v1/writer-termination/proof` uses only
`X-Writer-Supervisor-Token` and the existing strict RetireCommand payload. It reads
immutable positive witnesses and validates the exact acquired supplier/process,
intent, response attempt and descriptor; losing contenders never get proof. It
returns the existing echoed WriterTerminationProof/WebsiteWriterTerminationProof
with canonical request SHA256. There is no external I/O, mutation, scope creation,
lock or inference of termination in this route.

`POST /internal/v1/outbound-writers/recovery` uses only
`X-Writer-Retirement-Token` and `{contract_version:1,registration_id:UUID}`.
Returns `{contract_version:1,state:PENDING|COMMAND|DONE,command:null|RetireCommand}`.
A matching positive witness plus LIVE acquired writer creates the complete exact
command under original scope/root locks before any submission. Restart/lost ACK
returns that identity even after retirement changed the revision. If the command
has no retirement receipt and a changed revision is positively observed under the
same locks, the old command cannot have committed retirement: retain it and create
a new revision-bound attempt. No ambiguous identity is overwritten. Normal joined
completion requires no retirement. Response contains no text or visitor credentials.

## Driver and rollout

Run `python retirement_worker.py` as a separate worker using the existing service
image and database, with dedicated WRITER_RETIREMENT_TOKEN, WRITER_SUPERVISOR_TOKEN
and WRITER_SUPERVISOR_BASE_URL pointing to the Conversation API. The bounded sweep
selects at most 100 matching LIVE witnessed writers using a durable UUID keyset
cursor shared by all drivers. The short cursor transaction commits progress before
attempting the page and releases its DB session. Tail exhaustion wraps to the
beginning, so earlier inserts and failed rows remain eligible; a restart can skip
an unfinished page until the next wrap, but cannot remove its evidence or command.
A persistent failed page cannot starve later eligible writers. The driver then
persists/reads exact recovery commands, then calls the existing retirement consumer.
That consumer reads proof with an eight-second deadline outside every DB session
and re-locks/revalidates afterward; original completion and retirement receipt
precedence remain unchanged. A sweep repeats every five seconds.
Database/connection/timeout failures at the
snapshot and row boundaries retain all uncertain state and emit finite sanitized
retry diagnostics. Readiness/outage retries wait five seconds and wake on shutdown;
cancellation propagates through the owning transaction and a stop request prevents
starting another row. Unexpected startup/runtime failures exit nonzero with a
finite diagnostic rather than an exception/SQL traceback. Multiple drivers share
only scan traversal progress and remain safe through common journal locks and
immutable command identity.

Keep the worker and producer secrets separate from child environment. No standalone
public endpoint, new database or automatic image publication was added. Deploy this
worker only after exact source/image/GitOps review. Configure required attestation
only with the paired supervised gateways and positive legacy drain. Whole parent
loss before durable observation stays UNKNOWN/LIVE. Neither timeout, missing pod,
lease expiry nor process ID lookup creates proof. Retained Telegram and Website
proof/recovery survive cohort/binding rollback; fresh send still requires its
existing admission and authority checks. Voice and Business contracts are unchanged.

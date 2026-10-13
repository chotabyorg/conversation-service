# Goal5 durable turn settling

## Policy and admission snapshot

`TURN_SETTLING_POLICIES` is optional service configuration. Each policy has a
version, channel, optional integration ID, quiet window, and maximum wait. A
channel policy provides the default; one integration-specific policy overrides
it. Values have finite bounds, and `max_wait_ms` must be at least the quiet
window. There is no implicit timeout. With no matching policy, the existing
immediate one-message path remains available.

Admission resolves policy once and writes the selected version and timing values
to the canonical `Message` in the same PostgreSQL transaction that accepts the
event and advances Conversation counters. A policy edit therefore affects only
later Messages. Replays do not change Message timestamps, quiet deadlines, or
settling revisions.

## Deadline and burst selection

For the oldest unassigned policy-bound Message at time `T0`:

```text
max_deadline   = T0 + max_wait
quiet_deadline = min(T0 + quiet_window, max_deadline)
```

Each next adjacent canonical inbound Message with the same policy snapshot and
accepted strictly before the current deadline resets the quiet deadline to
`min(message.created_at + quiet_window, max_deadline)`. At the deadline, the
oldest eligible prefix becomes one Turn. A Message at or beyond the cutoff stays
pending for a later Turn. Selection includes at most 256 Messages; a longer burst
continues in a later Turn after the active slot is free. Canonical Message IDs,
sequence, and order are retained individually in `turn_input_messages`; no
synthetic concatenated Message is created. Turn context freezes at the selected
last sequence, so later arrivals cannot leak into that request.

Non-adjacent canonical sequences, an intervening agent Message, or a changed
policy snapshot ends the current selection. Policy-free Messages stay on the
immediate selector and are not merged into a policy-bound burst. A direct
`POST /conversations/{id}/turns` request that includes a policy-bound inbound
Message fails with `SETTLING_REQUIRED`.

## Persistence, active work, and forward progress

The worker reconstructs deadlines from persisted `Message.created_at` and policy
snapshot columns after every restart. The Conversation row lock serializes
admission against selection. Under that lock the scheduler rechecks control
authority and the active-Turn slot, selects unassigned inputs, and inserts the
Turn and all `turn_input_messages` mappings in one transaction. The partial
unique index on active Turn status remains the final one-active-Turn guard.

Additional inbound during settling, claim, execution, or a delivery attempt is
retained as a canonical Message. It never mutates a frozen Turn. The scheduler
orders candidate Conversations by the oldest pending policy-bound Message and
examines the full candidate set in that order, rather than repeatedly stopping
at a fixed first-N prefix. Each candidate is revalidated with
`FOR UPDATE SKIP LOCKED`; locked, not-yet-due, or otherwise ineligible work does
not prevent later eligible Conversations from being considered in the same pass.
The Telegram worker's separate bounded keyset scan remains the Goal4B immediate
path; Goal5 does not replace or duplicate its starvation fix.

## Supersede and dispatch fence

Every new canonical customer inbound increments `Conversation.settling_revision`,
including Messages admitted with no settling policy; those still use immediate
one-message selection but can supersede an older, not-yet-dispatched draft. A
Turn snapshots the current Conversation revision in its freeze transaction, so
Messages already accepted but outside its selected window are ordered for later
work without making the newly frozen Turn stale. Before
provider/socket dispatch, the managed adapter acquires locks in this order:

1. Conversation;
2. completed AgentTurn;
3. the source Turn's unique DeliveryAttempt.

Within one transaction it checks completed draft identity, current AGENT control,
unchanged authority revision, and whether any newer policy-bound input advanced
the settling revision. It writes either `DISPATCH_REQUESTED` and returns
`send=true`, or terminal `SUPERSEDED` evidence and returns `send=false`. Inbound
admission uses the same Conversation lock, so it serializes on one side of this
decision. No gateway-local debounce or send permit exists.

Superseding affects communication only. The immutable Turn result and its opaque
`operation_refs` remain retained, and Conversation never rolls back completed
Business effects. The next Turn uses later unassigned canonical Messages. An
unfinished external write is marked `UNKNOWN` when retried after an interrupted
attempt; no new provider send is authorized. Definitive late evidence may still
reconcile the existing delivery.

## Issue #18 acceptance mapping

The relevant runtime criteria from [AI issue #18](https://github.com/chotabyorg/ai-agent-service/issues/18)
map to this implementation:

| Criterion | Implementation and evidence |
| --- | --- |
| Quiet window resets on new eligible Message | Admission snapshots timing; `_burst` advances quiet deadline; `test_quiet_reset_selects_original_messages_once_as_one_turn` |
| Bounded wait under continuous inbound | Fixed first-message max deadline; `test_max_wait_bounds_continuous_burst` |
| Policies vary by channel/integration; no universal timeout | `SettlingPolicy`, exact integration override in `Settings.settling_policy_for`, `test_settling_policy_resolution_is_explicit_and_integration_override_wins` |
| Original IDs and ordering are preserved | `turn_input_messages` plus immutable selected IDs; quiet-reset and Website settling tests |
| One eligible burst forms one Turn; no second queue authority | Shared PostgreSQL selector and one-active unique index; `test_two_workers_racing_select_one_turn` |
| Single Message and inputs beyond the window are explicit | Snapshot selection with a single-ID Turn; separated-input regression in `test_settled_inputs_outside_window_form_later_turns` |
| Retries, duplicates, and concurrent inbound preserve canonical identity | Inbound event idempotency plus scheduler lock; restart and inbound/delivery race tests |
| No gateway debounce | No gateway changes; workers call the shared Conversation scheduler |
| Voice partial/VAD exclusion | The scheduler begins only from persisted canonical CUSTOMER/INBOUND Messages; no Voice adapter or partial transcript is added here |

Existing worker adapters currently invoke the shared selector for managed
Telegram and Website. Other channels can adopt it when their Conversation-owned
worker and managed binding are introduced; no behavior is enabled by this
checkpoint alone.

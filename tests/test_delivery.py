from uuid import uuid4

import pytest
from sqlalchemy import text

from tests.conftest import error

LEGAL = {
    "GENERATED": ["DISPATCH_REQUESTED"],
    "DISPATCH_REQUESTED": [
        "PROVIDER_ACCEPTED",
        "DELIVERED",
        "READ",
        "FAILED",
        "UNKNOWN",
        "PARTIAL",
        "INTERRUPTED",
    ],
    "PROVIDER_ACCEPTED": ["DELIVERED", "READ", "FAILED", "UNKNOWN", "PARTIAL", "INTERRUPTED"],
    "UNKNOWN": ["PROVIDER_ACCEPTED", "DELIVERED", "READ", "FAILED", "PARTIAL", "INTERRUPTED"],
    "PARTIAL": ["DELIVERED", "READ", "FAILED", "UNKNOWN", "INTERRUPTED"],
    "INTERRUPTED": ["DELIVERED", "READ", "FAILED", "UNKNOWN", "PARTIAL"],
    "DELIVERED": ["READ"],
    "READ": [],
    "FAILED": [],
}


@pytest.mark.parametrize(
    "source,target",
    [
        (source, target)
        for source, targets in LEGAL.items()
        for target in targets + ([source] if source != "GENERATED" else [])
    ],
)
async def test_every_documented_delivery_transition_and_same_state_enrichment(h, source, target):
    _, attempt, _ = await h.delivery()
    if source != "GENERATED":
        attempt, _ = await h.reconcile(attempt, "DISPATCH_REQUESTED")
        if source != "DISPATCH_REQUESTED":
            attempt, _ = await h.reconcile(attempt, source)
    previous_revision = attempt["revision"]
    attempt, _ = await h.reconcile(attempt, target)
    assert attempt["status"] == target and attempt["revision"] == previous_revision + 1
    if target == "READ":
        assert attempt["delivered_at"] is not None and attempt["read_at"] is not None


async def test_reconciliation_equivalent_utc_offsets_are_exact_key_replay(h):
    _, attempt, _ = await h.delivery()
    attempt, _ = await h.reconcile(attempt, "DISPATCH_REQUESTED")
    attempt, payload = await h.reconcile(
        attempt, "UNKNOWN", provider_occurred_at="2026-10-06T12:00:00+03:00"
    )
    response = await h.call(
        "POST",
        f"/delivery-attempts/{attempt['id']}/reconcile",
        dict(payload, provider_occurred_at="2026-10-06T09:00:00.000000Z"),
    )
    assert response.status_code == 200 and response.json() == dict(
        attempt=attempt, applied_revision=2, replayed=True
    )


async def test_all_keys_retained_old_replay_returns_current_state_and_applied_revision(h, sql):
    accepted, attempt, create = await h.delivery()
    conversation_id = accepted["conversation"]["id"]
    history = []
    for status in [
        "DISPATCH_REQUESTED",
        "UNKNOWN",
        "PARTIAL",
        "INTERRUPTED",
        "UNKNOWN",
        "PROVIDER_ACCEPTED",
        "DELIVERED",
        "READ",
    ]:
        attempt, request = await h.reconcile(attempt, status)
        history.append((request, attempt["revision"]))
        if status == "UNKNOWN":
            assert attempt["status"] == "UNKNOWN" and attempt["status"] != "FAILED"
        if status == "PROVIDER_ACCEPTED":
            assert attempt["delivered_at"] is None and attempt["read_at"] is None
    assert attempt["delivered_at"] and attempt["read_at"]
    control = await h.call(
        "POST",
        f"/conversations/{conversation_id}/control",
        dict(expected_authority_revision=0, control_mode="HUMAN"),
    )
    assert control.status_code == 200
    for request, revision in history:
        replay = await h.call("POST", f"/delivery-attempts/{attempt['id']}/reconcile", request)
        assert replay.status_code == 200
        assert replay.json() == dict(attempt=attempt, applied_revision=revision, replayed=True)
        changed = dict(request, provider_message_id="changed-reference")
        error(
            await h.call("POST", f"/delivery-attempts/{attempt['id']}/reconcile", changed),
            "DELIVERY_RECONCILIATION_CONFLICT",
        )
    replay_create = await h.call(
        "POST", f"/conversations/{conversation_id}/delivery-attempts", create
    )
    assert replay_create.status_code == 200 and replay_create.json() == dict(
        attempt=attempt, replayed=True
    )
    error(
        await h.call(
            "POST",
            f"/conversations/{conversation_id}/delivery-attempts",
            dict(create, expected_authority_revision=1),
        ),
        "DELIVERY_IDENTITY_CONFLICT",
    )
    async with sql.connect() as connection:
        assert await connection.scalar(text("SELECT count(*) FROM delivery_attempts")) == 1
        assert await connection.scalar(
            text("SELECT count(*) FROM delivery_reconciliations")
        ) == len(history)
        assert await connection.scalar(text("SELECT count(*) FROM messages")) == 1
    conversation = (await h.call("GET", f"/conversations/{conversation_id}")).json()
    assert conversation["input_revision"] == conversation["last_message_sequence"] == 1


async def test_unknown_never_redispatches_or_creates_another_attempt(h):
    accepted, attempt, create = await h.delivery()
    attempt, _ = await h.reconcile(attempt, "DISPATCH_REQUESTED")
    attempt, _ = await h.reconcile(attempt, "UNKNOWN", error_code="TIMEOUT")
    error(
        await h.call(
            "POST",
            f"/delivery-attempts/{attempt['id']}/reconcile",
            dict(
                reconciliation_key=str(uuid4()),
                expected_revision=2,
                status="DISPATCH_REQUESTED",
                expected_authority_revision=0,
            ),
        ),
        "INVALID_DELIVERY_TRANSITION",
    )
    error(
        await h.call(
            "POST",
            f"/conversations/{accepted['conversation']['id']}/delivery-attempts",
            dict(create, delivery_key=str(uuid4())),
        ),
        "DELIVERY_INTENT_EXISTS",
    )
    current = (await h.call("GET", f"/delivery-attempts/{attempt['id']}")).json()
    assert current["status"] == "UNKNOWN" and current["revision"] == 2
    attempt, _ = await h.reconcile(attempt, "FAILED", error_code="PROVIDER_REJECTED")
    assert attempt["status"] == "FAILED"
    error(
        await h.call(
            "POST",
            f"/delivery-attempts/{attempt['id']}/reconcile",
            dict(reconciliation_key=str(uuid4()), expected_revision=3, status="DELIVERED"),
        ),
        "INVALID_DELIVERY_TRANSITION",
    )


@pytest.mark.parametrize(
    "proven,target",
    [("DELIVERED", "FAILED"), ("DELIVERED", "UNKNOWN"), ("READ", "DELIVERED"), ("READ", "UNKNOWN")],
)
async def test_positive_delivery_evidence_cannot_regress(h, proven, target):
    _, attempt, _ = await h.delivery()
    attempt, _ = await h.reconcile(attempt, "DISPATCH_REQUESTED")
    attempt, _ = await h.reconcile(attempt, proven)
    error(
        await h.call(
            "POST",
            f"/delivery-attempts/{attempt['id']}/reconcile",
            dict(reconciliation_key=str(uuid4()), expected_revision=2, status=target),
        ),
        "INVALID_DELIVERY_TRANSITION",
    )
    assert (await h.call("GET", f"/delivery-attempts/{attempt['id']}")).json() == attempt


async def test_reference_enrichment_immutable_provider_refs_and_stale_revision(h):
    _, attempt, _ = await h.delivery()
    attempt, _ = await h.reconcile(attempt, "DISPATCH_REQUESTED")
    attempt, _ = await h.reconcile(
        attempt, "PROVIDER_ACCEPTED", provider_operation_id="operation-123"
    )
    attempt, _ = await h.reconcile(attempt, "PROVIDER_ACCEPTED", provider_message_id="message-123")
    assert (
        attempt["provider_operation_id"] == "operation-123"
        and attempt["provider_message_id"] == "message-123"
    )
    error(
        await h.call(
            "POST",
            f"/delivery-attempts/{attempt['id']}/reconcile",
            dict(reconciliation_key=str(uuid4()), expected_revision=2, status="DELIVERED"),
        ),
        "DELIVERY_REVISION_CONFLICT",
    )
    error(
        await h.call(
            "POST",
            f"/delivery-attempts/{attempt['id']}/reconcile",
            dict(
                reconciliation_key=str(uuid4()),
                expected_revision=3,
                status="DELIVERED",
                provider_operation_id="different",
            ),
        ),
        "DELIVERY_REFERENCE_CONFLICT",
    )
    attempt, _ = await h.reconcile(attempt, "READ")
    assert attempt["provider_operation_id"] == "operation-123"
    assert attempt["provider_message_id"] == "message-123"


async def test_dispatch_authority_fenced_but_earlier_facts_reconcile_after_control(h):
    accepted, attempt, _ = await h.delivery()
    conversation_id = accepted["conversation"]["id"]
    assert (
        await h.call(
            "POST",
            f"/conversations/{conversation_id}/control",
            dict(expected_authority_revision=0, control_mode="AGENT"),
        )
    ).status_code == 200
    error(
        await h.call(
            "POST",
            f"/delivery-attempts/{attempt['id']}/reconcile",
            dict(
                reconciliation_key=str(uuid4()),
                expected_revision=0,
                status="DISPATCH_REQUESTED",
                expected_authority_revision=0,
            ),
        ),
        "AUTHORITY_REVISION_CONFLICT",
    )
    error(
        await h.call(
            "POST",
            f"/delivery-attempts/{attempt['id']}/reconcile",
            dict(
                reconciliation_key=str(uuid4()),
                expected_revision=0,
                status="DISPATCH_REQUESTED",
                expected_authority_revision=1,
            ),
        ),
        "AUTHORITY_REVISION_CONFLICT",
    )

    # A separate fresh draft takes the new authority snapshot.
    event = await h.ingest(provider_thread_id="fresh-authority")
    accepted2, turn = await h.draft(event)
    create = dict(
        delivery_key=str(uuid4()), source_turn_id=turn["id"], expected_authority_revision=0
    )
    response = await h.call(
        "POST", f"/conversations/{accepted2['conversation']['id']}/delivery-attempts", create
    )
    assert response.status_code == 201
    attempt2 = response.json()["attempt"]
    attempt2, _ = await h.reconcile(attempt2, "DISPATCH_REQUESTED")
    assert (
        await h.call(
            "POST",
            f"/conversations/{accepted2['conversation']['id']}/control",
            dict(expected_authority_revision=0, control_mode="HUMAN"),
        )
    ).status_code == 200
    attempt2, _ = await h.reconcile(attempt2, "DELIVERED")
    assert attempt2["status"] == "DELIVERED"


async def test_new_delivery_rejects_no_draft_and_source_authority_mismatch(h):
    accepted = await h.ingest()
    turn, _ = await h.select(accepted)
    create = dict(
        delivery_key=str(uuid4()), source_turn_id=turn["id"], expected_authority_revision=0
    )
    path = f"/conversations/{accepted['conversation']['id']}/delivery-attempts"
    error(await h.call("POST", path, create), "NO_DRAFT")
    claim = await h.claim(turn)
    assert (
        await h.call(
            "POST",
            f"/turns/{turn['id']}/complete",
            dict(h.proof(claim), outcome="DRAFT_READY", draft_text="draft"),
        )
    ).status_code == 200
    assert (
        await h.call(
            "POST",
            f"/conversations/{turn['conversation_id']}/control",
            dict(expected_authority_revision=0, control_mode="AGENT"),
        )
    ).status_code == 200
    error(
        await h.call("POST", path, dict(create, expected_authority_revision=1)),
        "AUTHORITY_REVISION_CONFLICT",
    )
